// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { type VgiBatch, type VgiSchema, isBatch, batchFromColumns } from "./arrow/index.js";
import { AuthContext } from "./auth.js";
import { buildLogBatch, coerceInt64 } from "./wire/response.js";

export enum MethodType {
  UNARY = "unary",
  STREAM = "stream",
}

/** Logging interface available to handlers. */
export interface LogContext {
  clientLog(level: string, message: string, extra?: Record<string, string>): void;
}

/**
 * Attributes for a Set-Cookie directive queued via {@link CallContext.setCookie}.
 * All fields are optional; omitted attributes are not serialized onto the header.
 */
export interface CookieAttrs {
  expires?: Date;
  maxAge?: number;
  domain?: string;
  path?: string;
  secure?: boolean;
  httpOnly?: boolean;
  sameSite?: "Strict" | "Lax" | "None";
  partitioned?: boolean;
}

/**
 * A queued cookie mutation for the HTTP response. Internal — callers
 * interact through {@link CallContext.setCookie} / {@link CallContext.deleteCookie}.
 */
export interface CookieSpec extends CookieAttrs {
  name: string;
  value: string;
  delete: boolean;
}

/** Extended context with authentication info, available to handlers. */
export interface CallContext extends LogContext {
  readonly auth: AuthContext;
  /**
   * Incoming request cookies.  Empty for non-HTTP transports.
   */
  readonly cookies: ReadonlyMap<string, string>;
  /**
   * Queue a Set-Cookie header on the HTTP response.  Only valid inside a
   * unary RPC method served over HTTP; throws otherwise.
   */
  setCookie(name: string, value: string, attrs?: CookieAttrs): void;
  /**
   * Queue an unset-cookie directive on the HTTP response.  Only valid
   * inside a unary RPC method served over HTTP; throws otherwise.
   */
  deleteCookie(name: string, opts?: { path?: string; domain?: string }): void;
}

const EMPTY_COOKIES: ReadonlyMap<string, string> = new Map();

function cookieNotUnaryHttpError(): Error {
  return new Error("setCookie/deleteCookie is only supported inside unary RPC methods served over HTTP");
}

/** Handler for unary (request-response) RPC methods. */
export type UnaryHandler = (
  params: Record<string, any>,
  ctx: LogContext,
) => Promise<Record<string, any>> | Record<string, any>;

/** Initialization function for producer streams. Returns the initial state object. */
export type ProducerInit<S = any> = (params: Record<string, any>) => Promise<S> | S;
/** Called repeatedly to produce output batches. Call `out.finish()` to end the stream. */
export type ProducerFn<S = any> = (state: S, out: OutputCollector) => Promise<void> | void;

/** Initialization function for exchange streams. Returns the initial state object. */
export type ExchangeInit<S = any> = (params: Record<string, any>) => Promise<S> | S;
/** Called once per input batch. Must emit exactly one output batch per call. */
export type ExchangeFn<S = any> = (state: S, input: VgiBatch, out: OutputCollector) => Promise<void> | void;

/** Produces a header batch sent before the first output batch in a stream. */
export type HeaderInit = (params: Record<string, any>, state: any, ctx: LogContext) => Record<string, any>;

/**
 * Optional handler invoked when the client signals cancellation by writing an
 * input batch carrying the ``vgi_rpc.cancel`` metadata key. The server runs
 * this hook once, before breaking out of the streaming loop, giving state
 * objects a chance to release resources. Errors are logged and swallowed.
 */
export type OnCancelFn<S = any> = (state: S) => Promise<void> | void;

export interface MethodDefinition {
  name: string;
  type: MethodType;
  paramsSchema: VgiSchema;
  resultSchema: VgiSchema;
  outputSchema?: VgiSchema;
  inputSchema?: VgiSchema;
  handler?: UnaryHandler;
  producerInit?: ProducerInit;
  producerFn?: ProducerFn;
  exchangeInit?: ExchangeInit;
  exchangeFn?: ExchangeFn;
  headerSchema?: VgiSchema;
  headerInit?: HeaderInit;
  onCancel?: OnCancelFn;
  doc?: string;
  defaults?: Record<string, any>;
  paramTypes?: Record<string, string>;
}

/** Metadata passed to dispatch hooks before and after RPC method execution. */
export interface DispatchInfo {
  /** RPC method name. */
  method: string;
  /** "unary" or "stream". */
  methodType: string;
  /** Server identifier. */
  serverId: string;
  /** Client-supplied request identifier, or null. */
  requestId: string | null;
  /** Logical service / protocol name. */
  protocol?: string;
  /** SHA-256 hex of the canonical __describe__ payload (always required in access log). */
  protocolHash?: string;
  /** Operator-supplied protocol-contract version label (optional). */
  protocolVersion?: string;
  /** Authenticated principal, empty string when anonymous. */
  principal?: string;
  /** Authentication domain, empty string when anonymous. */
  authDomain?: string;
  /** True when the call was authenticated. */
  authenticated?: boolean;
  /** HTTP transport: remote IP:port. */
  remoteAddr?: string;
  /** Self-contained Arrow IPC stream of the request batch (unary + stream init only). */
  requestData?: Uint8Array;
  /** Stream lifecycle identifier (32-char lowercase hex); empty on unary. */
  streamId?: string;
  /** True when a stream was cancelled by the client. */
  cancelled?: boolean;
}

/** Per-call I/O counters, matching Python's CallStatistics. */
export interface CallStatistics {
  inputBatches: number;
  outputBatches: number;
  inputRows: number;
  outputRows: number;
  inputBytes: number;
  outputBytes: number;
}

/** Opaque token returned by onDispatchStart, passed back to onDispatchEnd. */
export type HookToken = unknown;

/**
 * Observability hook called around RPC dispatch.
 * Implementations must be safe for concurrent use (HTTP transport is concurrent).
 */
export interface DispatchHook {
  onDispatchStart(info: DispatchInfo): HookToken;
  onDispatchEnd(token: HookToken, info: DispatchInfo, stats: CallStatistics, error?: Error): void;
}

export interface EmittedBatch {
  batch: VgiBatch;
  metadata?: Map<string, string>;
}

/**
 * Accumulates output batches during a produce/exchange call.
 * Enforces that exactly one data batch is emitted per call (plus any number of log batches).
 */
export class OutputCollector implements CallContext {
  private _batches: EmittedBatch[] = [];
  private _dataBatchIdx: number | null = null;
  private _finished = false;
  private _producerMode: boolean;
  private _outputSchema: VgiSchema;
  private _serverId: string;
  private _requestId: string | null;
  private _cookieSinkEnabled = false;
  private _responseCookies: CookieSpec[] = [];
  readonly auth: AuthContext;
  readonly cookies: ReadonlyMap<string, string>;

  constructor(
    outputSchema: VgiSchema,
    producerMode = true,
    serverId = "",
    requestId: string | null = null,
    authContext?: AuthContext,
    cookies?: ReadonlyMap<string, string>,
  ) {
    this._outputSchema = outputSchema;
    this._producerMode = producerMode;
    this._serverId = serverId;
    this._requestId = requestId;
    this.auth = authContext ?? AuthContext.anonymous();
    this.cookies = cookies ?? EMPTY_COOKIES;
  }

  /**
   * Mark this collector as able to accept Set-Cookie directives.  Called
   * by the unary HTTP dispatcher only; streaming and non-HTTP paths leave
   * the sink disabled so setCookie/deleteCookie throw.
   * @internal
   */
  enableCookieSink(): void {
    this._cookieSinkEnabled = true;
  }

  /**
   * Return and clear all queued cookie mutations.
   * @internal
   */
  drainResponseCookies(): CookieSpec[] {
    const cookies = this._responseCookies;
    this._responseCookies = [];
    return cookies;
  }

  setCookie(name: string, value: string, attrs?: CookieAttrs): void {
    if (!this._cookieSinkEnabled) throw cookieNotUnaryHttpError();
    this._responseCookies.push({
      name,
      value,
      delete: false,
      ...(attrs ?? {}),
    });
  }

  deleteCookie(name: string, opts?: { path?: string; domain?: string }): void {
    if (!this._cookieSinkEnabled) throw cookieNotUnaryHttpError();
    this._responseCookies.push({
      name,
      value: "",
      delete: true,
      path: opts?.path,
      domain: opts?.domain,
    });
  }

  get outputSchema(): VgiSchema {
    return this._outputSchema;
  }

  get finished(): boolean {
    return this._finished;
  }

  get batches(): EmittedBatch[] {
    return this._batches;
  }

  /** Emit a pre-built batch as the data batch for this call. */
  emit(batch: VgiBatch, metadata?: Map<string, string>): void;
  /** Emit a data batch from column arrays keyed by field name. Int64 Number values are coerced to BigInt. */
  emit(columns: Record<string, any[]>): void;
  emit(batchOrColumns: VgiBatch | Record<string, any[]>, metadata?: Map<string, string>): void {
    let batch: VgiBatch;
    if (isBatch(batchOrColumns)) {
      batch = batchOrColumns;
    } else {
      const coerced = coerceInt64(this._outputSchema, batchOrColumns as Record<string, any[]>);
      // Build columns dict ensuring each field has an array (vectorFromArray-equivalent under the hood).
      const cols: Record<string, any[]> = {};
      for (const f of this._outputSchema.fields) {
        const v = coerced[f.name];
        cols[f.name] = Array.isArray(v) ? v : [v];
      }
      batch = batchFromColumns(this._outputSchema, cols);
    }
    if (this._dataBatchIdx !== null) {
      throw new Error("Only one data batch may be emitted per call");
    }
    this._dataBatchIdx = this._batches.length;
    this._batches.push({ batch, metadata });
  }

  /** Single-row convenience. Wraps each value in `[value]` then calls `emit()`. */
  emitRow(values: Record<string, any>): void {
    const columns: Record<string, any[]> = {};
    for (const [key, value] of Object.entries(values)) {
      columns[key] = [value];
    }
    this.emit(columns);
  }

  /** Signal stream completion for producer streams. Throws if called on exchange streams. */
  finish(): void {
    if (!this._producerMode) {
      throw new Error(
        "finish() is not allowed on exchange streams; " + "exchange streams must emit exactly one data batch per call",
      );
    }
    this._finished = true;
  }

  /** Emit a zero-row client-directed log batch. */
  clientLog(level: string, message: string, extra?: Record<string, string>): void {
    const batch = buildLogBatch(this._outputSchema, level, message, extra, this._serverId, this._requestId);
    this._batches.push({ batch });
  }
}
