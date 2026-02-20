import { type Schema, RecordBatch, recordBatchFromArrays } from "apache-arrow";
import { buildLogBatch, coerceInt64 } from "./wire/response.js";

export enum MethodType {
  UNARY = "unary",
  STREAM = "stream",
}

/** Logging interface available to handlers. */
export interface LogContext {
  clientLog(level: string, message: string, extra?: Record<string, string>): void;
}

/** Handler for unary (request-response) RPC methods. */
export type UnaryHandler = (
  params: Record<string, any>,
  ctx: LogContext,
) => Promise<Record<string, any>> | Record<string, any>;

/** Initialization function for producer streams. Returns the initial state object. */
export type ProducerInit<S = any> = (
  params: Record<string, any>,
) => Promise<S> | S;
/** Called repeatedly to produce output batches. Call `out.finish()` to end the stream. */
export type ProducerFn<S = any> = (
  state: S,
  out: OutputCollector,
) => Promise<void> | void;

/** Initialization function for exchange streams. Returns the initial state object. */
export type ExchangeInit<S = any> = (
  params: Record<string, any>,
) => Promise<S> | S;
/** Called once per input batch. Must emit exactly one output batch per call. */
export type ExchangeFn<S = any> = (
  state: S,
  input: RecordBatch,
  out: OutputCollector,
) => Promise<void> | void;

/** Produces a header batch sent before the first output batch in a stream. */
export type HeaderInit = (
  params: Record<string, any>,
  state: any,
  ctx: LogContext,
) => Record<string, any>;

export interface MethodDefinition {
  name: string;
  type: MethodType;
  paramsSchema: Schema;
  resultSchema: Schema;
  outputSchema?: Schema;
  inputSchema?: Schema;
  handler?: UnaryHandler;
  producerInit?: ProducerInit;
  producerFn?: ProducerFn;
  exchangeInit?: ExchangeInit;
  exchangeFn?: ExchangeFn;
  headerSchema?: Schema;
  headerInit?: HeaderInit;
  doc?: string;
  defaults?: Record<string, any>;
  paramTypes?: Record<string, string>;
}

export interface EmittedBatch {
  batch: RecordBatch;
  metadata?: Map<string, string>;
}

/**
 * Accumulates output batches during a produce/exchange call.
 * Enforces that exactly one data batch is emitted per call (plus any number of log batches).
 */
export class OutputCollector implements LogContext {
  private _batches: EmittedBatch[] = [];
  private _dataBatchIdx: number | null = null;
  private _finished = false;
  private _producerMode: boolean;
  private _outputSchema: Schema;
  private _serverId: string;
  private _requestId: string | null;

  constructor(outputSchema: Schema, producerMode = true, serverId = "", requestId: string | null = null) {
    this._outputSchema = outputSchema;
    this._producerMode = producerMode;
    this._serverId = serverId;
    this._requestId = requestId;
  }

  get outputSchema(): Schema {
    return this._outputSchema;
  }

  get finished(): boolean {
    return this._finished;
  }

  get batches(): EmittedBatch[] {
    return this._batches;
  }

  /** Emit a pre-built RecordBatch as the data batch for this call. */
  emit(batch: RecordBatch, metadata?: Map<string, string>): void;
  /** Emit a data batch from column arrays keyed by field name. Int64 Number values are coerced to BigInt. */
  emit(columns: Record<string, any[]>): void;
  emit(
    batchOrColumns: RecordBatch | Record<string, any[]>,
    metadata?: Map<string, string>,
  ): void {
    let batch: RecordBatch;
    if (batchOrColumns instanceof RecordBatch) {
      batch = batchOrColumns;
    } else {
      const coerced = coerceInt64(this._outputSchema, batchOrColumns);
      batch = recordBatchFromArrays(coerced, this._outputSchema);
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
        "finish() is not allowed on exchange streams; " +
          "exchange streams must emit exactly one data batch per call",
      );
    }
    this._finished = true;
  }

  /** Emit a zero-row client-directed log batch. */
  clientLog(level: string, message: string, extra?: Record<string, string>): void {
    const batch = buildLogBatch(
      this._outputSchema,
      level,
      message,
      extra,
      this._serverId,
      this._requestId,
    );
    this._batches.push({ batch });
  }

}
