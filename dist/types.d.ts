import { type VgiBatch, type VgiSchema } from "./arrow/index.js";
import { AuthContext } from "./auth.js";
/**
 * Whether an RPC method is request/response or streaming. Mirrors Python's
 * `MethodType` and is carried in the `__describe__` payload.
 */
export declare enum MethodType {
    /** Single request batch in, single result batch out. */
    UNARY = "unary",
    /** Streamed batches — either a producer or an exchange stream. */
    STREAM = "stream"
}
/**
 * Coarse identifier of the transport binding a {@link VgiRpcServer} or
 * HTTP handler.  Workers (RPC implementations) read this via
 * {@link CallContext.kind} or the {@link ServeStartHook} lifecycle hook
 * to tailor startup behaviour (skip HTTP-only caching, enable
 * transport-specific metrics, etc.).
 *
 * Values are wire/log-friendly strings to match Python's `TransportKind`
 * StrEnum byte-for-byte across language boundaries.
 *
 * - `PIPE` — Stdio worker (the standalone {@link VgiRpcServer} loop).
 * - `HTTP` — Fetch-style HTTP handler (`createHttpHandler`).
 * - `UNIX` — AF_UNIX socket handler (the launcher path).
 * - `TCP` — AF_INET socket handler. Raw Arrow-IPC framing over a bare TCP
 *   socket — no auth/TLS; use `HTTP` for untrusted networks.
 */
export declare enum TransportKind {
    /** Stdio worker — the standalone {@link VgiRpcServer} loop. */
    PIPE = "pipe",
    /** Fetch-style HTTP handler (`createHttpHandler`). */
    HTTP = "http",
    /** AF_UNIX socket handler (the launcher path). */
    UNIX = "unix",
    /** AF_INET (TCP) socket handler. Raw Arrow-IPC framing over a bare TCP
     *  socket — no authentication or TLS; trusted networks only. */
    TCP = "tcp"
}
/**
 * Optional lifecycle hook fired once per process before the first
 * dispatched request.
 *
 * For the stdio server, fires inside `VgiRpcServer.run()` before the
 * first read.  For HTTP, fires lazily on the first request handled
 * (fork-safe for pre-fork servers).
 *
 * If the hook raises, the server logs the exception and propagates it,
 * leaving the bind state unset so the next attempt re-fires the hook
 * rather than silently skipping it.
 */
export type ServeStartHook = (kind: TransportKind) => void | Promise<void>;
/** Logging interface available to handlers. */
export interface LogContext {
    /** Emit a client-directed log message (sent as a zero-row log batch on the
     *  wire). `level` is a severity label such as `"info"`, `"warning"`, or
     *  `"error"`; `extra` carries optional structured string key/value pairs. */
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
/** Per-request sticky-session sink. Internal — populated by the HTTP handler
 *  when sticky sessions are enabled, read/mutated by {@link CallContext}'s
 *  `openSession` / `closeSession` / `session` getters. */
export interface StickyContext {
    readonly acceptOpens: boolean;
    state: unknown | null;
    sessionId: string | null;
    mintToken: string | null;
    closed: boolean;
    action: "none" | "resume" | "open" | "close";
    _open(state: unknown, ttl: number | undefined): void;
    _close(): void;
}
/** Extended context with authentication info, available to handlers. */
export interface CallContext extends LogContext {
    /** Authenticated principal for this call; {@link AuthContext.anonymous} when
     *  the request was not authenticated. */
    readonly auth: AuthContext;
    /** Coarse identifier of the bound transport, or `undefined` until the
     *  server begins serving (the value is committed by the lifecycle hook
     *  on the very first request). */
    readonly kind?: TransportKind;
    /**
     * Wire body bytes the framework will accept this iteration before
     * triggering a continuation token (producer streams) or strict-fail
     * with an EXCEPTION batch (unary / stream-exchange).  Snapshot at
     * collector construction; not live.  `undefined` when no cap is
     * configured or the transport doesn't expose one (stdio).
     */
    readonly remainingResponseBytes?: number;
    /**
     * External-channel bytes left this iteration.  Always a hard cap —
     * externalised uploads have no escape valve like producer
     * continuation tokens.  Undefined when no cap is configured or
     * externalisation is disabled.
     */
    readonly remainingExternalizedResponseBytes?: number;
    /** True iff the server has an externalisation backend wired up. */
    readonly externalizationEnabled?: boolean;
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
    deleteCookie(name: string, opts?: {
        path?: string;
        domain?: string;
    }): void;
    /**
     * Live sticky-session state object, or `null` when no session is bound to
     * this request. HTTP-only — other transports always return `null`.
     */
    readonly session: unknown | null;
    /**
     * Opaque 24-char-hex session ID, or `null` when no session is bound.
     * Survives {@link closeSession} so post-close access-log records still
     * carry the id.
     */
    readonly sessionId: string | null;
    /**
     * Register a sticky session holding *state* for subsequent requests on
     * this transport. HTTP-only — throws on other transports, on calls
     * without the `VGI-Session-Accept: true` opt-in header, or when a
     * session is already bound to this request.
     */
    openSession(state: unknown, ttl?: number): void;
    /** Invalidate the sticky session bound to this request. Idempotent. */
    closeSession(): void;
}
/** Handler for unary (request-response) RPC methods. */
export type UnaryHandler = (params: Record<string, any>, ctx: LogContext) => Promise<Record<string, any>> | Record<string, any>;
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
/**
 * In-memory definition of one registered RPC method, produced by the
 * {@link Protocol} builder and consumed by the dispatch layer. Which optional
 * fields are populated depends on the method {@link type}: `handler` for unary
 * methods, `producerInit`/`producerFn` for producer streams, and
 * `exchangeInit`/`exchangeFn` for exchange streams.
 */
export interface MethodDefinition {
    /** Method name as registered on the protocol. */
    name: string;
    /** Whether the method is unary or streaming. */
    type: MethodType;
    /** Schema of the request parameters batch. */
    paramsSchema: VgiSchema;
    /** Schema of the unary result batch (unused for streams). */
    resultSchema: VgiSchema;
    /** Schema of streamed output batches (producer and exchange streams). */
    outputSchema?: VgiSchema;
    /** Schema of streamed input batches (exchange streams only). */
    inputSchema?: VgiSchema;
    /** Implementation for unary methods. */
    handler?: UnaryHandler;
    /** Builds the initial state object for a producer stream. */
    producerInit?: ProducerInit;
    /** Produces output batches for a producer stream. */
    producerFn?: ProducerFn;
    /** Builds the initial state object for an exchange stream. */
    exchangeInit?: ExchangeInit;
    /** Handles each input batch of an exchange stream. */
    exchangeFn?: ExchangeFn;
    /** Schema of the optional per-stream header batch. */
    headerSchema?: VgiSchema;
    /** Builds the optional header batch emitted before the first output batch. */
    headerInit?: HeaderInit;
    /** Optional hook run when the client cancels a stream. */
    onCancel?: OnCancelFn;
    /** Human-readable method documentation, surfaced via introspection. */
    doc?: string;
    /** Default values applied to omitted request parameters. */
    defaults?: Record<string, any>;
    /** Human-readable parameter type names, surfaced via introspection. */
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
    /** Coarse transport identifier — `pipe` for stdio, `http` for fetch
     *  handlers, `unix` for AF_UNIX. */
    kind?: TransportKind;
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
    /** Sticky session ID (24-char hex). Present only when the request was bound
     *  to a sticky session or the method opened/closed one. */
    sessionId?: string;
    /** Sticky-session lifecycle action observed during dispatch — one of
     *  `"none"` / `"resume"` / `"open"` / `"close"`. Omitted when sticky is
     *  disabled or the request never touched the sticky middleware. */
    sessionAction?: "none" | "resume" | "open" | "close";
}
/** Per-call I/O counters, matching Python's CallStatistics. */
export interface CallStatistics {
    /** Number of input batches read from the client. */
    inputBatches: number;
    /** Number of output batches written to the client. */
    outputBatches: number;
    /** Total rows across all input batches. */
    inputRows: number;
    /** Total rows across all output batches. */
    outputRows: number;
    /** Total serialized bytes of all input batches. */
    inputBytes: number;
    /** Total serialized bytes of all output batches. */
    outputBytes: number;
}
/** Opaque token returned by onDispatchStart, passed back to onDispatchEnd. */
export type HookToken = unknown;
/**
 * Observability hook called around RPC dispatch.
 * Implementations must be safe for concurrent use (HTTP transport is concurrent).
 */
export interface DispatchHook {
    /** Invoked before the method runs. The returned {@link HookToken} is opaque
     *  to the framework and passed back to {@link onDispatchEnd}. */
    onDispatchStart(info: DispatchInfo): HookToken;
    /** Invoked after the method completes or throws. `stats` carries the per-call
     *  I/O counters; `error` is set only when the dispatch failed. */
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
export declare class OutputCollector implements CallContext {
    private _batches;
    private _dataBatchIdx;
    private _finished;
    private _producerMode;
    private _outputSchema;
    private _serverId;
    private _requestId;
    private _cookieSinkEnabled;
    private _responseCookies;
    private _stickyContext;
    /** Authenticated principal for this call; {@link AuthContext.anonymous} when
     *  the request was not authenticated. */
    readonly auth: AuthContext;
    readonly cookies: ReadonlyMap<string, string>;
    readonly kind?: TransportKind;
    readonly remainingResponseBytes?: number;
    readonly remainingExternalizedResponseBytes?: number;
    readonly externalizationEnabled?: boolean;
    constructor(outputSchema: VgiSchema, producerMode?: boolean, serverId?: string, requestId?: string | null, authContext?: AuthContext, cookies?: ReadonlyMap<string, string>, kind?: TransportKind, 
    /** Snapshot budget fields exposed to worker code via {@link CallContext}.
     *  Optional — non-HTTP transports omit them and existing call sites
     *  remain source-compatible. */
    budgets?: {
        remainingResponseBytes?: number;
        remainingExternalizedResponseBytes?: number;
        externalizationEnabled?: boolean;
    });
    /**
     * Mark this collector as able to accept Set-Cookie directives.  Called
     * by the unary HTTP dispatcher only; streaming and non-HTTP paths leave
     * the sink disabled so setCookie/deleteCookie throw.
     * @internal
     */
    enableCookieSink(): void;
    /**
     * Return and clear all queued cookie mutations.
     * @internal
     */
    drainResponseCookies(): CookieSpec[];
    setCookie(name: string, value: string, attrs?: CookieAttrs): void;
    deleteCookie(name: string, opts?: {
        path?: string;
        domain?: string;
    }): void;
    /** Attach the sticky-session sink the HTTP handler built for this request.
     *  @internal */
    attachStickyContext(ctx: StickyContext): void;
    get session(): unknown | null;
    get sessionId(): string | null;
    openSession(state: unknown, ttl?: number): void;
    closeSession(): void;
    /** Schema of the data batches this collector emits. */
    get outputSchema(): VgiSchema;
    /** True once {@link finish} has been called (producer streams only). */
    get finished(): boolean;
    /** Batches emitted so far this call — the single data batch plus any log
     *  batches, in emission order. Consumed by the dispatch layer. */
    get batches(): EmittedBatch[];
    /** Index of the data batch within {@link batches}, or null if none was
     *  emitted this call. Log batches never occupy this slot. */
    get dataBatchIdx(): number | null;
    /** Emit a pre-built batch as the data batch for this call. */
    emit(batch: VgiBatch, metadata?: Map<string, string>): void;
    /** Emit a data batch from column arrays keyed by field name. Int64 Number values are coerced to BigInt. */
    emit(columns: Record<string, any[]>): void;
    /** Single-row convenience. Wraps each value in `[value]` then calls `emit()`. */
    emitRow(values: Record<string, any>): void;
    /** Signal stream completion for producer streams. Throws if called on exchange streams. */
    finish(): void;
    /** Emit a zero-row client-directed log batch. */
    clientLog(level: string, message: string, extra?: Record<string, string>): void;
}
//# sourceMappingURL=types.d.ts.map