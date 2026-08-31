import type { RecordBatch } from "@query-farm/apache-arrow";
/** Rows inferred for compatibility, or a batch built from an explicit declared Arrow schema. */
export type ExchangeInput = Record<string, any>[] | RecordBatch;
/** Options for {@link httpConnect}, the HTTP-transport RPC client. */
export interface HttpConnectOptions {
    /**
     * Statically declared service description. When supplied, the client uses
     * these method and stream schemas directly and does not call `__describe__`.
     * This is required for services that intentionally expose only their
     * declared RPC surface and for exact exchange input schemas that cannot be
     * recovered by inspecting runtime values.
     */
    description?: import("./introspect.js").ServiceDescription;
    /** Route prefix the server mounts its methods under (e.g. `/api`). Trailing slashes are stripped. Defaults to no prefix. */
    prefix?: string;
    /** Callback invoked for each log/error message the server emits during a request. */
    onLog?: (msg: LogMessage) => void;
    /** When set, request bodies are zstd-compressed at this level and `Accept-Encoding: zstd` is sent. Omit to disable compression. */
    compressionLevel?: number;
    /** Authorization header value (e.g. "Bearer <token>"). Sent with every request. */
    authorization?: string;
    /** External storage config for resolving externalized batches. */
    externalLocation?: import("../external.js").ExternalLocationConfig;
    /**
     * Request implementation used for every HTTP operation owned by this
     * client, including introspection and upload-URL exchange. Defaults to the
     * runtime's global `fetch`. Node callers normally use
     * `httpConnectSocks5h` instead of setting this directly.
     */
    fetch?: typeof globalThis.fetch;
}
/** A log or error message delivered to an {@link HttpConnectOptions.onLog} callback. */
export interface LogMessage {
    /** Severity, mirroring the server's log level (e.g. `INFO`, `WARNING`, `EXCEPTION`). */
    level: string;
    /** The human-readable log text. */
    message: string;
    /** Optional structured fields attached to the log record. */
    extra?: Record<string, any>;
}
/**
 * A live streaming method call. Exchange methods drive the server with
 * {@link StreamSession.exchange}; producer methods are consumed by async
 * iteration. Always {@link StreamSession.close} when done.
 */
export interface StreamSession {
    /** The method's header row (returned once at stream start), or `null` if the method declares no header. */
    readonly header: Record<string, any> | null;
    /** Send one batch of input rows and receive the server's corresponding output rows (exchange streams). */
    exchange(input: ExchangeInput): Promise<Record<string, any>[]>;
    /** Send one producer tick with optional application custom metadata and
     * return the rows emitted for that tick. */
    tick(metadata?: ReadonlyMap<string, string>): Promise<Record<string, any>[]>;
    /** Iterate the server-produced output batches one row-array at a time (producer streams). */
    [Symbol.asyncIterator](): AsyncIterableIterator<Record<string, any>[]>;
    /** Tear down the stream, flushing/draining the underlying transport. */
    close(): void;
}
/** Options for {@link pipeConnect}, the client over raw readable/writable streams. */
export interface PipeConnectOptions {
    /** Callback invoked for each log/error message the server emits during a request. */
    onLog?: (msg: LogMessage) => void;
    /** External storage config for resolving externalized batches. */
    externalLocation?: import("../external.js").ExternalLocationConfig;
}
/**
 * Options for {@link tcpConnect}, the raw-TCP-socket RPC client.
 *
 * SECURITY: raw TCP carries no authentication or TLS — connect only to
 * trusted endpoints (use {@link httpConnect} for untrusted networks).
 */
export interface TcpConnectOptions extends PipeConnectOptions {
}
/** Options for the Node-only {@link tcpConnectSocks5h} constructor. */
export interface Socks5hTcpConnectOptions extends TcpConnectOptions {
    /** One deadline for proxy TCP setup and the complete SOCKS5 negotiation. Default: 5000 ms. */
    connectTimeoutMs?: number;
    /** Cancels proxy setup and negotiation. */
    signal?: AbortSignal;
}
/** Options for the Node-only {@link httpConnectSocks5h} constructor. */
export interface Socks5hHttpConnectOptions extends Omit<HttpConnectOptions, "fetch"> {
    /** One deadline for each proxy TCP setup, SOCKS5 negotiation, and TLS handshake. Default: 5000 ms. */
    connectTimeoutMs?: number;
    /** Total deadline for one HTTP request, including setup and response. Default: 300000 ms. */
    requestTimeoutMs?: number;
    /** Maximum buffered HTTP response, including headers. Default: 268435456 bytes. */
    maxResponseBytes?: number;
    /** Maximum HTTP response-header bytes within maxResponseBytes. Default: 65536 bytes. */
    maxResponseHeaderBytes?: number;
    /** Cancels HTTP requests and any in-progress proxy setup. */
    signal?: AbortSignal;
}
/** Options for {@link subprocessConnect}, which spawns a server process and pipes to it. */
export interface SubprocessConnectOptions extends PipeConnectOptions {
    /** Working directory for the spawned process. Defaults to the current directory. */
    cwd?: string;
    /** Extra environment variables, merged over the current `process.env`. */
    env?: Record<string, string>;
    /** How to handle the child's stderr. Defaults to `"ignore"`. */
    stderr?: "inherit" | "pipe" | "ignore";
}
//# sourceMappingURL=types.d.ts.map