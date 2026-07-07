import type { ExternalLocationConfig } from "./external.js";
import type { Protocol } from "./protocol.js";
import { type DispatchHook, type ServeStartHook, TransportKind } from "./types.js";
/**
 * RPC server that reads Arrow IPC requests from stdin and writes responses to stdout.
 * Supports unary and streaming (producer/exchange) methods.
 */
export declare class VgiRpcServer {
    private protocol;
    private enableDescribe;
    private serverId;
    private _describePromise;
    private protocolVersion;
    private dispatchHook;
    private externalConfig;
    private onServeStart;
    /** True once the on_serve_start hook has fired successfully. The bind
     *  state is committed only after the hook returns, so a transient
     *  failure on first request leaves it `false` and the next request
     *  re-fires rather than silently skipping. Mirrors Python 7b3999c. */
    private serveStartFired;
    constructor(protocol: Protocol, options?: {
        /** Enable the `describe` RPC method (service self-description). Default `true`. */
        enableDescribe?: boolean;
        /** Opaque per-process server identifier surfaced to clients and the landing page. */
        serverId?: string;
        /** Hook invoked around each dispatched request (tracing/metrics/auth enrichment). */
        dispatchHook?: DispatchHook;
        /** Configuration for externalizing oversized record batches to blob storage. */
        externalLocation?: ExternalLocationConfig;
        /** Protocol version string reported in the service description. */
        protocolVersion?: string;
        /** Lifecycle hook fired once before the first dispatched request. */
        onServeStart?: ServeStartHook;
    });
    /** Fire the on_serve_start hook once for this transport. Idempotent
     *  on success — re-throws on failure without committing the bind. */
    private notifyTransport;
    /** Build (or retrieve cached) describe batch + protocol hash. */
    private describeInfo;
    /** Validate a client's declared protocol_version against the Protocol's
     *  declared version. Caller invokes only when
     *  `protocol.protocolVersionParts` is non-null. Mirrors Python's
     *  `RpcServer._check_protocol_version`: exact major+minor match, patch
     *  ignored; directional error message names which side is older. */
    private checkProtocolVersion;
    /** Start the server loop over stdin/stdout. Reads requests until stdin closes. */
    run(): Promise<void>;
    /**
     * Serve requests over an explicit byte-stream pair until the readable ends —
     * the transport-agnostic core that {@link run} (stdin/stdout) is built on.
     *
     * Use this to serve over any duplex channel that the stdio/unix/tcp helpers
     * don't cover: a Web Worker / `MessagePort` bridge, an in-memory pipe, or a
     * pre-connected socket. The loop, on_serve_start firing, and EOF/broken-pipe
     * handling are identical to {@link run}.
     *
     * @param readable incoming request bytes — a web `ReadableStream<Uint8Array>`
     *   or a Node `Readable` (e.g. a `Duplex` bridging a MessagePort).
     * @param writable outgoing response sink — a stdout-like fd number, or a
     *   `net.Socket` / structurally-compatible `Duplex`. Omit for the stdout fd.
     * @param transportKind reported to the `on_serve_start` hook (default `PIPE`).
     */
    serveConnection(readable: ReadableStream<Uint8Array> | NodeJS.ReadableStream, writable?: number | import("node:net").Socket, transportKind?: TransportKind): Promise<void>;
    private serveOne;
}
//# sourceMappingURL=server.d.ts.map