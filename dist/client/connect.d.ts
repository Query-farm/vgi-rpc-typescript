import { Schema } from "@query-farm/apache-arrow";
import { type ServiceDescription } from "./introspect.js";
import { HttpStreamSession } from "./stream.js";
import type { HttpConnectOptions, StreamSession } from "./types.js";
/** A connected RPC client, returned by {@link httpConnect}, {@link pipeConnect}, and {@link subprocessConnect}. */
export interface RpcClient {
    /** Invoke a unary method. Returns the single result row, or `null` for void methods. Parameter defaults from `__describe__` are applied automatically. */
    call(method: string, params?: Record<string, any>): Promise<Record<string, any> | null>;
    /** Open a streaming method, returning a {@link StreamSession} for exchange or producer iteration. */
    stream(method: string, params?: Record<string, any>): Promise<StreamSession>;
    /** Fetch the server's method/protocol description (cached after the first call). */
    describe(): Promise<ServiceDescription>;
    /** Release transport resources; for subprocess clients this also terminates the child process. */
    close(): void;
}
/** An HTTP-connected RPC client: {@link RpcClient} plus the HTTP-only continuation-resume surface. */
export interface HttpRpcClient extends RpcClient {
    /** Open a streaming method, returning an {@link HttpStreamSession} for exchange or producer iteration. */
    stream(method: string, params?: Record<string, any>): Promise<HttpStreamSession>;
    /**
     * Resume a producer stream from a continuation `token` without re-binding.
     *
     * A continuation request (`POST /{method}/exchange` carrying only the
     * `STATE_KEY` token) is fully self-describing: the server recovers the
     * producer state, schemas, and function identity from the signed token
     * alone, so no bind/init round-trip is needed. This is the cheap path for a
     * stateless relay that holds a per-batch token (see
     * {@link HttpStreamSession.nextWithToken}) and resumes on any
     * connection/node — unlike `stream(...)` which would produce and discard a
     * fresh first turn before seeking.
     *
     * `token` is the opaque blob from {@link HttpStreamSession.nextWithToken},
     * which packs both the cursor and the call token; the resuming node may
     * never have seen this stream's `/init`, so it needs both.
     *
     * The returned session is positioned at `token`; the first `nextWithToken()`
     * (or iteration) issues the continuation. `outputSchema` is unused on the
     * producer-continuation path (each response's IPC stream carries its own
     * schema) and defaults to the empty schema.
     *
     * Mirrors Python's `_HttpProxy.resume_stream`.
     */
    resumeStream(method: string, token: string, outputSchema?: Schema): Promise<HttpStreamSession>;
}
/**
 * Connect to a vgi-rpc server over HTTP. The returned client lazily introspects
 * the server (caching `__describe__`) on the first call and transparently handles
 * zstd compression, authorization, and 413 request externalization.
 */
export declare function httpConnect(baseUrl: string, options?: HttpConnectOptions): HttpRpcClient;
//# sourceMappingURL=connect.d.ts.map