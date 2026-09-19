import type { Schema } from "@query-farm/apache-arrow";
import { type HttpServerCapabilities } from "./capabilities.js";
import { type ServiceDescription } from "./introspect.js";
import type { RawBatch, RawStreamSession } from "./raw.js";
import { HttpStreamSession } from "./stream.js";
import type { HttpConnectOptions, StreamSession } from "./types.js";
import { type UploadUrlPair } from "./uploadUrl.js";
/** A connected RPC client, returned by {@link httpConnect}, {@link pipeConnect}, and {@link subprocessConnect}. */
export interface RpcClient {
    /** Invoke a unary method. Returns the single result row, or `null` for void methods. Parameter defaults from the server's description are applied automatically. */
    call(method: string, params?: Record<string, any>): Promise<Record<string, any> | null>;
    /** Open a streaming method, returning a {@link StreamSession} for exchange or producer iteration. */
    stream(method: string, params?: Record<string, any>): Promise<StreamSession>;
    /**
     * Invoke a unary method from an already-encoded request batch.
     *
     * The batch-level twin of {@link RpcClient.call}, for a caller that holds
     * encoded Arrow rather than values: `input` crosses verbatim (schema,
     * buffers and custom metadata alike), and the reply comes back as the
     * server encoded it. Resolves to `null` when the method returns nothing.
     *
     * `input.metadata` is the call's dispatch metadata and must already carry
     * `vgi_rpc.method` and `vgi_rpc.protocol`; nothing here supplies a default
     * for either. Unlike {@link RpcClient.call} this applies no parameter
     * defaults and needs no introspection round trip.
     */
    callRaw(method: string, input: RawBatch): Promise<RawBatch | null>;
    /**
     * Open a streaming method from an already-encoded request batch.
     *
     * `options.isExchange` and `options.hasHeader` come from the method's
     * declaration — this is the batch-level path, so there is no description to
     * read them from and no name-shaped guess worth making.
     */
    streamRaw(method: string, input: RawBatch, options: {
        isExchange: boolean;
        hasHeader: boolean;
    }): Promise<RawStreamSession>;
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
    /** Open a streaming method from an already-encoded request batch. */
    streamRaw(method: string, input: RawBatch, options: {
        isExchange: boolean;
        hasHeader: boolean;
    }): Promise<HttpStreamSession>;
    /** Discover what this server advertises on `OPTIONS {prefix}/health`. */
    capabilities(): Promise<HttpServerCapabilities>;
    /** Ask the server for `count` pre-signed upload/download URL pairs. */
    requestUploadUrls(count?: number): Promise<UploadUrlPair[]>;
    /**
     * Enter a sticky-session scope on this connection.
     *
     * Every subsequent request carries `VGI-Session-Accept: true` (the
     * server-side opt-in), the session token once the server has minted one,
     * and any `VGI-Echo-<name>` headers the server asked to have echoed back —
     * which is how a session survives a load balancer that has no cookie to
     * work with. Pass `token` to resume a session the server already holds.
     *
     * Scoped, not permanent: {@link HttpRpcClient.endSession} closes it.
     */
    beginSession(token?: string | null): void;
    /** The session token in flight, or `null` when no session is open. */
    currentSessionToken(): string | null;
    /**
     * The `VGI-Echo-*` values captured when the session opened, keyed by the
     * header name to replay them under. Empty when there are none.
     */
    currentEchoHeaders(): Record<string, string>;
    /**
     * Hand the session token to the caller and stop tracking it, so
     * {@link HttpRpcClient.endSession} leaves the server-side session alive for
     * a later {@link HttpRpcClient.beginSession} to resume.
     */
    detachSession(): string | null;
    /**
     * Leave the sticky-session scope, closing the server-side session with a
     * best-effort `DELETE {prefix}/__session__` unless it was detached.
     */
    endSession(): Promise<void>;
}
/**
 * Connect to a vgi-rpc server over HTTP. The returned client lazily introspects
 * the server via `vgi_rpc.Reflection.v1` (caching the result) on the first call and transparently handles
 * zstd compression, authorization, and 413 request externalization.
 */
export declare function httpConnect(rawBaseUrl: string, options?: HttpConnectOptions): HttpRpcClient;
//# sourceMappingURL=connect.d.ts.map