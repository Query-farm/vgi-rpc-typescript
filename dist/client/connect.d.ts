import { type ServiceDescription } from "./introspect.js";
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
/**
 * Connect to a vgi-rpc server over HTTP. The returned client lazily introspects
 * the server (caching `__describe__`) on the first call and transparently handles
 * zstd compression, authorization, and 413 request externalization.
 */
export declare function httpConnect(baseUrl: string, options?: HttpConnectOptions): RpcClient;
//# sourceMappingURL=connect.d.ts.map