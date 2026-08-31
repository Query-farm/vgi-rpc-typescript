/** Strict, credential-free SOCKS5h support for the Node client transports. */
import { type Socket } from "node:net";
import type { HttpRpcClient, RpcClient } from "./connect.js";
import type { Socks5hHttpConnectOptions, Socks5hTcpConnectOptions } from "./types.js";
/** Parsed address of a credential-free SOCKS5h proxy. */
export interface Socks5hProxy {
    /** Proxy host name or normalized IP literal. */
    readonly host: string;
    /** Proxy TCP port in the range 1 through 65535. */
    readonly port: number;
}
/** Parse a credential-free `socks5h://host:port` URI. */
export declare function parseSocks5hProxy(value: string): Socks5hProxy;
/**
 * Open one TCP tunnel through a SOCKS5h proxy. Target DNS names are encoded as
 * SOCKS domain names and therefore resolved only by the proxy.
 */
export declare function dialSocks5h(proxyUri: string | Socks5hProxy, targetHost: string, targetPort: number, options?: {
    connectTimeoutMs?: number;
    signal?: AbortSignal;
}): Promise<Socket>;
/** Connect the raw Arrow/TCP client through SOCKS5h. */
export declare function tcpConnectSocks5h(host: string, port: number, proxy: string, options?: Socks5hTcpConnectOptions): Promise<RpcClient>;
/** Create a Node fetch implementation whose every connection uses SOCKS5h. */
export declare function createSocks5hFetch(proxy: string, connectTimeoutMs?: number, requestTimeoutMs?: number, maxResponseBytes?: number, maxResponseHeaderBytes?: number): typeof fetch;
/** Connect the HTTP RPC client through SOCKS5h with no direct fallback. */
export declare function httpConnectSocks5h(baseUrl: string, proxy: string, options?: Socks5hHttpConnectOptions): HttpRpcClient;
//# sourceMappingURL=socks5h.d.ts.map