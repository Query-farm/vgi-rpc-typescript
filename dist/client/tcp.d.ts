import type { RpcClient } from "./connect.js";
import type { TcpConnectOptions } from "./types.js";
/**
 * Connect to a vgi-rpc server over a raw TCP socket and wrap it with
 * {@link pipeConnect}. The network analog of `subprocessConnect`: identical
 * lockstep raw Arrow-IPC framing, only the transport differs. Nagle's
 * algorithm is disabled (`setNoDelay(true)`) so lockstep requests are not
 * delayed coalescing writes. The returned client's {@link RpcClient.close}
 * also destroys the socket.
 *
 * SECURITY: raw TCP carries **no authentication or TLS** — only connect to
 * trusted endpoints. Use `httpConnect` for untrusted networks.
 *
 * @param host Hostname or IP address of the TCP server.
 * @param port TCP port of the server.
 * @param options Optional log/external-location configuration.
 */
export declare function tcpConnect(host: string, port: number, options?: TcpConnectOptions): RpcClient;
//# sourceMappingURL=tcp.d.ts.map