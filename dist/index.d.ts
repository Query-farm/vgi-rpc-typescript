export { createSocks5hFetch, dialSocks5h, httpConnectSocks5h, parseSocks5hProxy, type Socks5hProxy, tcpConnectSocks5h, } from "./client/socks5h.js";
export { tcpConnect } from "./client/tcp.js";
export * from "./index.core.js";
export { acquireLock, computeHash as launcherComputeHash, defaultStateDir, type FileLockHandle, type GcResult, gcStateDir, type LaunchConfig, launch, probeSocket, type ServeTcpHandle, type ServeTcpOptions, type ServeUnixHandle, type ServeUnixOptions, type SocketPaths, type StatusRow, serveTcp, serveUnix, socketPaths, statusRows, tryAcquireLock, } from "./launcher/index.js";
export { DEFAULT_MAX_PROXY_V2_BYTES, formatProxyEndpoint, normalizeProxyIpAddress, type ProxyProtocolV2Address, type ProxyProtocolV2Endpoint, ProxyProtocolV2Error, type ProxyProtocolV2IrohIdentity, parseIrohProxyProtocolV2, parseProxyProtocolV2, readIrohProxyProtocolV2, readProxyProtocolV2, VGI_IROH_ENDPOINT_TLV, } from "./launcher/proxy-protocol-v2.js";
export { type TailscaleLocalApiOptions, type TailscaleServeOptions, tailscaleLocalApiIdentityProvider, tailscaleServeIdentityProvider, } from "./tailscale.js";
//# sourceMappingURL=index.d.ts.map