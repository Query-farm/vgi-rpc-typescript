// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

export {
  createSocks5hFetch,
  dialSocks5h,
  httpConnectSocks5h,
  parseSocks5hProxy,
  type Socks5hProxy,
  tcpConnectSocks5h,
} from "./client/socks5h.js";
// Node-only raw-TCP client (statically imports `node:net`, so it lives outside
// the runtime-agnostic core that browser/workerd bundles re-export).
export { tcpConnect } from "./client/tcp.js";
// Full node/bun barrel: the runtime-agnostic core plus the node-only AF_UNIX
// launcher. Cloudflare Worker / browser builds resolve `./index.workerd.ts`
// (the `workerd`/`worker`/`browser` export conditions) instead, which omits
// the launcher so node:net/fs/child_process never enter the bundle.
export * from "./index.core.js";
export {
  acquireLock,
  computeHash as launcherComputeHash,
  defaultStateDir,
  type FileLockHandle,
  type GcResult,
  gcStateDir,
  type LaunchConfig,
  launch,
  probeSocket,
  type ServeTcpHandle,
  type ServeTcpOptions,
  type ServeUnixHandle,
  type ServeUnixOptions,
  type SocketPaths,
  type StatusRow,
  serveTcp,
  serveUnix,
  socketPaths,
  statusRows,
  tryAcquireLock,
} from "./launcher/index.js";
export {
  DEFAULT_MAX_PROXY_V2_BYTES,
  formatProxyEndpoint,
  normalizeProxyIpAddress,
  type ProxyProtocolV2Address,
  type ProxyProtocolV2Endpoint,
  ProxyProtocolV2Error,
  type ProxyProtocolV2IrohIdentity,
  parseIrohProxyProtocolV2,
  parseProxyProtocolV2,
  readIrohProxyProtocolV2,
  readProxyProtocolV2,
  VGI_IROH_ENDPOINT_TLV,
} from "./launcher/proxy-protocol-v2.js";
export {
  type TailscaleLocalApiOptions,
  type TailscaleServeOptions,
  tailscaleLocalApiIdentityProvider,
  tailscaleServeIdentityProvider,
} from "./tailscale.js";
