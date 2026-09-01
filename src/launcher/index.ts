// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * AF_UNIX worker launcher — TypeScript port of `vgi_rpc.launcher`.
 *
 * Two halves:
 *
 * - **Coordination** ({@link launch}, {@link computeHash}, {@link gcStateDir},
 *   {@link statusRows}): spawn-or-reuse a long-running worker process for a
 *   given command tuple, returning the AF_UNIX socket path the caller
 *   should connect to.  Hash and on-disk layout match the Python
 *   implementation byte-for-byte so workers in any language under the
 *   same tuple resolve to the same socket.
 *
 * - **Worker runner** ({@link serveUnix}): bind a Unix socket and serve a
 *   {@link Protocol} via per-connection IPC streams.  Implements the
 *   `--unix PATH` / `--idle-timeout SEC` / `UNIX:<path>` contract so
 *   launchers (Python or TS) can spawn TS workers transparently.
 */

export { computeHash } from "./hash.js";
export { type LaunchConfig, launch } from "./launch.js";
export { acquireLock, type FileLockHandle, tryAcquireLock } from "./lock.js";
export {
  DEFAULT_MAX_PROXY_V2_BYTES,
  formatProxyEndpoint,
  normalizeProxyIpAddress,
  type ProxyProtocolV2Address,
  type ProxyProtocolV2Endpoint,
  ProxyProtocolV2Error,
  parseProxyProtocolV2,
  readProxyProtocolV2,
} from "./proxy-protocol-v2.js";
export { type ServeTcpHandle, type ServeTcpOptions, serveTcp } from "./serve-tcp.js";
export { type ServeUnixHandle, type ServeUnixOptions, serveUnix } from "./serve-unix.js";
export {
  defaultStateDir,
  type GcResult,
  gcStateDir,
  probeSocket,
  type SocketPaths,
  type StatusRow,
  socketPaths,
  statusRows,
} from "./state.js";
