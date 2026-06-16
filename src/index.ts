// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

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
  type ServeUnixHandle,
  type ServeUnixOptions,
  type SocketPaths,
  type StatusRow,
  serveUnix,
  socketPaths,
  statusRows,
  tryAcquireLock,
} from "./launcher/index.js";
