// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Cloudflare Worker / browser entry. Re-exports only the runtime-agnostic core
// — the node-only AF_UNIX launcher (node:net `createServer`, node:fs, node:
// child_process) is deliberately excluded so it never enters a workerd/browser
// bundle. A Worker cannot listen on sockets, so the launcher has no use there.
// Resolved via the `workerd`/`worker`/`browser` export conditions in
// package.json.
export * from "./index.core.js";
