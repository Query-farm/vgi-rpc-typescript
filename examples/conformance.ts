// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Conformance worker — 48-method reference RPC service exercising all framework
 * capabilities. Used by the Python CLI to verify wire-protocol compatibility.
 *
 * Flags:
 *   --access-log <path>   Append JSONL access-log records to <path>.
 *
 * Run: bun run examples/conformance.ts
 */
import { openSync } from "node:fs";
import { AccessLogHook, FdSink, VgiRpcServer } from "../src/index.js";
import { protocol } from "./conformance-protocol.js";

const args = process.argv.slice(2);
let accessLogPath: string | undefined;
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--access-log" && i + 1 < args.length) {
    accessLogPath = args[++i];
  }
}

let dispatchHook: AccessLogHook | undefined;
if (accessLogPath) {
  const fd = openSync(accessLogPath, "a");
  dispatchHook = new AccessLogHook(new FdSink(fd), "vgi-rpc-typescript-conformance");
}

const server = new VgiRpcServer(protocol, { enableDescribe: true, dispatchHook });
server.run();
