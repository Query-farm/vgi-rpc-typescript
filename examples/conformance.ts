// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Conformance worker — 48-method reference RPC service exercising all framework
 * capabilities. Used by the Python CLI to verify wire-protocol compatibility.
 *
 * Flags:
 *   --access-log <path>   Append JSONL access-log records to <path>.
 *   --access-log-sample R Log only fraction R (0.0-1.0) of successful calls.
 *                         Errors are always logged and every kept record
 *                         carries `sample_rate`. An out-of-range R fails here,
 *                         at startup, not at the first request.
 *   --access-log-async    Drain records through a bounded queue instead of
 *                         writing them inline. Trades durability for latency.
 *   --tcp [HOST:]PORT     Serve over a raw TCP socket instead of stdin/stdout.
 *                         HOST defaults to 127.0.0.1 (loopback only); PORT may
 *                         be 0 to let the OS auto-select. Prints
 *                         `TCP:<host>:<port>` once bound, then nothing more on
 *                         stdout. No auth/TLS — trusted networks only.
 *
 * Run: bun run examples/conformance.ts
 */
import { openSync } from "node:fs";
import { AccessLogHook, FdSink, serveTcp, VgiRpcServer } from "../src/index.js";
import { protocol } from "./conformance-protocol.js";

const args = process.argv.slice(2);
let accessLogPath: string | undefined;
let accessLogSample = 1;
let accessLogAsync = false;
let tcpArg: string | undefined;
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--access-log" && i + 1 < args.length) {
    accessLogPath = args[++i];
  } else if (args[i] === "--access-log-sample" && i + 1 < args.length) {
    accessLogSample = Number.parseFloat(args[++i]);
  } else if (args[i] === "--access-log-async") {
    accessLogAsync = true;
  } else if (args[i] === "--tcp" && i + 1 < args.length) {
    tcpArg = args[++i];
  }
}

let dispatchHook: AccessLogHook | undefined;
if (accessLogPath) {
  const fd = openSync(accessLogPath, "a");
  dispatchHook = new AccessLogHook(new FdSink(fd), {
    serverVersion: "vgi-rpc-typescript-conformance",
    sampleRate: accessLogSample,
    async: accessLogAsync,
  });
  // Queued records are only durable once drained; a worker that exits with a
  // full queue would otherwise lose the tail of its own audit trail.
  if (accessLogAsync) {
    const hook = dispatchHook;
    process.on("exit", () => hook.flush());
  }
}

if (tcpArg !== undefined) {
  // Parse `[HOST:]PORT`, mirroring the Python `--tcp` parsing. Host defaults
  // to loopback. `serveTcp` prints `TCP:<host>:<port>` once bound.
  let host = "127.0.0.1";
  let portPart = tcpArg;
  if (tcpArg.includes(":")) {
    const idx = tcpArg.lastIndexOf(":");
    const hostPart = tcpArg.slice(0, idx);
    if (hostPart) host = hostPart;
    portPart = tcpArg.slice(idx + 1);
  }
  const port = Number.parseInt(portPart, 10);
  if (Number.isNaN(port)) {
    process.stderr.write(`--tcp expects [HOST:]PORT, got '${tcpArg}'\n`);
    process.exit(2);
  }
  const handle = await serveTcp(protocol, {
    host,
    port,
    enableDescribe: true,
    dispatchHook,
    protocolVersion: protocol.protocolVersion,
    idleTimeout: 0,
  });
  await handle.done;
} else {
  const server = new VgiRpcServer(protocol, { enableDescribe: true, dispatchHook });
  server.run();
}
