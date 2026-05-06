// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Cross-language conformance access-log hook.
 *
 * Emits one JSON record per RPC dispatch to a {@link Sink} (typically a file
 * descriptor opened in append mode).  The record shape conforms to the
 * vgi-rpc access-log specification (`docs/access-log-spec.md` and
 * `vgi_rpc/access_log.schema.json` in the Python reference repo).
 *
 * Use {@link AccessLogHook} to align this implementation with `vgi-rpc-test
 * --access-log` so worker behaviour is checked across language ports by the
 * same tool that gates the conformance suite.
 */

import type { CallStatistics, DispatchHook, DispatchInfo, HookToken } from "./types.js";

/** Where the hook writes formatted JSON lines. */
export interface AccessLogSink {
  write(line: string): void;
}

// Indirect-string require so esbuild can't pull node:fs into the bundle.
// Workers should use a custom sink (e.g., one backed by `console.log`).
const _NODE_FS_MOD = "node:fs";
function _loadWriteSync(): (fd: number, data: Uint8Array, offset?: number, len?: number) => number {
  const req: any = (globalThis as any).require ?? null;
  if (!req) {
    throw new Error(
      "FdSink requires Node.js or Bun (node:fs.writeSync). For other runtimes, " +
        "supply a custom AccessLogSink that wraps console.log or your logger.",
    );
  }
  return req(_NODE_FS_MOD).writeSync;
}

/** A sink backed by a file descriptor; uses synchronous writes for ordering. */
export class FdSink implements AccessLogSink {
  private readonly _writeSync = _loadWriteSync();
  constructor(private readonly fd: number) {}
  write(line: string): void {
    const buf = new TextEncoder().encode(line);
    let offset = 0;
    while (offset < buf.length) {
      const n = this._writeSync(this.fd, buf, offset, buf.length - offset);
      if (n <= 0) throw new Error(`access-log writeSync returned ${n}`);
      offset += n;
    }
  }
}

interface StartToken {
  startNs: bigint;
}

function rfc3339Utc(): string {
  const d = new Date();
  const yyyy = d.getUTCFullYear().toString().padStart(4, "0");
  const mm = (d.getUTCMonth() + 1).toString().padStart(2, "0");
  const dd = d.getUTCDate().toString().padStart(2, "0");
  const hh = d.getUTCHours().toString().padStart(2, "0");
  const mi = d.getUTCMinutes().toString().padStart(2, "0");
  const ss = d.getUTCSeconds().toString().padStart(2, "0");
  const ms = d.getUTCMilliseconds().toString().padStart(3, "0");
  return `${yyyy}-${mm}-${dd}T${hh}:${mi}:${ss}.${ms}Z`;
}

function base64(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString("base64");
}

/** Round to 2 decimal places. */
function roundTo2(f: number): number {
  return Math.round(f * 100) / 100;
}

export class AccessLogHook implements DispatchHook {
  constructor(
    private readonly sink: AccessLogSink,
    private readonly serverVersion = "",
  ) {}

  onDispatchStart(_info: DispatchInfo): HookToken {
    const token: StartToken = { startNs: process.hrtime.bigint() };
    return token;
  }

  onDispatchEnd(token: HookToken, info: DispatchInfo, stats: CallStatistics, error?: Error): void {
    const t = token as StartToken | undefined;
    const durationMs = t
      ? roundTo2(Number(process.hrtime.bigint() - t.startNs) / 1_000_000)
      : 0;

    const status = error ? "error" : "ok";
    const errType = error ? (error as Error & { type?: string }).type ?? error.constructor.name : "";
    const errMsg = error?.message ?? "";

    const protocol = info.protocol ?? "";
    const rec: Record<string, unknown> = {
      timestamp: rfc3339Utc(),
      level: "INFO",
      logger: "vgi_rpc.access",
      message: `${protocol}.${info.method} ${status}`,
      server_id: info.serverId,
      protocol,
      protocol_hash: info.protocolHash ?? "",
      method: info.method,
      method_type: info.methodType,
      principal: info.principal ?? "",
      auth_domain: info.authDomain ?? "",
      authenticated: info.authenticated ?? false,
      remote_addr: info.remoteAddr ?? "",
      duration_ms: durationMs,
      status,
      error_type: errType,
    };

    if (errMsg) rec.error_message = errMsg;
    if (this.serverVersion) rec.server_version = this.serverVersion;
    if (info.protocolVersion) rec.protocol_version = info.protocolVersion;
    if (info.requestId) rec.request_id = info.requestId;
    if (info.requestData && info.requestData.length > 0) {
      rec.request_data = base64(info.requestData);
    }
    if (info.methodType === "stream") {
      rec.stream_id = info.streamId ?? "00000000000000000000000000000000";
    }
    if (info.cancelled) rec.cancelled = true;

    if (
      stats.inputBatches +
        stats.outputBatches +
        stats.inputRows +
        stats.outputRows +
        stats.inputBytes +
        stats.outputBytes !==
      0
    ) {
      rec.input_batches = stats.inputBatches;
      rec.output_batches = stats.outputBatches;
      rec.input_rows = stats.inputRows;
      rec.output_rows = stats.outputRows;
      rec.input_bytes = stats.inputBytes;
      rec.output_bytes = stats.outputBytes;
    }

    try {
      this.sink.write(`${JSON.stringify(rec)}\n`);
    } catch {
      // best-effort
    }
  }
}
