// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Generic Unix-socket worker launcher — TypeScript port of
 * `vgi_rpc.launcher.launch`.
 *
 * Coordinates spawn-or-reuse of long-running worker processes that serve
 * RPC over `AF_UNIX` sockets.  Designed for clients that want a warm
 * worker without managing its lifecycle themselves.
 *
 * Architecture (cross-language identical to the Python implementation):
 *
 * - The launcher derives a deterministic socket path from a hash of the
 *   worker command tuple (cmd + args + cwd + `VGI_RPC_*` env), so the
 *   same worker is reused across unrelated callers.
 * - Concurrent first-callers serialise on a per-hash lockfile.
 * - Each spawned worker self-terminates after `idleTimeout` seconds with
 *   zero connected clients (the worker side enforces this — see the
 *   `serveUnix` runner in the same module for TS workers, or
 *   `vgi_rpc.rpc.serve_unix` for Python).
 *
 * Worker contract — across language ports:
 *
 * - Accept `--unix PATH` and `--idle-timeout SEC` on the command line.
 * - Emit exactly one line `UNIX:<absolute-path>\n` on **stdout** (flushed)
 *   once bind+listen succeed.  Write nothing further to stdout afterward.
 * - Tolerate (or suppress) stdout noise *before* the bind line — the
 *   launcher skips non-`UNIX:` prefix lines for resilience.
 */

import { type ChildProcessWithoutNullStreams, spawn } from "node:child_process";
import { createWriteStream, unlinkSync } from "node:fs";

import { computeHash } from "./hash.js";
import { acquireLock, tryAcquireLock } from "./lock.js";
import { defaultStateDir, gcStateDir, probeSocket, socketPaths, writeMeta } from "./state.js";

/** Maximum number of stale entries the opportunistic in-launch GC scans. */
const DEFAULT_GC_LIMIT = 16;

/** Inputs to {@link launch}. */
export interface LaunchConfig {
  /** The worker command and its arguments.  Must be non-empty. */
  workerArgv: readonly string[];
  /** Explicit socket path; when omitted, derived from the hash of the tuple. */
  socketPath?: string;
  /** Worker self-shutdown after this many seconds idle.  Forwarded as
   *  `--idle-timeout SEC`.  Default: 300. */
  idleTimeout?: number;
  /** Maximum seconds to block waiting for the per-hash file lock.  Default: 30. */
  connectTimeout?: number;
  /** Maximum seconds to wait for the worker to print `UNIX:<path>`.  Default: 60. */
  workerStartupTimeout?: number;
  /** If set, worker stderr is appended to this file; otherwise discarded. */
  workerStderr?: string;
  /** Override the default state directory. */
  stateDir?: string;
}

/**
 * Ensure a worker is running and return its socket path.
 *
 * Either the existing worker for this hash is reused (probe succeeds) or
 * a fresh one is spawned under flock.  Throws on any failure to bring up
 * a worker.
 */
export async function launch(config: LaunchConfig): Promise<string> {
  if (!config.workerArgv || config.workerArgv.length === 0) {
    throw new Error("workerArgv must be non-empty");
  }
  const stateDir = config.stateDir ?? defaultStateDir();
  const idleTimeout = config.idleTimeout ?? 300;
  const connectTimeoutMs = (config.connectTimeout ?? 30) * 1000;
  const startupTimeoutMs = (config.workerStartupTimeout ?? 60) * 1000;

  let lockPath: string;
  let sockPath: string;
  let metaPath: string | null;
  let hashId: string | null;

  if (config.socketPath !== undefined) {
    const { resolve } = await import("node:path");
    sockPath = resolve(config.socketPath);
    // Explicit paths get a sibling lock, no .meta, skipped by status/gc.
    lockPath = `${sockPath}.lock`;
    metaPath = null;
    hashId = null;
  } else {
    hashId = await computeHash(config.workerArgv);
    const paths = socketPaths(stateDir, hashId);
    lockPath = paths.lockPath;
    sockPath = paths.sockPath;
    metaPath = paths.metaPath;
  }

  const handle = await acquireLock(lockPath, connectTimeoutMs);
  try {
    // Probe — maybe a worker is already serving for this hash.
    if (await probeSocket(sockPath)) {
      return sockPath;
    }
    // Stale socket cleanup.
    try {
      unlinkSync(sockPath);
    } catch {
      // ENOENT is normal; anything else is broadened away (matches Python's
      // OSError suppression for Windows ERROR_SHARING_VIOLATION).
    }
    if (metaPath !== null) {
      writeMeta(metaPath, config.workerArgv, process.cwd(), sockPath);
    }
    await spawnWorker(config.workerArgv, sockPath, idleTimeout, config.workerStderr ?? null, startupTimeoutMs);
    return sockPath;
  } finally {
    handle.release();
    // Opportunistic GC after release — bounded so it can't dominate runtime.
    if (hashId !== null) {
      try {
        await gcStateDir(
          stateDir,
          async (p) => {
            const h = tryAcquireLock(p);
            return h ? () => h.release() : null;
          },
          { limit: DEFAULT_GC_LIMIT, excludeHash: hashId },
        );
      } catch {
        // GC is best-effort.
      }
    }
  }
}

/** Spawn the worker, wait for `UNIX:<path>` on stdout, return when ready. */
async function spawnWorker(
  workerArgv: readonly string[],
  sockPath: string,
  idleTimeout: number,
  workerStderr: string | null,
  startupTimeoutMs: number,
): Promise<void> {
  const fullArgv = [...workerArgv, "--unix", sockPath, "--idle-timeout", String(idleTimeout)];
  const [cmd, ...rest] = fullArgv;

  const stderrTarget = workerStderr === null ? "ignore" : "pipe";

  const proc = spawn(cmd, rest, {
    stdio: ["ignore", "pipe", stderrTarget],
    detached: false,
  }) as ChildProcessWithoutNullStreams;

  if (workerStderr !== null && proc.stderr) {
    // Append mode so multiple worker generations share one log file.
    const sink = createWriteStream(workerStderr, { flags: "a" });
    proc.stderr.pipe(sink);
  }

  const expectedPrefix = `UNIX:${sockPath}`;

  // Read line-by-line from stdout until we see the bind announcement.
  const reader = lineReader(proc.stdout);
  const deadline = Date.now() + startupTimeoutMs;

  while (Date.now() < deadline) {
    // Race the next stdout line against the worker's exit and the deadline.
    const remaining = deadline - Date.now();
    const result = await Promise.race([
      reader.next().then((r) => ({ kind: "line" as const, value: r })),
      onceExit(proc).then((rc) => ({ kind: "exit" as const, rc })),
      delay(remaining).then(() => ({ kind: "timeout" as const })),
    ]);

    if (result.kind === "exit") {
      throw new Error(`worker exited before readiness (rc=${result.rc})`);
    }
    if (result.kind === "timeout") {
      proc.kill("SIGTERM");
      throw new Error(`worker did not emit UNIX:<path> within ${startupTimeoutMs}ms`);
    }
    if (result.value.done) {
      // stdout closed without the announcement.
      const rc = await onceExit(proc);
      throw new Error(`worker exited before readiness (rc=${rc})`);
    }
    const line = result.value.value;
    if (line.startsWith("UNIX:")) {
      if (line !== expectedPrefix) {
        proc.kill("SIGTERM");
        throw new Error(
          `worker bound to unexpected path: ${JSON.stringify(line)} (expected ${JSON.stringify(expectedPrefix)})`,
        );
      }
      // Drain remaining stdout so a buffer-full doesn't deadlock the worker.
      reader.drainAndDiscard();
      return;
    }
    // Non-matching prefix — third-party noise; log at debug and keep reading.
    process.env.VGI_RPC_LAUNCHER_DEBUG &&
      process.stderr.write(`launcher: skipping pre-bind stdout line: ${JSON.stringify(line)}\n`);
  }
  proc.kill("SIGTERM");
  throw new Error(`worker did not emit UNIX:<path> within ${startupTimeoutMs}ms`);
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

interface LineReader {
  next(): Promise<{ done: boolean; value: string }>;
  drainAndDiscard(): void;
}

/** Newline-delimited line reader over a Node Readable stream. */
function lineReader(stream: NodeJS.ReadableStream): LineReader {
  let buffer = "";
  let ended = false;
  const queued: string[] = [];
  const waiters: Array<(line: { done: boolean; value: string }) => void> = [];
  let discardMode = false;

  const flushWaiter = () => {
    if (waiters.length === 0) return;
    if (queued.length > 0) {
      const w = waiters.shift();
      w?.({ done: false, value: queued.shift() ?? "" });
    } else if (ended) {
      const w = waiters.shift();
      w?.({ done: true, value: "" });
    }
  };

  stream.setEncoding?.("utf8");
  stream.on("data", (chunk) => {
    if (discardMode) return;
    buffer += String(chunk);
    for (;;) {
      const nl = buffer.indexOf("\n");
      if (nl < 0) break;
      const line = buffer.slice(0, nl).replace(/\r$/, "");
      buffer = buffer.slice(nl + 1);
      queued.push(line);
    }
    flushWaiter();
  });
  stream.on("end", () => {
    ended = true;
    if (buffer.length > 0) {
      queued.push(buffer.replace(/\r$/, ""));
      buffer = "";
    }
    flushWaiter();
  });
  stream.on("error", () => {
    ended = true;
    flushWaiter();
  });

  return {
    next() {
      return new Promise((resolve) => {
        waiters.push(resolve);
        flushWaiter();
      });
    },
    drainAndDiscard() {
      discardMode = true;
      // Drop any queued lines and let the stream flow into the void.
      queued.length = 0;
      stream.resume?.();
    },
  };
}

function onceExit(proc: ChildProcessWithoutNullStreams): Promise<number | null> {
  return new Promise((resolve) => {
    proc.once("exit", (code) => resolve(code));
  });
}

function delay(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, Math.max(0, ms)));
}
