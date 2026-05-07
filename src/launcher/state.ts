// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * State directory + lock/sock/meta path layout for the AF_UNIX worker
 * launcher.  Mirrors `vgi_rpc.launcher` so cross-language tooling
 * resolves to identical filesystem locations.
 */

import { existsSync, mkdirSync, readFileSync, statSync, unlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import * as path from "node:path";

import { computeHash } from "./hash.js";

/** Filesystem layout for one worker tuple: `<state_dir>/<hash>.{lock,sock,meta}`. */
export interface SocketPaths {
  lockPath: string;
  sockPath: string;
  metaPath: string;
}

export function socketPaths(stateDir: string, hashId: string): SocketPaths {
  return {
    lockPath: path.join(stateDir, `${hashId}.lock`),
    sockPath: path.join(stateDir, `${hashId}.sock`),
    metaPath: path.join(stateDir, `${hashId}.meta`),
  };
}

/**
 * Resolve the per-user state directory used for lockfiles + sockets.
 *
 * - Linux: `$XDG_RUNTIME_DIR/vgi-rpc/` when set (systemd-managed,
 *   auto-cleaned on logout); otherwise `$TMPDIR/vgi-rpc-$UID/`.
 * - macOS / BSD: `$TMPDIR/vgi-rpc-$UID/`.
 * - Windows: `$TMP/vgi-rpc/`.
 *
 * The directory is created mode 0700 if missing.  On POSIX, we refuse to
 * operate on a directory not owned by the current user — defends against
 * a hijacked `/tmp/vgi-rpc-$UID` left by an attacker.
 */
export function defaultStateDir(): string {
  let base: string;
  if (process.platform === "win32") {
    base = path.join(tmpdir(), "vgi-rpc");
  } else {
    const xdg = process.env.XDG_RUNTIME_DIR;
    if (xdg) {
      base = path.join(xdg, "vgi-rpc");
    } else {
      const uid = typeof process.geteuid === "function" ? process.geteuid() : 0;
      base = path.join(tmpdir(), `vgi-rpc-${uid}`);
    }
  }
  mkdirSync(base, { recursive: true, mode: 0o700 });
  if (process.platform !== "win32" && typeof process.geteuid === "function") {
    // Tighten mode every call (cheap + idempotent) and refuse a hijacked dir.
    try {
      // chmodSync is in node:fs; using statSync is enough to read the owner.
      const st = statSync(base);
      if (st.uid !== process.geteuid()) {
        throw new Error(`state directory ${base} is not owned by current user`);
      }
    } catch (err) {
      if ((err as { code?: string })?.code === "ENOENT") {
        // Race with the mkdirSync above — leave it to the next caller.
      } else {
        throw err;
      }
    }
  }
  return base;
}

// ---------------------------------------------------------------------------
// Meta file
// ---------------------------------------------------------------------------

/** Best-effort write of human-readable launch metadata.  Used by `--status`. */
export function writeMeta(metaPath: string, workerArgv: readonly string[], cwd: string, sockPath: string): void {
  const payload = {
    cmd: [...workerArgv],
    cwd,
    socket: sockPath,
    started_at: Date.now() / 1000,
    launcher_pid: process.pid,
  };
  try {
    writeFileSync(metaPath, JSON.stringify(payload, null, 2), { encoding: "utf8", mode: 0o600 });
  } catch {
    // observability is best-effort
  }
}

// ---------------------------------------------------------------------------
// Status / GC
// ---------------------------------------------------------------------------

export interface StatusRow {
  hashId: string;
  cmd: string[];
  cwd: string;
  socket: string;
  startedAt: number | null;
  alive: boolean;
}

export interface GcResult {
  /** Hash IDs of stale entries that were removed. */
  cleaned: string[];
  /** Hash IDs whose lockfile is currently held (a launch is in flight or the
   *  worker is alive). */
  skippedInUse: string[];
}

/** Probe whether anyone is currently accepting on `sockPath`. */
export async function probeSocket(sockPath: string, timeoutMs = 2000): Promise<boolean> {
  if (!existsSync(sockPath)) return false;
  // Lazy require so workerd / browser bundles that never call probeSocket
  // don't trip on `node:net`.
  const net = await import("node:net");
  return new Promise<boolean>((resolve) => {
    const sock = net.createConnection({ path: sockPath });
    const timer = setTimeout(() => {
      sock.destroy();
      resolve(false);
    }, timeoutMs);
    sock.once("connect", () => {
      clearTimeout(timer);
      sock.end();
      resolve(true);
    });
    sock.once("error", () => {
      clearTimeout(timer);
      resolve(false);
    });
  });
}

function tryReadMeta(metaPath: string): { cmd: string[]; cwd: string; startedAt: number | null } {
  try {
    const raw = readFileSync(metaPath, "utf8");
    const meta = JSON.parse(raw);
    return {
      cmd: Array.isArray(meta.cmd) ? meta.cmd.map(String) : [],
      cwd: typeof meta.cwd === "string" ? meta.cwd : "",
      startedAt: typeof meta.started_at === "number" ? meta.started_at : null,
    };
  } catch {
    return { cmd: [], cwd: "", startedAt: null };
  }
}

/** List one row per `<hash>.lock` in `stateDir`.  Read-only; takes no locks. */
export async function statusRows(stateDir: string): Promise<StatusRow[]> {
  const { readdirSync } = await import("node:fs");
  const rows: StatusRow[] = [];
  let entries: string[];
  try {
    entries = readdirSync(stateDir);
  } catch {
    return rows;
  }
  for (const name of entries.sort()) {
    if (!name.endsWith(".lock")) continue;
    const hashId = name.slice(0, -5);
    const { sockPath, metaPath } = socketPaths(stateDir, hashId);
    const meta = tryReadMeta(metaPath);
    rows.push({
      hashId,
      cmd: meta.cmd,
      cwd: meta.cwd,
      socket: sockPath,
      startedAt: meta.startedAt,
      alive: await probeSocket(sockPath),
    });
  }
  return rows;
}

/**
 * Remove `<hash>.lock`/`.sock`/`.meta` triples whose worker is no longer
 * accepting connections.
 *
 * @param tryAcquire Function provided by the lock module so we don't pull
 *  the lock implementation into a circular import.  Returns a release
 *  callback when the lock can be acquired non-blocking, or `null` when
 *  it's already held (the worker is alive or another launch is in flight).
 */
export async function gcStateDir(
  stateDir: string,
  tryAcquire: (lockPath: string) => Promise<(() => void) | null>,
  options?: { limit?: number; excludeHash?: string },
): Promise<GcResult> {
  const { readdirSync } = await import("node:fs");
  const cleaned: string[] = [];
  const skipped: string[] = [];
  const limit = options?.limit ?? null;
  const excludeHash = options?.excludeHash ?? null;

  let entries: string[];
  try {
    entries = readdirSync(stateDir);
  } catch {
    return { cleaned, skippedInUse: skipped };
  }

  let seen = 0;
  for (const name of entries.sort()) {
    if (!name.endsWith(".lock")) continue;
    if (limit !== null && seen >= limit) break;
    seen += 1;
    const hashId = name.slice(0, -5);
    if (excludeHash !== null && hashId === excludeHash) continue;

    const { lockPath, sockPath, metaPath } = socketPaths(stateDir, hashId);
    const release = await tryAcquire(lockPath);
    if (release === null) {
      skipped.push(hashId);
      continue;
    }
    try {
      if (await probeSocket(sockPath)) {
        // Worker is alive but didn't hold its own lock — odd, but leave it.
        continue;
      }
      for (const p of [sockPath, metaPath, lockPath]) {
        try {
          unlinkSync(p);
        } catch {
          // best-effort
        }
      }
      cleaned.push(hashId);
    } finally {
      release();
    }
  }
  return { cleaned, skippedInUse: skipped };
}

/** Re-export for callers that want canonical hashing without going through the
 *  launch entry point. */
export { computeHash };
