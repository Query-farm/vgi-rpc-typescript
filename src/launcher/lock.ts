// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Cross-process file lock with PID-stamp fallback.
 *
 * Python's launcher uses `filelock` (POSIX `flock(2)` / Windows
 * `LockFileEx`), which auto-releases on process death.  Node has no
 * equivalent in its standard library, so we approximate it with a
 * persistent PID-stamp protocol:
 *
 *   - File exists, empty content                 → unlocked (slot marker)
 *   - File exists, content `<PID>`, PID alive    → held by that PID
 *   - File exists, content `<PID>`, PID dead     → stale, treat as unlocked
 *   - File doesn't exist                         → unlocked (slot never used)
 *
 * The lockfile **persists** after release (we truncate to zero bytes
 * rather than unlinking) so cross-language scanners — `statusRows` /
 * `gcStateDir` here and Python's `gc_state_dir` — can use lockfile
 * presence as a "this hash slot has been used at some point" marker
 * even when no launcher is currently coordinating.
 *
 * The acquire path has a small race window: between reading the stamp
 * and writing ours, another process can interleave.  Mitigations:
 *
 *   1. After writing our PID we re-read and verify; on mismatch we
 *      retry up to a small bound, then back off.
 *   2. The launcher's bind() step is itself a kernel mutex — two
 *      workers racing past the lock will see exactly one bind()
 *      succeed; the other fails fast with EADDRINUSE and the second
 *      launcher's spawnWorker() surfaces the error.
 *
 * That's looser than `flock`-based mutual exclusion but adequate for
 * the launcher's use case: the protected critical section is short
 * (probe + spawn).
 */

import { closeSync, constants as FS, openSync, readSync, statSync, writeSync } from "node:fs";

/** Result of a successful lock acquisition. */
export interface FileLockHandle {
  /** Path to the lockfile (informational). */
  readonly path: string;
  /** Release the lock — truncates the file to zero bytes; the file
   *  itself persists as a slot marker.  Idempotent. */
  release(): void;
}

const POLL_MS = 50;
/** Max retries for the post-write verify step. */
const VERIFY_RETRIES = 5;

function pidAlive(pid: number): boolean {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    return (err as { code?: string })?.code === "EPERM";
  }
}

function readPid(path: string): number {
  try {
    const fd = openSync(path, FS.O_RDONLY);
    try {
      const buf = Buffer.alloc(64);
      const n = readSync(fd, buf, 0, buf.length, 0);
      const text = buf.subarray(0, n).toString("utf8").trim();
      if (text === "") return 0;
      const parsed = Number(text);
      return Number.isInteger(parsed) ? parsed : 0;
    } finally {
      closeSync(fd);
    }
  } catch {
    return 0;
  }
}

function tryStampPid(path: string): boolean {
  // Open r/w, creating if missing.  Truncate to zero, write our PID.
  // The natural race here is mitigated by the post-write verify in the
  // caller.
  const fd = openSync(path, FS.O_RDWR | FS.O_CREAT, 0o600);
  try {
    // Truncate via ftruncateSync — Node's fs has it but only on the fd.
    // Use a fresh write at offset 0 with the full string and re-stat to
    // confirm the file is our PID's worth of bytes.
    const stamp = Buffer.from(String(process.pid), "utf8");
    // Truncate by reopening with O_TRUNC would re-create; instead use ftruncateSync.
    // Node 18+ has `fs.ftruncateSync`.
    const { ftruncateSync } = require("node:fs");
    ftruncateSync(fd, 0);
    let written = 0;
    while (written < stamp.length) {
      const n = writeSync(fd, stamp, written, stamp.length - written, 0 + written);
      if (n <= 0) throw new Error(`writeSync returned ${n}`);
      written += n;
    }
    // Sanity stat — confirm the file is our stamp's size (lossy check
    // for the basic interleave race).
    const st = statSync(path);
    if (st.size !== stamp.length) return false;
    return true;
  } finally {
    closeSync(fd);
  }
}

function clearStamp(path: string): void {
  try {
    const fd = openSync(path, FS.O_RDWR);
    try {
      const { ftruncateSync } = require("node:fs");
      ftruncateSync(fd, 0);
    } finally {
      closeSync(fd);
    }
  } catch {
    // already gone
  }
}

/**
 * Try to acquire the lock once, non-blocking.
 *
 * Returns a release callback on success, or `null` when the lock is
 * held by another live process.  Stale stamps (PID not alive) are
 * cleared and the call retries.
 */
export function tryAcquireLock(lockPath: string): FileLockHandle | null {
  for (let attempt = 0; attempt < VERIFY_RETRIES; attempt++) {
    const existingPid = readPid(lockPath);
    if (existingPid > 0 && pidAlive(existingPid)) {
      // Held by a live process (possibly even ourselves on a different
      // call site — match Python's filelock semantics: not reentrant).
      return null;
    }
    // Stale, empty, or missing — try to claim the slot.
    if (!tryStampPid(lockPath)) continue;
    const verifyPid = readPid(lockPath);
    if (verifyPid !== process.pid) {
      // Lost the race to a peer that wrote after our truncate.
      continue;
    }
    let released = false;
    return {
      path: lockPath,
      release() {
        if (released) return;
        released = true;
        clearStamp(lockPath);
      },
    };
  }
  return null;
}

/** Async version that polls until the lock is acquired or the timeout fires. */
export async function acquireLock(lockPath: string, timeoutMs: number): Promise<FileLockHandle> {
  const deadline = Date.now() + Math.max(0, timeoutMs);
  for (;;) {
    const handle = tryAcquireLock(lockPath);
    if (handle) return handle;
    if (Date.now() >= deadline) {
      throw new Error(`failed to acquire ${lockPath} within ${timeoutMs}ms`);
    }
    await new Promise((r) => setTimeout(r, POLL_MS));
  }
}
