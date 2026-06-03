import { computeHash } from "./hash.js";
/** Filesystem layout for one worker tuple: `<state_dir>/<hash>.{lock,sock,meta}`. */
export interface SocketPaths {
    /** Advisory lockfile path (`<hash>.lock`) guarding launch + liveness. */
    lockPath: string;
    /** AF_UNIX socket path (`<hash>.sock`) the worker binds. */
    sockPath: string;
    /** Launch-metadata JSON path (`<hash>.meta`) read by `--status`. */
    metaPath: string;
}
/** Derive the lock/sock/meta path triple for a worker `hashId` under `stateDir`. */
export declare function socketPaths(stateDir: string, hashId: string): SocketPaths;
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
export declare function defaultStateDir(): string;
/** Best-effort write of human-readable launch metadata.  Used by `--status`. */
export declare function writeMeta(metaPath: string, workerArgv: readonly string[], cwd: string, sockPath: string): void;
/** One row of `--status` output describing a launched worker tuple. */
export interface StatusRow {
    /** Canonical hash identifying the worker tuple (the `<hash>` filename stem). */
    hashId: string;
    /** Worker argv recorded at launch, or `[]` when the meta file is missing. */
    cmd: string[];
    /** Working directory recorded at launch, or `""` when unknown. */
    cwd: string;
    /** AF_UNIX socket path the worker binds. */
    socket: string;
    /** Unix epoch seconds the worker was launched, or `null` when unknown. */
    startedAt: number | null;
    /** Whether a probe connection to {@link StatusRow.socket} currently succeeds. */
    alive: boolean;
}
/** Outcome of a {@link gcStateDir} sweep. */
export interface GcResult {
    /** Hash IDs of stale entries that were removed. */
    cleaned: string[];
    /** Hash IDs whose lockfile is currently held (a launch is in flight or the
     *  worker is alive). */
    skippedInUse: string[];
}
/** Probe whether anyone is currently accepting on `sockPath`. */
export declare function probeSocket(sockPath: string, timeoutMs?: number): Promise<boolean>;
/** List one row per `<hash>.lock` in `stateDir`.  Read-only; takes no locks. */
export declare function statusRows(stateDir: string): Promise<StatusRow[]>;
/**
 * Remove `<hash>.lock`/`.sock`/`.meta` triples whose worker is no longer
 * accepting connections.
 *
 * @param tryAcquire Function provided by the lock module so we don't pull
 *  the lock implementation into a circular import.  Returns a release
 *  callback when the lock can be acquired non-blocking, or `null` when
 *  it's already held (the worker is alive or another launch is in flight).
 */
export declare function gcStateDir(stateDir: string, tryAcquire: (lockPath: string) => Promise<(() => void) | null>, options?: {
    limit?: number;
    excludeHash?: string;
}): Promise<GcResult>;
/** Re-export for callers that want canonical hashing without going through the
 *  launch entry point. */
export { computeHash };
//# sourceMappingURL=state.d.ts.map