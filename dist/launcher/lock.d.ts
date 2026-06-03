/** Result of a successful lock acquisition. */
export interface FileLockHandle {
    /** Path to the lockfile (informational). */
    readonly path: string;
    /** Release the lock — truncates the file to zero bytes; the file
     *  itself persists as a slot marker.  Idempotent. */
    release(): void;
}
/**
 * Try to acquire the lock once, non-blocking.
 *
 * Returns a release callback on success, or `null` when the lock is
 * held by another live process.  Stale stamps (PID not alive) are
 * cleared and the call retries.
 */
export declare function tryAcquireLock(lockPath: string): FileLockHandle | null;
/** Async version that polls until the lock is acquired or the timeout fires. */
export declare function acquireLock(lockPath: string, timeoutMs: number): Promise<FileLockHandle>;
//# sourceMappingURL=lock.d.ts.map