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
export declare function launch(config: LaunchConfig): Promise<string>;
//# sourceMappingURL=launch.d.ts.map