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
    /** Write one access-log line. The trailing newline is included by the caller. */
    write(line: string): void;
}
/** A sink backed by a file descriptor; uses synchronous writes for ordering. */
export declare class FdSink implements AccessLogSink {
    private readonly fd;
    private readonly _writeSync;
    constructor(fd: number);
    /** Write `line` to the file descriptor, looping until the buffer is fully flushed. */
    write(line: string): void;
}
/**
 * Options for {@link AccessLogHook}.
 *
 * `level` matches Python's logger-level gating in `_emit_access_log`:
 * at "INFO" the heavy `request_data` field (a base64 of the full
 * request batch — typically 8+ KiB per init RPC) is replaced with a
 * `truncated: true` marker plus `original_request_bytes`, so the
 * access-log schema's "unary requires request_data unless truncated"
 * invariant still holds. Bump to "DEBUG" to capture full payloads for
 * replay/audit.
 */
export interface AccessLogOptions {
    /** Server version string (optional). */
    serverVersion?: string;
    /** Verbosity for heavy fields. Default: "INFO". */
    level?: "INFO" | "DEBUG";
}
export declare class AccessLogHook implements DispatchHook {
    private readonly sink;
    private readonly serverVersion;
    private readonly level;
    constructor(sink: AccessLogSink, options?: AccessLogOptions | string);
    /** Capture a high-resolution start timestamp; returned token feeds {@link onDispatchEnd}. */
    onDispatchStart(_info: DispatchInfo): HookToken;
    /** Emit one access-log JSON record for the completed dispatch (best-effort;
     *  write errors are swallowed so logging never breaks a request). */
    onDispatchEnd(token: HookToken, info: DispatchInfo, stats: CallStatistics, error?: Error): void;
}
//# sourceMappingURL=access-log.d.ts.map