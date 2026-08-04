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
/** One assembled access-log record, keyed by the spec's snake_case field names. */
type AccessRecord = Record<string, unknown>;
/** Placeholder substituted for a sensitive claim value. */
export declare const REDACTED = "[redacted]";
/** Policy applied to `claims` before they reach a record. */
export type ClaimRedactor = (claims: Record<string, unknown>) => Record<string, unknown>;
/**
 * Replace sensitive claim *values* with {@link REDACTED}.
 *
 * Matching is on the name a value arrived under, never on its content: a
 * claim called `context` holding an email address is not caught, and cannot
 * be without guessing at free text — a boundary worth stating rather than
 * pretending to exceed.
 *
 * Values are replaced rather than dropped so the record still shows which
 * claims the credential carried. "Did this token carry an `email` claim?" is
 * exactly what an audit log exists to answer; "what was it?" is not.
 */
export declare function redactClaims(claims: Record<string, unknown>): Record<string, unknown>;
/** Pass claims through verbatim. Only for logs you own end to end. */
export declare function noRedaction(claims: Record<string, unknown>): Record<string, unknown>;
/** W3C trace identifiers of the span a call ran under. */
export interface TraceContext {
    /** 32 lowercase hex characters. */
    traceId: string;
    /** 16 lowercase hex characters. */
    spanId: string;
}
/** Resolves the trace context of the *currently active* span, or `null`. */
export type TraceContextResolver = () => TraceContext | null;
/**
 * Read `trace_id` / `span_id` from whatever span is current.
 *
 * Reading the *active* span rather than something the framework threads
 * through is what lets a record correlate with an application-opened span as
 * readily as with one opened by `createOtelHook`. Never throws: an
 * observability failure must not surface as a request failure.
 */
export declare function otelTraceContext(): TraceContext | null;
/**
 * Keep a deterministic fraction of access-log records.
 *
 * Three properties separate a sampler that helps from one that quietly costs
 * someone an incident, and all three are enforced here:
 *
 * **Errors are never sampled out.** A rate below 1 exists because successful
 * calls are repetitive, which is exactly what failures are not. A consumer
 * must be able to read a fall in error count as a fix landing rather than as
 * the dice going the other way.
 *
 * **The decision is per call, not per record.** It is a function of a stable
 * identifier — `stream_id` when present, `request_id` otherwise — so every
 * record of one stream shares its init's fate. Random per-record sampling
 * shreds a multi-record call into fragments indistinguishable from data loss,
 * and the calls likeliest to be split are the long streams worth studying.
 *
 * **The rate rides on each kept record** as `sample_rate`. A consumer scaling
 * counts has to divide by it, and a rate discoverable only from a
 * deployment's flags is a rate that gets guessed wrong.
 */
export declare class AccessLogSampler {
    private readonly rate;
    private readonly threshold;
    /** @throws RangeError when `rate` is outside 0.0–1.0 — at construction, so a
     *  rate of `100` meaning "100%" fails at startup rather than silently
     *  logging everything from the first request onward. */
    constructor(rate: number);
    /**
     * Decide whether `record` survives, stamping `sample_rate` when it does.
     *
     * @param record - The assembled record; mutated when kept under a rate < 1.
     * @param key - Stable per-call identifier the decision hashes.
     */
    keep(record: AccessRecord, key: string): boolean;
}
/**
 * Options for {@link AccessLogHook}.
 *
 * `level` matches Python's logger-level gating in `_emit_access_log`: at
 * "INFO" the heavy `request_data` field (a base64 of the full request batch —
 * typically 8+ KiB per init RPC) is replaced with a
 * `truncated: "payload_omitted"` marker plus `original_request_bytes`, so the
 * access-log schema's "unary requires request_data unless truncated"
 * invariant still holds. Bump to "DEBUG" to capture full payloads for
 * replay/audit.
 */
export interface AccessLogOptions {
    /** Server version string (optional). */
    serverVersion?: string;
    /** Verbosity for heavy fields. Default: "INFO". */
    level?: "INFO" | "DEBUG";
    /**
     * Per-record byte cap. Records above it shed fields in the order the spec
     * mandates (`request_data`, then `claims`, then a sentinel form) so log
     * shippers with a per-line ceiling do not drop the line outright. Default:
     * 1 MiB. Pass `0` to disable.
     */
    maxRecordBytes?: number;
    /**
     * Fraction of *successful* calls to log, 0.0–1.0. Default 1.0 (log
     * everything). Errors are always logged. See {@link AccessLogSampler}.
     *
     * @throws RangeError at construction when out of range.
     */
    sampleRate?: number;
    /** Drain records through a bounded queue instead of writing inline.
     *  Opt-in: it trades the guarantee that a record on disk means the call
     *  completed. Default: false. */
    async?: boolean;
    /** Queue depth when `async` is set. Default: 10000. */
    queueSize?: number;
    /** Trace-context source. Defaults to {@link otelTraceContext}, which reads
     *  the active OpenTelemetry span when `@opentelemetry/api` is installed and
     *  is a no-op otherwise. Pass `() => null` to disable. */
    traceContext?: TraceContextResolver;
    /** Claim-redaction policy. Defaults to {@link redactClaims}; pass
     *  {@link noRedaction} for a service that owns its logs end to end. */
    redactor?: ClaimRedactor;
}
export declare class AccessLogHook implements DispatchHook {
    private readonly sink;
    private readonly serverVersion;
    private readonly level;
    private readonly maxRecordBytes;
    private readonly sampler;
    private readonly traceContext;
    private readonly redactor;
    private readonly queue;
    constructor(sink: AccessLogSink, options?: AccessLogOptions | string);
    /** Drain any queued records immediately. No-op for a synchronous hook;
     *  call it on shutdown when `async` is enabled. */
    flush(): void;
    /** Records lost to a full queue and not yet reported. Diagnostics only —
     *  the count is reported in-band on the next record through. */
    get droppedRecords(): number;
    /** Capture a high-resolution start timestamp; returned token feeds {@link onDispatchEnd}. */
    onDispatchStart(_info: DispatchInfo): HookToken;
    /** Emit one access-log JSON record for the completed dispatch (best-effort;
     *  write errors are swallowed so logging never breaks a request). */
    onDispatchEnd(token: HookToken, info: DispatchInfo, stats: CallStatistics, error?: Error): void;
    /** Apply the configured redactor, failing closed if it throws. */
    private redact;
    private currentTrace;
    private emit;
    private write;
    /**
     * Serialize `rec`, shedding fields in the spec's order when it exceeds the
     * per-record cap.
     *
     * Downstream shippers (Vector's `file` source, Fluent Bit's `tail` input)
     * silently drop lines longer than their per-line ceiling, so an oversized
     * record is not a large record — it is a missing one. `error_message` is
     * never truncated: operators rely on the full server-side message, so the
     * sentinel form preserves it even while dropping everything else.
     */
    private format;
}
export {};
//# sourceMappingURL=access-log.d.ts.map