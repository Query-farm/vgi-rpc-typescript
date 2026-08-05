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
  /** Write one access-log line. The trailing newline is included by the caller. */
  write(line: string): void;
}

// Indirect-string require so esbuild can't pull node:fs into the bundle.
// Workers should use a custom sink (e.g., one backed by `console.log`).
const _NODE_FS_MOD = "node:fs";
function _loadWriteSync(): (fd: number, data: Uint8Array, offset?: number, len?: number) => number {
  const req: any = (import.meta as any).require ?? (globalThis as any).require ?? null;
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
  /** Write `line` to the file descriptor, looping until the buffer is fully flushed. */
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

/** One assembled access-log record, keyed by the spec's snake_case field names. */
type AccessRecord = Record<string, unknown>;

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

const _ENCODER = new TextEncoder();

function utf8Length(value: string): number {
  return _ENCODER.encode(value).byteLength;
}

// ---------------------------------------------------------------------------
// Claim redaction
// ---------------------------------------------------------------------------

/** Placeholder substituted for a sensitive claim value. */
export const REDACTED = "[redacted]";

/** Policy applied to `claims` before they reach a record. */
export type ClaimRedactor = (claims: Record<string, unknown>) => Record<string, unknown>;

// Credential-shaped names first, then the standard OIDC claims that are
// personal data. An access log outlives the token it describes by months or
// years and is shipped to systems chosen for searchability, not for holding
// PII — so `email` / `phone_number` reaching it verbatim is a retention
// problem, not a debugging feature.
const CLAIM_REDACT_RE =
  /password|token|secret|key|authorization|email|phone|address|birthdate|gender|^name$|given_name|family_name|middle_name|nickname|preferred_username|picture|profile|website/i;

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
export function redactClaims(claims: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(claims)) {
    out[k] = CLAIM_REDACT_RE.test(k) ? REDACTED : v;
  }
  return out;
}

/** Pass claims through verbatim. Only for logs you own end to end. */
export function noRedaction(claims: Record<string, unknown>): Record<string, unknown> {
  return { ...claims };
}

// ---------------------------------------------------------------------------
// Trace correlation
// ---------------------------------------------------------------------------

/** W3C trace identifiers of the span a call ran under. */
export interface TraceContext {
  /** 32 lowercase hex characters. */
  traceId: string;
  /** 16 lowercase hex characters. */
  spanId: string;
}

/** Resolves the trace context of the *currently active* span, or `null`. */
export type TraceContextResolver = () => TraceContext | null;

const TRACE_ID_RE = /^[0-9a-f]{32}$/;
const SPAN_ID_RE = /^[0-9a-f]{16}$/;

// Resolved once: `null` until first use, then the OTel trace API or `false`
// when @opentelemetry/api is not installed. A per-call resolution attempt
// would be the most expensive thing on the common path, where it is absent.
// Indirect-string require for the same reason FdSink uses one — the bundler
// must not turn an optional peer dependency into a hard one.
const _OTEL_MOD = "@opentelemetry/api";
let _otelTrace: any = null;

/**
 * Read `trace_id` / `span_id` from whatever span is current.
 *
 * Reading the *active* span rather than something the framework threads
 * through is what lets a record correlate with an application-opened span as
 * readily as with one opened by `createOtelHook`. Never throws: an
 * observability failure must not surface as a request failure.
 */
export function otelTraceContext(): TraceContext | null {
  if (_otelTrace === null) {
    _otelTrace = false;
    try {
      const req: any = (import.meta as any).require ?? (globalThis as any).require ?? null;
      if (req) _otelTrace = req(_OTEL_MOD)?.trace ?? false;
    } catch {
      _otelTrace = false;
    }
  }
  if (!_otelTrace) return null;
  try {
    const ctx = _otelTrace.getActiveSpan()?.spanContext();
    if (!ctx) return null;
    const traceId = String(ctx.traceId ?? "");
    const spanId = String(ctx.spanId ?? "");
    // An unsampled/invalid context carries all-zero ids; emitting those
    // would join every such record to one non-existent trace.
    if (!TRACE_ID_RE.test(traceId) || !SPAN_ID_RE.test(spanId)) return null;
    if (/^0+$/.test(traceId) || /^0+$/.test(spanId)) return null;
    return { traceId, spanId };
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Sampling
// ---------------------------------------------------------------------------

/** FNV-1a, 32-bit. Cheap, well-distributed, and dependency-free — the
 *  decision only has to be stable within a process, not across languages. */
function fnv1a32(text: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < text.length; i++) {
    hash ^= text.charCodeAt(i) & 0xff;
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash >>> 0;
}

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
export class AccessLogSampler {
  private readonly threshold: number;

  /** @throws RangeError when `rate` is outside 0.0–1.0 — at construction, so a
   *  rate of `100` meaning "100%" fails at startup rather than silently
   *  logging everything from the first request onward. */
  constructor(private readonly rate: number) {
    if (!(typeof rate === "number" && Number.isFinite(rate) && rate >= 0 && rate <= 1)) {
      throw new RangeError(`access-log sample rate must be between 0.0 and 1.0, got ${rate}`);
    }
    this.threshold = rate * 0xffffffff;
  }

  /**
   * Decide whether `record` survives, stamping `sample_rate` when it does.
   *
   * @param record - The assembled record; mutated when kept under a rate < 1.
   * @param key - Stable per-call identifier the decision hashes.
   */
  keep(record: AccessRecord, key: string): boolean {
    if (this.rate >= 1) return true;
    if (record.status === "error") return true;
    if (fnv1a32(key) > this.threshold) return false;
    record.sample_rate = this.rate;
    return true;
  }
}

// ---------------------------------------------------------------------------
// Emission
// ---------------------------------------------------------------------------

/**
 * A bounded, non-blocking record queue drained off the dispatch path.
 *
 * Writing synchronously puts sink latency in the request path. Handing
 * records to a deferred drain removes that, but only if the queue is bounded
 * and enqueueing never waits — an unbounded queue turns a stalled disk into
 * an OOM, and a blocking put reintroduces exactly the latency the queue was
 * meant to remove. Full therefore means drop.
 *
 * What makes dropping acceptable rather than silent corruption is that it is
 * reported: the next record through carries `dropped_records`, so the loss is
 * visible in the log itself. A log that loses records without saying so is
 * worse than a slow one, because a consumer cannot tell a quiet period from a
 * lossy one.
 *
 * The trade is durability — a crash loses whatever is still queued — which is
 * why it is opt-in and wrong for audit.
 */
class AsyncRecordQueue {
  private readonly queue: AccessRecord[] = [];
  private dropped = 0;
  private scheduled = false;

  constructor(
    private readonly capacity: number,
    private readonly writeRecord: (record: AccessRecord) => void,
  ) {}

  enqueue(record: AccessRecord): void {
    if (this.queue.length >= this.capacity) {
      this.dropped++;
      return;
    }
    if (this.dropped) {
      // Attribute the loss to the first record that gets through after it,
      // so the count reaches the same file the lost records would have.
      record.dropped_records = this.dropped;
      this.dropped = 0;
    }
    this.queue.push(record);
    if (!this.scheduled) {
      this.scheduled = true;
      // A macrotask, not a microtask: the point is to leave the turn that is
      // serving the request, not merely the current call stack.
      setTimeout(() => {
        this.scheduled = false;
        this.flush();
      }, 0);
    }
  }

  /** Drain every queued record now. Callers use this on shutdown (and tests
   *  use it to observe writes deterministically). */
  flush(): void {
    while (this.queue.length > 0) {
      const record = this.queue.shift()!;
      this.writeRecord(record);
    }
  }

  /** Records lost since the last successful enqueue. */
  get droppedCount(): number {
    return this.dropped;
  }
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

export class AccessLogHook implements DispatchHook {
  private readonly serverVersion: string;
  private readonly level: "INFO" | "DEBUG";
  private readonly maxRecordBytes: number;
  private readonly sampler: AccessLogSampler;
  private readonly traceContext: TraceContextResolver;
  private readonly redactor: ClaimRedactor;
  private readonly queue: AsyncRecordQueue | null;

  constructor(
    private readonly sink: AccessLogSink,
    options: AccessLogOptions | string = {},
  ) {
    // Backward compatibility: the original signature accepted a bare
    // serverVersion string as the second arg.
    const opts: AccessLogOptions = typeof options === "string" ? { serverVersion: options } : options;
    this.serverVersion = opts.serverVersion ?? "";
    this.level = opts.level ?? "INFO";
    this.maxRecordBytes = opts.maxRecordBytes ?? 1_048_576;
    this.sampler = new AccessLogSampler(opts.sampleRate ?? 1);
    this.traceContext = opts.traceContext ?? otelTraceContext;
    this.redactor = opts.redactor ?? redactClaims;
    this.queue = opts.async ? new AsyncRecordQueue(opts.queueSize ?? 10_000, (rec) => this.write(rec)) : null;
  }

  /** Drain any queued records immediately. No-op for a synchronous hook;
   *  call it on shutdown when `async` is enabled. */
  flush(): void {
    this.queue?.flush();
  }

  /** Records lost to a full queue and not yet reported. Diagnostics only —
   *  the count is reported in-band on the next record through. */
  get droppedRecords(): number {
    return this.queue?.droppedCount ?? 0;
  }

  /** Capture a high-resolution start timestamp; returned token feeds {@link onDispatchEnd}. */
  onDispatchStart(_info: DispatchInfo): HookToken {
    const token: StartToken = { startNs: process.hrtime.bigint() };
    return token;
  }

  /** Emit one access-log JSON record for the completed dispatch (best-effort;
   *  write errors are swallowed so logging never breaks a request). */
  onDispatchEnd(token: HookToken, info: DispatchInfo, stats: CallStatistics, error?: Error): void {
    const t = token as StartToken | undefined;
    const durationMs = t ? roundTo2(Number(process.hrtime.bigint() - t.startNs) / 1_000_000) : 0;

    const status = error ? "error" : "ok";
    const errType = error ? ((error as Error & { type?: string }).type ?? error.constructor.name) : "";
    const errMsg = error?.message ?? "";

    const protocol = info.protocol ?? "";
    const rec: AccessRecord = {
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
    if (info.httpStatus !== undefined) rec.http_status = info.httpStatus;
    // Trace correlation. `request_id` only joins records within this service;
    // these join them to the surrounding distributed trace. Both or neither.
    const trace = this.currentTrace();
    if (trace) {
      rec.trace_id = trace.traceId;
      rec.span_id = trace.spanId;
    }
    if (info.requestData && info.requestData.length > 0) {
      // At INFO, the per-request base64 payload dominates record size (an
      // init RPC commonly logs 8+ KiB of base64 per call) and audit consumers
      // rarely need the bytes — they care about who/what/when. The marker is
      // "payload_omitted", NOT `true`: nothing was lost to a size cap here,
      // and a consumer scanning for real data loss needs the two to be
      // distinguishable. Bump level to DEBUG to re-enable the full payload.
      const encoded = base64(info.requestData);
      if (this.level === "DEBUG") {
        rec.request_data = encoded;
      } else {
        rec.original_request_bytes = encoded.length;
        rec.truncated = "payload_omitted";
      }
    }
    if (info.methodType === "stream") {
      // The schema requires the field on every stream record, including ones
      // for requests that failed before a stream existed — a mistyped method,
      // a cursor that would not open. All-zeros is the sentinel for that case
      // and nothing else: a transport that established a stream reports its
      // chain id, the same value on `/init` and every continuation.
      rec.stream_id = info.streamId ?? "00000000000000000000000000000000";
    }
    if (info.cancelled) rec.cancelled = true;

    if (info.claims && Object.keys(info.claims).length > 0) {
      const claims = this.redact(info.claims);
      if (Object.keys(claims).length > 0) rec.claims = claims;
    }

    // Egress accounting. `input_bytes` / `output_bytes` below measure logical
    // Arrow buffers — what the worker processed. These measure what actually
    // crossed the network, a different number in both directions: compression
    // shrinks the body, and externalised payloads leave it entirely.
    if (info.requestBytes !== undefined) rec.request_bytes = info.requestBytes;
    if (info.externalizedBytes) rec.externalized_bytes = info.externalizedBytes;

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

    // `response_bytes` cannot be measured here: response compression runs
    // after dispatch returns, so a record emitted now could only ever report
    // the uncompressed body — the wrong number for anything that costs money.
    // When the transport offers a deferral point, hand the record over and let
    // it emit once the final body exists.
    const sampleKey = info.streamId || info.requestId || `${rec.timestamp}:${info.method}`;
    if (info.deferral) {
      info.deferral.defer((responseBytes) => {
        if (responseBytes !== undefined) rec.response_bytes = responseBytes;
        this.emit(rec, sampleKey);
      });
      return;
    }
    this.emit(rec, sampleKey);
  }

  /** Apply the configured redactor, failing closed if it throws. */
  private redact(claims: Record<string, unknown>): Record<string, unknown> {
    try {
      return this.redactor(claims);
    } catch (err) {
      // Fail closed. A broken redactor must not take the request down with
      // it, but it must not fail *open* either — drop the claims entirely
      // rather than write unredacted ones to a log that outlives the token.
      console.warn("vgi-rpc access log: claim redactor threw; dropping claims from the record", err);
      return {};
    }
  }

  private currentTrace(): TraceContext | null {
    try {
      return this.traceContext();
    } catch {
      return null;
    }
  }

  private emit(rec: AccessRecord, sampleKey: string): void {
    if (!this.sampler.keep(rec, sampleKey)) return;
    if (this.queue) {
      this.queue.enqueue(rec);
      return;
    }
    this.write(rec);
  }

  private write(rec: AccessRecord): void {
    try {
      this.sink.write(`${this.format(rec)}\n`);
    } catch {
      // best-effort
    }
  }

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
  private format(rec: AccessRecord): string {
    let line = JSON.stringify(rec);
    if (this.maxRecordBytes <= 0 || utf8Length(line) <= this.maxRecordBytes) return line;

    const requestData = rec.request_data;
    if (typeof requestData === "string") {
      rec.original_request_bytes = requestData.length;
      delete rec.request_data;
      // Genuine size-driven shedding — `true`, as distinct from the
      // "payload_omitted" this record may have carried a moment ago.
      rec.truncated = true;
      line = JSON.stringify(rec);
      if (utf8Length(line) <= this.maxRecordBytes) return line;
    }

    if (rec.claims !== undefined) {
      rec.claims = {};
      rec.truncated = true;
      line = JSON.stringify(rec);
      if (utf8Length(line) <= this.maxRecordBytes) return line;
    }

    const sentinel: AccessRecord = {
      timestamp: rec.timestamp,
      level: "INFO",
      logger: "vgi_rpc.access",
      message: "record_too_large",
      server_id: rec.server_id ?? "",
      protocol: rec.protocol ?? "",
      protocol_hash: rec.protocol_hash ?? "",
      method: rec.method ?? "",
      method_type: rec.method_type ?? "unary",
      principal: rec.principal ?? "",
      auth_domain: rec.auth_domain ?? "",
      authenticated: rec.authenticated ?? false,
      remote_addr: rec.remote_addr ?? "",
      duration_ms: rec.duration_ms ?? 0,
      status: rec.status ?? "ok",
      error_type: rec.error_type ?? "",
      truncated: "record_too_large",
    };
    if (rec.method_type === "stream" && typeof rec.stream_id === "string") sentinel.stream_id = rec.stream_id;
    if (sentinel.status === "error") {
      sentinel.error_message =
        typeof rec.error_message === "string" && rec.error_message ? rec.error_message : "record_too_large";
    }
    if (typeof rec.dropped_records === "number") sentinel.dropped_records = rec.dropped_records;
    if (typeof rec.sample_rate === "number") sentinel.sample_rate = rec.sample_rate;
    return JSON.stringify(sentinel);
  }
}
