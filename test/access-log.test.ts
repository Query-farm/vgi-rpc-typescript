// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Access-log behaviour the JSON schema cannot check.
 *
 * `vgi-rpc-test --access-log` validates the *shape* of every record; what it
 * cannot see is whether sampling is deterministic, whether a dropped record is
 * reported, whether a broken redactor fails closed, or whether
 * `response_bytes` is the compressed size rather than the one the handler
 * produced. Those are the properties that make the fields trustworthy, so they
 * are pinned here.
 */

import { describe, expect, test } from "bun:test";
import { RecordBatch, RecordBatchStreamWriter, recordBatchFromArrays } from "@query-farm/apache-arrow";
import { AccessLogHook, AccessLogSampler, type AccessLogSink, noRedaction, redactClaims } from "../src/access-log.js";
import { REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY } from "../src/constants.js";
import type { ExternalStorage } from "../src/external.js";
import { ARROW_CONTENT_TYPE } from "../src/http/common.js";
import { createHttpHandler } from "../src/http/index.js";
import { Protocol, str } from "../src/index.js";
import type { CallStatistics, DispatchInfo } from "../src/types.js";
import { gzipCompress } from "../src/util/gzip.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

class CaptureSink implements AccessLogSink {
  readonly lines: string[] = [];
  write(line: string): void {
    this.lines.push(line);
  }
  records(): Record<string, any>[] {
    return this.lines.map((l) => JSON.parse(l));
  }
  last(): Record<string, any> {
    return JSON.parse(this.lines[this.lines.length - 1]!);
  }
}

const NO_STATS: CallStatistics = {
  inputBatches: 0,
  outputBatches: 0,
  inputRows: 0,
  outputRows: 0,
  inputBytes: 0,
  outputBytes: 0,
};

function info(overrides: Partial<DispatchInfo> = {}): DispatchInfo {
  return {
    method: "echo",
    methodType: "unary",
    serverId: "abc123abc123",
    requestId: "req-1",
    protocol: "TestService",
    protocolHash: "a".repeat(64),
    ...overrides,
  };
}

/** Run one dispatch through `hook` and return nothing — the sink holds the record. */
function dispatch(hook: AccessLogHook, i: DispatchInfo, error?: Error): void {
  const token = hook.onDispatchStart(i);
  hook.onDispatchEnd(token, i, NO_STATS, error);
}

// ---------------------------------------------------------------------------
// Sampling
// ---------------------------------------------------------------------------

describe("access log sampling", () => {
  test("rejects an out-of-range rate at construction, not at first request", () => {
    // 100 meaning "100%" must be a startup failure; silently logging
    // everything is the outcome this guards against.
    expect(() => new AccessLogSampler(100)).toThrow(RangeError);
    expect(() => new AccessLogSampler(-0.1)).toThrow(RangeError);
    expect(() => new AccessLogHook(new CaptureSink(), { sampleRate: 2 })).toThrow(RangeError);
    expect(() => new AccessLogSampler(0)).not.toThrow();
    expect(() => new AccessLogSampler(1)).not.toThrow();
  });

  test("the decision is per call: one stream_id, one fate", () => {
    const sampler = new AccessLogSampler(0.5);
    const streamId = "f".repeat(32);
    const decisions = new Set<boolean>();
    for (let i = 0; i < 20; i++) {
      decisions.add(sampler.keep({ status: "ok" }, streamId));
    }
    expect(decisions.size).toBe(1);
  });

  test("every record of a stream shares its init's fate", () => {
    const sink = new CaptureSink();
    // A rate low enough that hitting it by accident on both records is not
    // what the assertion is measuring — the point is init and continuation
    // agree, whichever way the hash falls.
    const hook = new AccessLogHook(sink, { sampleRate: 0.5 });
    const streamId = "ab".repeat(16);
    const init = info({ methodType: "stream", streamId, requestData: new Uint8Array([1, 2, 3]) });
    const continuation = info({ methodType: "stream", streamId });
    dispatch(hook, init);
    dispatch(hook, continuation);
    expect(sink.lines.length === 0 || sink.lines.length === 2).toBe(true);
  });

  test("errors are never sampled out and kept records carry sample_rate", () => {
    const sink = new CaptureSink();
    // Rate 0 keeps nothing that the sampler is allowed to drop.
    const hook = new AccessLogHook(sink, { sampleRate: 0 });
    for (let i = 0; i < 50; i++) {
      dispatch(hook, info({ requestId: `ok-${i}` }));
    }
    expect(sink.lines.length).toBe(0);

    dispatch(hook, info({ requestId: "boom" }), new Error("kaboom"));
    expect(sink.lines.length).toBe(1);
    expect(sink.last().status).toBe("error");

    // A sampled-in record must state the rate: a consumer scaling counts has
    // to divide by it.
    const keepSink = new CaptureSink();
    const keepHook = new AccessLogHook(keepSink, { sampleRate: 0.99 });
    let seen = 0;
    for (let i = 0; i < 200 && seen === 0; i++) {
      dispatch(keepHook, info({ requestId: `id-${i}` }));
      seen = keepSink.lines.length;
    }
    expect(seen).toBeGreaterThan(0);
    expect(keepSink.records()[0]!.sample_rate).toBe(0.99);
  });

  test("rate 1 logs everything and omits sample_rate", () => {
    const sink = new CaptureSink();
    const hook = new AccessLogHook(sink, { sampleRate: 1 });
    dispatch(hook, info());
    expect(sink.lines.length).toBe(1);
    expect(sink.last().sample_rate).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Asynchronous emission
// ---------------------------------------------------------------------------

describe("access log async emission", () => {
  test("a full queue drops rather than blocks, and says how many", () => {
    const sink = new CaptureSink();
    const hook = new AccessLogHook(sink, { async: true, queueSize: 2 });

    // Nothing is written inline — that is the entire point of the queue.
    for (let i = 0; i < 5; i++) dispatch(hook, info({ requestId: `q-${i}` }));
    expect(sink.lines.length).toBe(0);
    expect(hook.droppedRecords).toBe(3);

    hook.flush();
    expect(sink.lines.length).toBe(2);
    expect(sink.records().every((r) => r.dropped_records === undefined)).toBe(true);

    // The next record through carries the loss, so a consumer can tell a
    // quiet period from a lossy one.
    dispatch(hook, info({ requestId: "after-drop" }));
    hook.flush();
    expect(sink.lines.length).toBe(3);
    expect(sink.last().dropped_records).toBe(3);
    expect(hook.droppedRecords).toBe(0);

    // The count resets — it is "since the last successful enqueue", not a
    // running total.
    dispatch(hook, info({ requestId: "clean" }));
    hook.flush();
    expect(sink.last().dropped_records).toBeUndefined();
  });

  test("queued records drain on their own without an explicit flush", async () => {
    const sink = new CaptureSink();
    const hook = new AccessLogHook(sink, { async: true, queueSize: 10 });
    dispatch(hook, info());
    expect(sink.lines.length).toBe(0);
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(sink.lines.length).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Claim redaction
// ---------------------------------------------------------------------------

describe("access log claim redaction", () => {
  test("replaces values by key and keeps the keys", () => {
    const out = redactClaims({
      sub: "user-1",
      email: "a@b.example",
      api_key: "sk-live-1",
      access_token: "tok",
      password: "hunter2",
      phone_number: "+1",
      role: "admin",
    });
    // Which claims a credential carried is exactly what an audit log is for.
    expect(Object.keys(out).sort()).toEqual(
      ["access_token", "api_key", "email", "password", "phone_number", "role", "sub"].sort(),
    );
    expect(out.email).toBe("[redacted]");
    expect(out.api_key).toBe("[redacted]");
    expect(out.access_token).toBe("[redacted]");
    expect(out.password).toBe("[redacted]");
    expect(out.phone_number).toBe("[redacted]");
    // Key-based only: `sub` and `role` are not credential-shaped names.
    expect(out.sub).toBe("user-1");
    expect(out.role).toBe("admin");
  });

  test("the default policy applies to emitted records", () => {
    const sink = new CaptureSink();
    const hook = new AccessLogHook(sink);
    dispatch(hook, info({ authenticated: true, claims: { sub: "u1", email: "a@b.example" } }));
    expect(sink.last().claims).toEqual({ sub: "u1", email: "[redacted]" });
  });

  test("noRedaction is available for a service that owns its logs", () => {
    const sink = new CaptureSink();
    const hook = new AccessLogHook(sink, { redactor: noRedaction });
    dispatch(hook, info({ authenticated: true, claims: { email: "a@b.example" } }));
    expect(sink.last().claims).toEqual({ email: "a@b.example" });
  });

  test("a redactor that throws fails closed", () => {
    const sink = new CaptureSink();
    const hook = new AccessLogHook(sink, {
      redactor: () => {
        throw new Error("policy lookup failed");
      },
    });
    dispatch(hook, info({ authenticated: true, claims: { email: "a@b.example", sub: "u1" } }));
    const rec = sink.last();
    // The record still exists — an observability failure must not fail the
    // call — but the claims are gone rather than unredacted.
    expect(rec.status).toBe("ok");
    expect(rec.claims).toBeUndefined();
    expect(JSON.stringify(rec)).not.toContain("a@b.example");
  });
});

// ---------------------------------------------------------------------------
// truncated: payload_omitted vs true
// ---------------------------------------------------------------------------

describe("access log truncation markers", () => {
  test("payload omission at INFO is 'payload_omitted', not 'true'", () => {
    const sink = new CaptureSink();
    const hook = new AccessLogHook(sink);
    dispatch(hook, info({ requestData: new Uint8Array(64) }));
    const rec = sink.last();
    expect(rec.truncated).toBe("payload_omitted");
    expect(rec.request_data).toBeUndefined();
    expect(rec.original_request_bytes).toBeGreaterThan(0);
  });

  test("DEBUG keeps the payload and sets no marker", () => {
    const sink = new CaptureSink();
    const hook = new AccessLogHook(sink, { level: "DEBUG" });
    dispatch(hook, info({ requestData: new Uint8Array([1, 2, 3, 4]) }));
    const rec = sink.last();
    expect(rec.truncated).toBeUndefined();
    expect(typeof rec.request_data).toBe("string");
  });

  test("size-driven shedding is 'true' — the value consumers filter on", () => {
    const sink = new CaptureSink();
    // DEBUG so the payload is in the record to begin with, and a cap it
    // cannot fit under.
    const hook = new AccessLogHook(sink, { level: "DEBUG", maxRecordBytes: 512 });
    dispatch(hook, info({ requestData: new Uint8Array(4096) }));
    const rec = sink.last();
    expect(rec.truncated).toBe(true);
    expect(rec.request_data).toBeUndefined();
    expect(rec.original_request_bytes).toBeGreaterThan(512);
  });

  test("claims are emptied before the sentinel form is reached", () => {
    const sink = new CaptureSink();
    const hook = new AccessLogHook(sink, { maxRecordBytes: 800 });
    const claims: Record<string, unknown> = {};
    for (let i = 0; i < 100; i++) claims[`claim_${i}`] = "x".repeat(20);
    dispatch(hook, info({ authenticated: true, claims }));
    const rec = sink.last();
    expect(rec.truncated).toBe(true);
    expect(rec.claims).toEqual({});
  });

  test("the sentinel form keeps the envelope and the full error message", () => {
    const sink = new CaptureSink();
    const hook = new AccessLogHook(sink, { maxRecordBytes: 200 });
    const message = "boom ".repeat(200);
    dispatch(hook, info(), new Error(message));
    const rec = sink.last();
    expect(rec.truncated).toBe("record_too_large");
    // error_message is never truncated — operators debug from it.
    expect(rec.error_message).toBe(message);
    expect(rec.server_id).toBe("abc123abc123");
    expect(rec.status).toBe("error");
  });
});

// ---------------------------------------------------------------------------
// Trace correlation
// ---------------------------------------------------------------------------

describe("access log trace correlation", () => {
  test("emits trace_id and span_id together as W3C hex", () => {
    const sink = new CaptureSink();
    const hook = new AccessLogHook(sink, {
      traceContext: () => ({ traceId: "0".repeat(31) + "1", spanId: "0".repeat(15) + "2" }),
    });
    dispatch(hook, info());
    const rec = sink.last();
    expect(rec.trace_id).toMatch(/^[0-9a-f]{32}$/);
    expect(rec.span_id).toMatch(/^[0-9a-f]{16}$/);
  });

  test("emits neither when no span is current", () => {
    const sink = new CaptureSink();
    const hook = new AccessLogHook(sink, { traceContext: () => null });
    dispatch(hook, info());
    expect(sink.last().trace_id).toBeUndefined();
    expect(sink.last().span_id).toBeUndefined();
  });

  test("a resolver that throws does not fail the record", () => {
    const sink = new CaptureSink();
    const hook = new AccessLogHook(sink, {
      traceContext: () => {
        throw new Error("otel exploded");
      },
    });
    dispatch(hook, info());
    expect(sink.lines.length).toBe(1);
    expect(sink.last().trace_id).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Egress accounting (HTTP transport)
// ---------------------------------------------------------------------------

function buildRequestIpc(schema: any, values: Record<string, any[]>, methodName: string): Uint8Array {
  const batch = recordBatchFromArrays(values, schema);
  const meta = new Map<string, string>([
    [RPC_METHOD_KEY, methodName],
    [REQUEST_VERSION_KEY, REQUEST_VERSION],
  ]);
  const withMeta = new RecordBatch(schema, batch.data, meta);
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, schema);
  writer.write(withMeta);
  writer.close();
  return writer.toUint8Array(true);
}

/** The method's own params schema — inferring one from an array of strings
 *  produces a type the request parser does not read back as a string. */
function paramsSchemaOf(protocol: Protocol, method: string): any {
  return protocol.getMethods().get(method)!.paramsSchema;
}

function egressProtocol(): Protocol {
  const protocol = new Protocol("EgressService");
  protocol.unary("bulk", {
    params: { value: str },
    result: { result: str },
    // Highly compressible: the whole point is that the logical Arrow size and
    // the on-wire size are different numbers.
    handler: ({ value }) => ({ result: value.repeat(4000) }),
  });
  return protocol;
}

describe("access log egress accounting", () => {
  test("response_bytes is the compressed body, not what the handler produced", async () => {
    const sink = new CaptureSink();
    const protocol = egressProtocol();
    const handler = createHttpHandler(protocol, {
      dispatchHook: new AccessLogHook(sink),
      serverId: "egress000001",
    });
    const params = paramsSchemaOf(protocol, "bulk");
    const body = buildRequestIpc(params, { value: ["hello"] }, "bulk");
    const response = await handler(
      new Request("http://localhost/bulk", {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE, "Accept-Encoding": "gzip" },
        body,
      }),
    );
    const wire = new Uint8Array(await response.arrayBuffer());
    expect(response.headers.get("Content-Encoding")).toBe("gzip");

    const rec = sink.last();
    expect(rec.status).toBe("ok");
    // The number reported is the number that crossed the network — the
    // measurement cannot be taken at handler time, when compression has not
    // run yet.
    expect(rec.response_bytes).toBe(wire.byteLength);
    // ...and it is far below the ~20 KB the handler actually produced.
    expect(rec.response_bytes).toBeLessThan(4000);
  });

  test("request_bytes is what the peer sent, before decompression", async () => {
    const sink = new CaptureSink();
    const protocol = egressProtocol();
    const handler = createHttpHandler(protocol, {
      dispatchHook: new AccessLogHook(sink),
      serverId: "egress000002",
    });
    const params = paramsSchemaOf(protocol, "bulk");
    const plain = buildRequestIpc(params, { value: ["hello world"] }, "bulk");
    const compressed = await gzipCompress(plain);
    await handler(
      new Request("http://localhost/bulk", {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE, "Content-Encoding": "gzip" },
        body: compressed as unknown as BodyInit,
      }),
    );
    const rec = sink.last();
    expect(rec.request_bytes).toBe(compressed.byteLength);
    expect(rec.request_bytes).toBeLessThan(plain.byteLength);
  });

  test("externalized_bytes counts uploads that never touch the body", async () => {
    const sink = new CaptureSink();
    let uploaded = 0;
    const storage: ExternalStorage = {
      async upload(data: Uint8Array): Promise<string> {
        uploaded += data.byteLength;
        return "https://storage.example/object";
      },
    };
    const protocol = egressProtocol();
    const handler = createHttpHandler(protocol, {
      dispatchHook: new AccessLogHook(sink),
      serverId: "egress000003",
      externalLocation: { storage, externalizeThresholdBytes: 1 },
    });
    const params = paramsSchemaOf(protocol, "bulk");
    const body = buildRequestIpc(params, { value: ["hello"] }, "bulk");
    const response = await handler(
      new Request("http://localhost/bulk", {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );
    const wire = new Uint8Array(await response.arrayBuffer());
    const rec = sink.last();
    expect(uploaded).toBeGreaterThan(0);
    expect(rec.externalized_bytes).toBe(uploaded);
    // The payload left the HTTP body entirely: without this field the bytes
    // are invisible to any transport-level accounting.
    expect(rec.response_bytes).toBeLessThan(rec.externalized_bytes);
    expect(wire.byteLength).toBeLessThan(uploaded);
  });
});
