// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { beforeAll, describe, expect, test } from "bun:test";
import {
  Field,
  Float64,
  Int32,
  RecordBatch,
  RecordBatchReader,
  RecordBatchStreamWriter,
  recordBatchFromArrays,
  Schema,
  Utf8,
} from "@query-farm/apache-arrow";
import {
  LOG_LEVEL_KEY,
  LOG_MESSAGE_KEY,
  REQUEST_VERSION,
  REQUEST_VERSION_KEY,
  RPC_METHOD_KEY,
  STATE_KEY,
} from "../../src/constants.js";
import { ARROW_CONTENT_TYPE } from "../../src/http/common.js";
import { createHttpHandler, float, int32, Protocol, str, TransportKind } from "../../src/index.js";
import { gzipDecompress } from "../../src/util/gzip.js";
import { zstdDecompress } from "../../src/util/zstd.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function buildRequestIpc(
  schema: Schema,
  values: Record<string, any[]>,
  methodName: string,
  metadata?: Map<string, string>,
): Uint8Array {
  const batch = recordBatchFromArrays(values, schema);
  const meta = metadata ?? new Map<string, string>();
  meta.set(RPC_METHOD_KEY, methodName);
  meta.set(REQUEST_VERSION_KEY, REQUEST_VERSION);
  const batchWithMeta = new RecordBatch(schema, batch.data, meta);

  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, schema);
  writer.write(batchWithMeta);
  writer.close();
  return writer.toUint8Array(true);
}

async function readResponseBatches(response: Response): Promise<{ schema: Schema; batches: RecordBatch[] }> {
  const body = new Uint8Array(await response.arrayBuffer());
  const reader = await RecordBatchReader.from(body);
  const schema = reader.schema!;
  const batches = reader.readAll();
  return { schema, batches };
}

// ---------------------------------------------------------------------------
// Test Protocol
// ---------------------------------------------------------------------------

function makeTestProtocol(): Protocol {
  const protocol = new Protocol("TestHTTP");

  protocol.unary("add", {
    params: { a: float, b: float },
    result: { result: float },
    handler: async ({ a, b }) => ({ result: a + b }),
    doc: "Add two numbers.",
  });

  protocol.unary("greet", {
    params: { name: str },
    result: { greeting: str },
    handler: async ({ name }) => ({ greeting: `Hello, ${name}!` }),
  });

  protocol.unary("fail", {
    params: {},
    result: {},
    handler: () => {
      throw new Error("intentional failure");
    },
  });

  protocol.unary("echo_with_log", {
    params: { value: str },
    result: { result: str },
    handler: (p, ctx) => {
      ctx.clientLog("INFO", `echo: ${p.value}`);
      return { result: p.value };
    },
  });

  protocol.producer<{ count: number; current: number }>("count", {
    params: { count: int32 },
    outputSchema: { n: int32 },
    init: ({ count }) => ({ count, current: 0 }),
    produce: (state, out) => {
      if (state.current >= state.count) {
        out.finish();
        return;
      }
      out.emitRow({ n: state.current });
      state.current++;
    },
  });

  protocol.exchange<{ factor: number }>("scale", {
    params: { factor: float },
    inputSchema: { value: float },
    outputSchema: { value: float },
    init: ({ factor }) => ({ factor }),
    exchange: (state, input, out) => {
      const value = input.getChildAt(0)?.get(0) as number;
      out.emitRow({ value: value * state.factor });
    },
  });

  // Emits a 0-row data batch with per-emit metadata every round — the shape
  // of a conditional-revalidation "not modified" reply.
  protocol.exchange<Record<string, never>>("empty_reply", {
    params: {},
    inputSchema: { value: float },
    outputSchema: { value: float },
    init: () => ({}),
    exchange: (_state, _input, out) => {
      const emptyBatch = recordBatchFromArrays({ value: [] }, new Schema([Field.new("value", new Float64())]));
      out.emit(emptyBatch as any, new Map([["vgi.cache.status", "not_modified"]]));
    },
  });

  return protocol;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("HTTP Handler", () => {
  let handler: (req: Request) => Response | Promise<Response>;
  const BASE = "http://localhost:9999";

  beforeAll(() => {
    const protocol = makeTestProtocol();
    handler = createHttpHandler(protocol, {
      prefix: "/vgi",
      serverId: "test-server",
    });
  });

  // -- Basic routing --

  test("POST to unknown path returns 404", async () => {
    const body = buildRequestIpc(new Schema([]), {}, "nope");
    const res = await handler(
      new Request(`${BASE}/vgi/nope`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );
    expect(res.status).toBe(404);
  });

  test("GET returns 404 HTML page", async () => {
    const res = await handler(new Request(`${BASE}/vgi/add`, { method: "GET" }));
    expect(res.status).toBe(404);
    expect(res.headers.get("Content-Type")).toContain("text/html");
  });

  test("wrong Content-Type returns 415", async () => {
    const res = await handler(
      new Request(`${BASE}/vgi/add`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}",
      }),
    );
    expect(res.status).toBe(415);
  });

  // -- Unary dispatch --

  test("unary add", async () => {
    const paramSchema = new Schema([new Field("a", new Float64(), false), new Field("b", new Float64(), false)]);
    const body = buildRequestIpc(paramSchema, { a: [3], b: [4] }, "add");

    const res = await handler(
      new Request(`${BASE}/vgi/add`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );

    expect(res.status).toBe(200);
    expect(res.headers.get("Content-Type")).toBe(ARROW_CONTENT_TYPE);

    const { batches } = await readResponseBatches(res);
    expect(batches.length).toBe(1);
    const result = batches[0].getChildAt(0)?.get(0);
    expect(result).toBe(7);
  });

  test("unary greet", async () => {
    const paramSchema = new Schema([new Field("name", new Utf8(), false)]);
    const body = buildRequestIpc(paramSchema, { name: ["World"] }, "greet");

    const res = await handler(
      new Request(`${BASE}/vgi/greet`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );

    expect(res.status).toBe(200);
    const { batches } = await readResponseBatches(res);
    const greeting = batches[0].getChildAt(0)?.get(0);
    expect(greeting).toBe("Hello, World!");
  });

  test("unary error returns 200 with X-VGI-RPC-Error header", async () => {
    const body = buildRequestIpc(new Schema([]), {}, "fail");

    const res = await handler(
      new Request(`${BASE}/vgi/fail`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );

    expect(res.status).toBe(200);
    expect(res.headers.get("X-VGI-RPC-Error")).toBe("true");
    const { batches } = await readResponseBatches(res);
    expect(batches.length).toBe(1);
    const meta = batches[0].metadata;
    expect(meta?.get(LOG_LEVEL_KEY)).toBe("EXCEPTION");
    expect(meta?.get(LOG_MESSAGE_KEY)).toContain("intentional failure");
  });

  test("unary with client log", async () => {
    const paramSchema = new Schema([new Field("value", new Utf8(), false)]);
    const body = buildRequestIpc(paramSchema, { value: ["test"] }, "echo_with_log");

    const res = await handler(
      new Request(`${BASE}/vgi/echo_with_log`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );

    expect(res.status).toBe(200);
    const { batches } = await readResponseBatches(res);
    // Should have log batch + result batch
    expect(batches.length).toBe(2);
    // First batch is the log
    expect(batches[0].metadata?.get(LOG_LEVEL_KEY)).toBe("INFO");
    expect(batches[0].metadata?.get(LOG_MESSAGE_KEY)).toBe("echo: test");
    // Second batch is the result
    expect(batches[1].getChildAt(0)?.get(0)).toBe("test");
  });

  // -- __describe__ --

  test("describe endpoint", async () => {
    const body = buildRequestIpc(new Schema([]), {}, "__describe__");

    const res = await handler(
      new Request(`${BASE}/vgi/__describe__`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );

    expect(res.status).toBe(200);
    const { batches } = await readResponseBatches(res);
    expect(batches.length).toBe(1);
    // Should list all methods
    const names: string[] = [];
    const nameCol = batches[0].getChildAt(0)!;
    for (let i = 0; i < batches[0].numRows; i++) {
      names.push(nameCol.get(i));
    }
    expect(names).toContain("add");
    expect(names).toContain("greet");
    expect(names).toContain("count");
    expect(names).toContain("scale");
  });

  // -- Capabilities --

  test("capabilities endpoint", async () => {
    const handlerWithCaps = createHttpHandler(makeTestProtocol(), {
      prefix: "/vgi",
      maxRequestBytes: 1048576,
      corsOrigins: "*",
    });

    const res = await handlerWithCaps(new Request(`${BASE}/vgi/__capabilities__`, { method: "OPTIONS" }));

    expect(res.status).toBe(204);
    expect(res.headers.get("VGI-Max-Request-Bytes")).toBe("1048576");
    expect(res.headers.get("Access-Control-Allow-Origin")).toBe("*");
  });

  // -- CORS --

  test("CORS headers on responses", async () => {
    const handlerWithCors = createHttpHandler(makeTestProtocol(), {
      prefix: "/vgi",
      corsOrigins: "*",
      serverId: "cors-test",
    });

    const paramSchema = new Schema([new Field("a", new Float64(), false), new Field("b", new Float64(), false)]);
    const body = buildRequestIpc(paramSchema, { a: [1], b: [2] }, "add");

    const res = await handlerWithCors(
      new Request(`${BASE}/vgi/add`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );

    expect(res.status).toBe(200);
    expect(res.headers.get("Access-Control-Allow-Origin")).toBe("*");
    expect(res.headers.get("Access-Control-Expose-Headers")).toBe(
      "WWW-Authenticate, X-Request-ID, X-VGI-Content-Encoding, X-VGI-RPC-Error, " +
        "VGI-Max-Response-Bytes, VGI-Max-Externalized-Response-Bytes, VGI-Externalization-Enabled, " +
        "VGI-Supported-Encodings, VGI-Auth-Reason",
    );
  });

  test("CORS preflight", async () => {
    const handlerWithCors = createHttpHandler(makeTestProtocol(), {
      prefix: "/vgi",
      corsOrigins: "*",
    });

    const res = await handlerWithCors(new Request(`${BASE}/vgi/add`, { method: "OPTIONS" }));

    expect(res.status).toBe(204);
    expect(res.headers.get("Access-Control-Allow-Origin")).toBe("*");
    expect(res.headers.get("Access-Control-Allow-Methods")).toBe("POST, OPTIONS");
    expect(res.headers.get("Access-Control-Allow-Headers")).toBe("Content-Type, Authorization");
    expect(res.headers.get("Access-Control-Expose-Headers")).toBe(
      "WWW-Authenticate, X-Request-ID, X-VGI-Content-Encoding, X-VGI-RPC-Error, " +
        "VGI-Max-Response-Bytes, VGI-Max-Externalized-Response-Bytes, VGI-Externalization-Enabled, " +
        "VGI-Supported-Encodings, VGI-Auth-Reason",
    );
  });

  test("CORS preflight includes Access-Control-Max-Age by default", async () => {
    const handlerWithCors = createHttpHandler(makeTestProtocol(), {
      prefix: "/vgi",
      corsOrigins: "*",
    });

    const res = await handlerWithCors(new Request(`${BASE}/vgi/add`, { method: "OPTIONS" }));
    expect(res.headers.get("Access-Control-Max-Age")).toBe("300");
  });

  test("CORS preflight custom corsMaxAge", async () => {
    const handlerWithCors = createHttpHandler(makeTestProtocol(), {
      prefix: "/vgi",
      corsOrigins: "*",
      corsMaxAge: 3600,
    });

    const res = await handlerWithCors(new Request(`${BASE}/vgi/add`, { method: "OPTIONS" }));
    expect(res.headers.get("Access-Control-Max-Age")).toBe("3600");
  });

  test("CORS preflight corsMaxAge=null omits header", async () => {
    const handlerWithCors = createHttpHandler(makeTestProtocol(), {
      prefix: "/vgi",
      corsOrigins: "*",
      corsMaxAge: null,
    });

    const res = await handlerWithCors(new Request(`${BASE}/vgi/add`, { method: "OPTIONS" }));
    expect(res.headers.get("Access-Control-Max-Age")).toBeNull();
  });

  test("Access-Control-Max-Age not set on non-OPTIONS", async () => {
    const handlerWithCors = createHttpHandler(makeTestProtocol(), {
      prefix: "/vgi",
      corsOrigins: "*",
      serverId: "max-age-test",
    });

    const paramSchema = new Schema([new Field("a", new Float64(), false), new Field("b", new Float64(), false)]);
    const body = buildRequestIpc(paramSchema, { a: [1], b: [2] }, "add");

    const res = await handlerWithCors(
      new Request(`${BASE}/vgi/add`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );

    expect(res.status).toBe(200);
    expect(res.headers.get("Access-Control-Max-Age")).toBeNull();
  });

  test("CORS is off unless configured", async () => {
    const plain = createHttpHandler(makeTestProtocol(), { prefix: "/vgi", serverId: "cors-off" });

    const preflight = await plain(
      new Request(`${BASE}/vgi/add`, {
        method: "OPTIONS",
        headers: { Origin: "https://evil.example", "Access-Control-Request-Method": "POST" },
      }),
    );
    // OPTIONS still answers — it doubles as capability discovery — but grants
    // no origin.
    expect(preflight.status).toBe(204);
    expect(preflight.headers.get("Access-Control-Allow-Origin")).toBeNull();
    expect(preflight.headers.get("Access-Control-Expose-Headers")).toBeNull();

    const paramSchema = new Schema([new Field("a", new Float64(), false), new Field("b", new Float64(), false)]);
    const actual = await plain(
      new Request(`${BASE}/vgi/add`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE, Origin: "https://evil.example" },
        body: buildRequestIpc(paramSchema, { a: [1], b: [2] }, "add"),
      }),
    );
    expect(actual.status).toBe(200);
    expect(actual.headers.get("Access-Control-Allow-Origin")).toBeNull();
  });

  test("CORS preflight echoes the requested headers", async () => {
    const handlerWithCors = createHttpHandler(makeTestProtocol(), {
      prefix: "/vgi",
      corsOrigins: "https://conformance.example",
    });

    // A browser only sends the headers the preflight permits; refusing
    // VGI-Session silently disables sticky, refusing VGI-Proxy-Proof takes out
    // every call behind a proof gate.
    const requested = "content-type, x-vgi-accept-encoding, vgi-session, vgi-session-accept, vgi-proxy-proof";
    const res = await handlerWithCors(
      new Request(`${BASE}/vgi/add`, {
        method: "OPTIONS",
        headers: {
          Origin: "https://conformance.example",
          "Access-Control-Request-Method": "POST",
          "Access-Control-Request-Headers": requested,
        },
      }),
    );

    expect(res.headers.get("Access-Control-Allow-Origin")).toBe("https://conformance.example");
    expect(res.headers.get("Access-Control-Allow-Methods")).toContain("POST");
    expect(res.headers.get("Access-Control-Allow-Headers")).toBe(requested);
  });

  test("CORS exposes the sticky session headers when sticky is on", async () => {
    const handlerWithCors = createHttpHandler(makeTestProtocol(), {
      prefix: "/vgi",
      corsOrigins: "*",
      enableSticky: true,
      stickyEchoHeaders: { "fly-force-instance-id": "abc" },
    });

    const res = await handlerWithCors(new Request(`${BASE}/vgi/add`, { method: "OPTIONS" }));
    const exposed = res.headers.get("Access-Control-Expose-Headers") ?? "";

    for (const name of [
      "VGI-Sticky-Enabled",
      "VGI-Sticky-Default-TTL",
      "VGI-Sticky-Echo-Headers",
      "VGI-Session",
      "VGI-Session-Close",
      "VGI-Echo-fly-force-instance-id",
    ]) {
      expect(exposed).toContain(name);
    }
  });

  test("CORS exposes the upload capability headers when uploads are on", async () => {
    const handlerWithCors = createHttpHandler(makeTestProtocol(), {
      prefix: "/vgi",
      corsOrigins: "*",
      maxRequestBytes: 4096,
      maxUploadBytes: 1024,
      uploadUrlProvider: {
        generateUploadUrl: () => ({ uploadUrl: "http://x/put", url: "http://x/get" }),
      },
    });

    const res = await handlerWithCors(new Request(`${BASE}/vgi/add`, { method: "OPTIONS" }));
    const exposed = res.headers.get("Access-Control-Expose-Headers") ?? "";

    expect(exposed).toContain("VGI-Max-Request-Bytes");
    expect(exposed).toContain("VGI-Upload-URL-Support");
    expect(exposed).toContain("VGI-Max-Upload-Bytes");
  });

  test("rejects a request whose Arrow parameter schema differs from the registered method", async () => {
    const wrongSchema = new Schema([new Field("name", new Float64(), false)]);
    const body = buildRequestIpc(wrongSchema, { name: [42] }, "greet");
    const res = await handler(
      new Request(`${BASE}/vgi/greet`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );
    expect(res.status).toBe(400);
    expect(await res.text()).toContain("Parameter schema mismatch");
  });

  test("rejects a stream init whose Arrow parameter schema differs from the registered method", async () => {
    const wrongSchema = new Schema([new Field("factor", new Utf8(), false)]);
    const body = buildRequestIpc(wrongSchema, { factor: ["not-a-float"] }, "scale");
    const res = await handler(
      new Request(`${BASE}/vgi/scale/init`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );
    expect(res.status).toBe(400);
    expect(await res.text()).toContain("Parameter schema mismatch");
  });

  test("rejects unary requests whose parameter batch does not have exactly one row", async () => {
    const paramSchema = new Schema([new Field("a", new Float64(), false), new Field("b", new Float64(), false)]);
    for (const values of [
      { a: [], b: [] },
      { a: [1, 2], b: [3, 4] },
    ]) {
      const res = await handler(
        new Request(`${BASE}/vgi/add`, {
          method: "POST",
          headers: { "Content-Type": ARROW_CONTENT_TYPE },
          body: buildRequestIpc(paramSchema, values, "add"),
        }),
      );
      expect(res.status).toBe(400);
      expect(await res.text()).toContain("Expected 1 row in request batch");
    }
  });

  test("rejects stream-init parameter batches that do not have exactly one row", async () => {
    const paramSchema = new Schema([new Field("count", new Int32(), false)]);
    const res = await handler(
      new Request(`${BASE}/vgi/count/init`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body: buildRequestIpc(paramSchema, { count: [] }, "count"),
      }),
    );
    expect(res.status).toBe(400);
    expect(await res.text()).toContain("Expected 1 row in request batch");
  });

  test("applies maxRequestBytes to the upload-URL endpoint", async () => {
    const limited = createHttpHandler(makeTestProtocol(), {
      prefix: "/vgi",
      maxRequestBytes: 64,
      uploadUrlProvider: {
        generateUploadUrl: () => ({
          uploadUrl: "https://storage.example/put",
          downloadUrl: "https://storage.example/get",
          expiresAt: new Date(Date.now() + 60_000),
        }),
      },
    });
    const res = await limited(
      new Request(`${BASE}/vgi/__upload_url__/init`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body: new Uint8Array(65),
      }),
    );
    expect(res.status).toBe(413);
  });

  test("bounds a chunked upload-URL request even without a general body cap", async () => {
    const limited = createHttpHandler(makeTestProtocol(), {
      prefix: "/vgi",
      uploadUrlProvider: {
        generateUploadUrl: () => ({
          uploadUrl: "https://storage.example/put",
          downloadUrl: "https://storage.example/get",
          expiresAt: new Date(Date.now() + 60_000),
        }),
      },
    });
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array(4096));
        controller.enqueue(new Uint8Array(4096));
        controller.enqueue(new Uint8Array(1));
        controller.close();
      },
    });
    const res = await limited(
      new Request(`${BASE}/vgi/__upload_url__/init`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body: stream,
      }),
    );
    expect(res.status).toBe(413);
  });

  test("every advertised capability header is exposed", async () => {
    // Mirrors the cross-language TestCors assertion: a capability header the
    // server advertises but does not expose is invisible to a browser and to
    // every other test here, which drives the handler ignoring CORS entirely.
    const handlerWithCors = createHttpHandler(makeTestProtocol(), {
      prefix: "/vgi",
      corsOrigins: "*",
      maxRequestBytes: 4096,
      maxResponseBytes: 8192,
      maxExternalizedResponseBytes: 8192,
      maxUploadBytes: 1024,
      uploadUrlProvider: {
        generateUploadUrl: () => ({ uploadUrl: "http://x/put", url: "http://x/get" }),
      },
      proxyProofRequired: true,
      proxyAuthHeaders: ["X-Forwarded-Access-Token"],
      enableSticky: true,
      stickyEchoHeaders: { "fly-force-instance-id": "abc" },
    });

    const res = await handlerWithCors(new Request(`${BASE}/vgi/health`, { method: "OPTIONS" }));
    const exposed = new Set(
      (res.headers.get("Access-Control-Expose-Headers") ?? "").split(",").map((h) => h.trim().toLowerCase()),
    );

    const advertised = [...res.headers.keys()].filter((n) => n.startsWith("vgi-") || n.startsWith("x-vgi-"));
    expect(advertised.length).toBeGreaterThan(0);
    for (const name of advertised) {
      expect(exposed).toContain(name);
    }
    // Rides error responses rather than being advertised, so the loop above
    // would not catch it.
    expect(exposed).toContain("x-vgi-rpc-error");
  });

  // -- Producer stream --

  test("producer stream init", async () => {
    const paramSchema = new Schema([new Field("count", new Int32(), false)]);
    const body = buildRequestIpc(paramSchema, { count: [3] }, "count");

    const res = await handler(
      new Request(`${BASE}/vgi/count/init`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );

    expect(res.status).toBe(200);
    const { batches } = await readResponseBatches(res);

    // ONE produce cycle per response, plus a continuation cursor — not the
    // whole stream in one body. This asserted all 3 batches arriving at /init,
    // which encoded a defect rather than the contract: with no byte cap
    // configured the turn loop never broke, so an entire scan was materialised
    // in one response. That collapsed parallel scans to a single reader (the
    // primary drained the shared work queue before peers connected), held the
    // whole result in RAM on both ends, and made the producer-cancellation
    // contract unreachable. vgi-rpc-python breaks after every produce cycle by
    // default; this now matches.
    // One data batch, then a 0-row batch carrying the continuation cursor.
    expect(batches.length).toBe(2);
    expect(batches[0].numRows).toBe(1);
    expect(batches[0].getChildAt(0)?.get(0)).toBe(0);
    expect(batches[1].numRows).toBe(0);
    const cursor = batches[1].metadata?.get(STATE_KEY);
    expect(cursor).toBeDefined();

    // The remaining values still arrive, one turn at a time — the stream is
    // incremental, not truncated.
    const seen = [0];
    let token = cursor;
    for (let turn = 0; turn < 5 && token; turn++) {
      const meta = new Map<string, string>();
      meta.set(STATE_KEY, token);
      // Continuation turns go to /exchange carrying the cursor — posting to
      // /init again would re-initialise the producer and replay from 0.
      const next = await handler(
        new Request(`${BASE}/vgi/count/exchange`, {
          method: "POST",
          headers: { "Content-Type": ARROW_CONTENT_TYPE },
          body: buildRequestIpc(paramSchema, { count: [3] }, "count", meta),
        }),
      );
      expect(next.status).toBe(200);
      const { batches: more } = await readResponseBatches(next);
      token = more[more.length - 1]?.metadata?.get(STATE_KEY);
      for (const b of more) {
        if (b.numRows > 0) seen.push(b.getChildAt(0)?.get(0) as number);
      }
      if (!token) break;
    }
    expect(seen).toEqual([0, 1, 2]);
  });

  // -- Exchange stream --

  test("exchange stream init + exchange", async () => {
    const paramSchema = new Schema([new Field("factor", new Float64(), false)]);
    const initBody = buildRequestIpc(paramSchema, { factor: [10] }, "scale");

    // Init
    const initRes = await handler(
      new Request(`${BASE}/vgi/scale/init`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body: initBody,
      }),
    );

    expect(initRes.status).toBe(200);
    const { batches: initBatches } = await readResponseBatches(initRes);
    // Exchange init returns a zero-row batch with state token
    expect(initBatches.length).toBe(1);
    expect(initBatches[0].numRows).toBe(0);
    const stateToken = initBatches[0].metadata?.get(STATE_KEY);
    expect(stateToken).toBeDefined();

    // Exchange round
    const inputSchema = new Schema([new Field("value", new Float64(), false)]);
    const exchangeMeta = new Map<string, string>();
    exchangeMeta.set(STATE_KEY, stateToken!);
    const exchangeBody = buildRequestIpc(inputSchema, { value: [5] }, "scale", exchangeMeta);

    const exchangeRes = await handler(
      new Request(`${BASE}/vgi/scale/exchange`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body: exchangeBody,
      }),
    );

    expect(exchangeRes.status).toBe(200);
    const { batches: exchangeBatches } = await readResponseBatches(exchangeRes);
    // Should have 1 data batch with token merged into metadata
    expect(exchangeBatches.length).toBe(1);
    // Data batch: 5 * 10 = 50
    expect(exchangeBatches[0].getChildAt(0)?.get(0)).toBe(50);
    expect(exchangeBatches[0].numRows).toBe(1);
    // Token is in the data batch's metadata
    expect(exchangeBatches[0].metadata?.get(STATE_KEY)).toBeDefined();
  });

  test("exchange 0-row data batch keeps emit metadata and token", async () => {
    const initBody = buildRequestIpc(new Schema([]), {}, "empty_reply");
    const initRes = await handler(
      new Request(`${BASE}/vgi/empty_reply/init`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body: initBody,
      }),
    );
    expect(initRes.status).toBe(200);
    const { batches: initBatches } = await readResponseBatches(initRes);
    const stateToken = initBatches[0].metadata?.get(STATE_KEY);
    expect(stateToken).toBeDefined();

    const inputSchema = new Schema([new Field("value", new Float64(), false)]);
    const exchangeMeta = new Map<string, string>();
    exchangeMeta.set(STATE_KEY, stateToken!);
    const exchangeBody = buildRequestIpc(inputSchema, { value: [5] }, "empty_reply", exchangeMeta);

    const exchangeRes = await handler(
      new Request(`${BASE}/vgi/empty_reply/exchange`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body: exchangeBody,
      }),
    );
    expect(exchangeRes.status).toBe(200);
    const { batches: exchangeBatches } = await readResponseBatches(exchangeRes);
    // Exactly one batch: the 0-row data batch itself carries the per-emit
    // metadata AND the continuation token (no bare batch + safety-net pair).
    expect(exchangeBatches.length).toBe(1);
    expect(exchangeBatches[0].numRows).toBe(0);
    expect(exchangeBatches[0].metadata?.get("vgi.cache.status")).toBe("not_modified");
    expect(exchangeBatches[0].metadata?.get(STATE_KEY)).toBeDefined();
  });

  test("exchange with multiple rounds", async () => {
    const paramSchema = new Schema([new Field("factor", new Float64(), false)]);
    const initBody = buildRequestIpc(paramSchema, { factor: [2] }, "scale");

    // Init
    const initRes = await handler(
      new Request(`${BASE}/vgi/scale/init`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body: initBody,
      }),
    );

    const { batches: initBatches } = await readResponseBatches(initRes);
    let token = initBatches[0].metadata?.get(STATE_KEY) ?? "";

    // Multiple exchange rounds
    for (const inputVal of [3, 7, 11]) {
      const inputSchema = new Schema([new Field("value", new Float64(), false)]);
      const meta = new Map<string, string>();
      meta.set(STATE_KEY, token);
      const body = buildRequestIpc(inputSchema, { value: [inputVal] }, "scale", meta);

      const res = await handler(
        new Request(`${BASE}/vgi/scale/exchange`, {
          method: "POST",
          headers: { "Content-Type": ARROW_CONTENT_TYPE },
          body,
        }),
      );

      expect(res.status).toBe(200);
      const { batches } = await readResponseBatches(res);
      expect(batches[0].getChildAt(0)?.get(0)).toBe(inputVal * 2);
      // Token is merged into the data batch's metadata
      token = batches[0].metadata?.get(STATE_KEY) ?? "";
      expect(token).toBeDefined();
    }
  });
});

// ---------------------------------------------------------------------------
// on_serve_start lifecycle
// ---------------------------------------------------------------------------

describe("HTTP onServeStart lifecycle", () => {
  const BASE = "http://localhost:9999";

  function request(body: Uint8Array): Request {
    return new Request(`${BASE}/vgi/ping`, {
      method: "POST",
      headers: { "Content-Type": ARROW_CONTENT_TYPE, "Accept-Encoding": "identity" },
      body,
    });
  }

  test("simultaneous first requests share one successful hook invocation", async () => {
    const protocol = new Protocol("lifecycle-concurrency");
    let dispatches = 0;
    protocol.unary("ping", {
      params: {},
      result: { result: str },
      handler: (_params, ctx) => {
        dispatches += 1;
        expect(ctx.kind).toBe(TransportKind.HTTP);
        return { result: "pong" };
      },
    });

    let release!: () => void;
    let markEntered!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const entered = new Promise<void>((resolve) => {
      markEntered = resolve;
    });
    let hookCalls = 0;
    const handler = createHttpHandler(protocol, {
      prefix: "/vgi",
      compressionLevel: null,
      onServeStart: async (kind) => {
        hookCalls += 1;
        expect(kind).toBe(TransportKind.HTTP);
        markEntered();
        await gate;
      },
    });
    const body = buildRequestIpc(new Schema([]), {}, "ping");

    const pending = Array.from({ length: 8 }, () => Promise.resolve(handler(request(body))));
    await entered;
    expect(hookCalls).toBe(1);
    expect(dispatches).toBe(0);

    release();
    const responses = await Promise.all(pending);
    expect(responses.every((response) => response.status === 200)).toBe(true);
    expect(hookCalls).toBe(1);
    expect(dispatches).toBe(8);
  });

  test("a failed hook retries once and never refires after success", async () => {
    const protocol = new Protocol("lifecycle-retry");
    protocol.unary("ping", {
      params: {},
      result: { result: str },
      handler: () => ({ result: "pong" }),
    });
    let hookCalls = 0;
    const handler = createHttpHandler(protocol, {
      prefix: "/vgi",
      compressionLevel: null,
      onServeStart: () => {
        hookCalls += 1;
        if (hookCalls === 1) throw new Error("transient startup failure");
      },
    });
    const body = buildRequestIpc(new Schema([]), {}, "ping");

    let firstErrorMessage = "";
    try {
      await handler(request(body));
    } catch (error) {
      firstErrorMessage = error instanceof Error ? error.message : String(error);
    }
    expect(firstErrorMessage).toContain("transient startup failure");
    expect((await handler(request(body))).status).toBe(200);
    expect((await handler(request(body))).status).toBe(200);
    expect(hookCalls).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// workerd has no `process` binding
// ---------------------------------------------------------------------------

describe("exchange path without a `process` global", () => {
  const BASE = "http://localhost:9999";

  /** Run `fn` with `globalThis.process` removed, as on workerd.
   *
   * The `await` inside the try is load-bearing: dispatch is asynchronous, so a
   * wrapper that restored `process` the moment `fn()` handed back its promise
   * would put the global back before the code under test ever read it — and
   * the test would pass against the exact bug it exists to catch. */
  async function withoutProcess<T>(fn: () => Promise<T>): Promise<T> {
    const original = Object.getOwnPropertyDescriptor(globalThis, "process");
    delete (globalThis as { process?: unknown }).process;
    try {
      return await fn();
    } finally {
      if (original) Object.defineProperty(globalThis, "process", original);
    }
  }

  // A bare `process.env` read on this path is a ReferenceError on workerd, not
  // an undefined. It took down every table-in-out and blended function on
  // Cloudflare Workers while plain table functions kept working, because only
  // these routes reach the exchange dispatcher.
  test("init + exchange succeed with no `process` binding", async () => {
    const handler = createHttpHandler(makeTestProtocol(), { prefix: "/vgi", serverId: "test-server" });

    const paramSchema = new Schema([new Field("factor", new Float64(), false)]);
    const initRes = await withoutProcess(() =>
      Promise.resolve(
        handler(
          new Request(`${BASE}/vgi/scale/init`, {
            method: "POST",
            headers: { "Content-Type": ARROW_CONTENT_TYPE },
            body: buildRequestIpc(paramSchema, { factor: [10] }, "scale"),
          }),
        ),
      ),
    );
    expect(initRes.status).toBe(200);

    const { batches: initBatches } = await readResponseBatches(initRes);
    const stateToken = initBatches[0].metadata?.get(STATE_KEY);
    expect(stateToken).toBeDefined();

    const inputSchema = new Schema([new Field("value", new Float64(), false)]);
    const meta = new Map<string, string>();
    meta.set(STATE_KEY, stateToken!);
    const exchangeRes = await withoutProcess(() =>
      Promise.resolve(
        handler(
          new Request(`${BASE}/vgi/scale/exchange`, {
            method: "POST",
            headers: { "Content-Type": ARROW_CONTENT_TYPE },
            body: buildRequestIpc(inputSchema, { value: [5] }, "scale", meta),
          }),
        ),
      ),
    );
    expect(exchangeRes.status).toBe(200);

    const { batches } = await readResponseBatches(exchangeRes);
    expect(batches[0].getChildAt(0)?.get(0)).toBe(50);
  });
});

// ---------------------------------------------------------------------------
// Response compression on workerd
// ---------------------------------------------------------------------------

describe("response compression on workerd", () => {
  const BASE = "http://localhost:9999";

  /** Build a handler with the runtime pretending to be Cloudflare Workers.
   *  `isWorkerd()` reads `navigator.userAgent`, and `createHttpHandler` reads
   *  it once at construction — so the stub has to wrap construction, not just
   *  the request. */
  function withNavigatorUserAgent<T>(userAgent: string, fn: () => T): T {
    const original = Object.getOwnPropertyDescriptor(globalThis, "navigator");
    Object.defineProperty(globalThis, "navigator", {
      value: { userAgent },
      configurable: true,
      writable: true,
    });
    try {
      return fn();
    } finally {
      if (original) {
        Object.defineProperty(globalThis, "navigator", original);
      } else {
        delete (globalThis as { navigator?: unknown }).navigator;
      }
    }
  }

  const paramSchema = new Schema([new Field("a", new Float64(), false), new Field("b", new Float64(), false)]);

  async function callAdd(handler: (req: Request) => Response | Promise<Response>): Promise<Response> {
    return handler(
      new Request(`${BASE}/vgi/add`, {
        method: "POST",
        headers: {
          "Content-Type": ARROW_CONTENT_TYPE,
          // The DuckDB extension's header pair: zstd is offered through both,
          // so `usedCustom` is false and a non-workerd server would stamp the
          // standard Content-Encoding.
          "Accept-Encoding": "deflate, gzip, br, zstd",
          "X-VGI-Accept-Encoding": "zstd, gzip",
        },
        body: buildRequestIpc(paramSchema, { a: [3], b: [4] }, "add"),
      }),
    );
  }

  // The codec is deliberately not pinned: these tests run under Bun, which
  // *can* encode zstd, so negotiation picks zstd here. Real workerd has no
  // zstd encoder and lands on gzip. What the workerd branch changes is which
  // header carries the label, and that is what these assert.
  async function decodeByHeader(res: Response): Promise<Uint8Array> {
    const codec = res.headers.get("X-VGI-Content-Encoding");
    // Checked before decoding: without it a regression that stamps the
    // standard header instead would decode with the wrong codec and surface
    // as an async stream error attributed to whichever test runs next.
    expect(codec).toMatch(/^(zstd|gzip)$/);
    const wire = new Uint8Array(await res.arrayBuffer());
    return codec === "zstd" ? zstdDecompress(wire) : gzipDecompress(wire);
  }

  // Cloudflare re-gzips a response that already carries a standard
  // Content-Encoding, yielding a double-encoded body under a single header.
  // Labelling with X-VGI-Content-Encoding keeps the edge's hands off it.
  test("labels the codec with X-VGI-Content-Encoding, not Content-Encoding", async () => {
    const handler = withNavigatorUserAgent("Cloudflare-Workers", () =>
      createHttpHandler(makeTestProtocol(), { prefix: "/vgi", serverId: "test-server" }),
    );

    const res = await callAdd(handler);
    expect(res.status).toBe(200);
    expect(res.headers.get("Content-Encoding")).toBeNull();
    expect(res.headers.get("X-VGI-Content-Encoding")).toMatch(/^(zstd|gzip)$/);
  });

  // The point of moving the header is to keep compressing — a regression to an
  // identity body would still decode correctly, so assert the body is encoded.
  test("still compresses, exactly once, and the body decodes to Arrow IPC", async () => {
    const handler = withNavigatorUserAgent("Cloudflare-Workers", () =>
      createHttpHandler(makeTestProtocol(), { prefix: "/vgi", serverId: "test-server" }),
    );

    const res = await callAdd(handler);
    const decoded = await decodeByHeader(res.clone());

    // Arrow IPC stream continuation marker. Reaching it after exactly one
    // decode is the whole point: the double-encoding bug left a second
    // compressed member here instead.
    expect(Array.from(decoded.slice(0, 4))).toEqual([0xff, 0xff, 0xff, 0xff]);

    // ...and the wire body really was compressed, not passed through.
    const wire = new Uint8Array(await res.arrayBuffer());
    expect(Array.from(wire.slice(0, 4))).not.toEqual([0xff, 0xff, 0xff, 0xff]);
    expect(wire.byteLength).toBeLessThan(decoded.byteLength);
  });

  test("off workerd the standard Content-Encoding is still used", async () => {
    const handler = withNavigatorUserAgent("Bun/1.0.0", () =>
      createHttpHandler(makeTestProtocol(), { prefix: "/vgi", serverId: "test-server" }),
    );

    const res = await callAdd(handler);
    expect(res.status).toBe(200);
    expect(res.headers.get("X-VGI-Content-Encoding")).toBeNull();
    expect(res.headers.get("Content-Encoding")).toBe("zstd");
  });
});
