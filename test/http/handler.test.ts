// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import {
  RecordBatchStreamWriter,
  RecordBatchReader,
  RecordBatch,
  Schema,
  Field,
  Float64,
  Int32,
  Utf8,
  recordBatchFromArrays,
} from "apache-arrow";
import { Protocol, float, int32, str, createHttpHandler } from "../../src/index.js";
import {
  RPC_METHOD_KEY,
  REQUEST_VERSION_KEY,
  REQUEST_VERSION,
  STATE_KEY,
  LOG_LEVEL_KEY,
  LOG_MESSAGE_KEY,
} from "../../src/constants.js";
import { ARROW_CONTENT_TYPE } from "../../src/http/common.js";

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

async function readResponseBatches(
  response: Response,
): Promise<{ schema: Schema; batches: RecordBatch[] }> {
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
    const body = buildRequestIpc(
      new Schema([]),
      {},
      "nope",
    );
    const res = await handler(
      new Request(`${BASE}/vgi/nope`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );
    expect(res.status).toBe(404);
  });

  test("GET returns 405", async () => {
    const res = await handler(
      new Request(`${BASE}/vgi/add`, { method: "GET" }),
    );
    expect(res.status).toBe(405);
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
    const paramSchema = new Schema([
      new Field("a", new Float64(), false),
      new Field("b", new Float64(), false),
    ]);
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

  test("unary error returns 500 with error batch", async () => {
    const body = buildRequestIpc(new Schema([]), {}, "fail");

    const res = await handler(
      new Request(`${BASE}/vgi/fail`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );

    expect(res.status).toBe(500);
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

    const res = await handlerWithCaps(
      new Request(`${BASE}/vgi/__capabilities__`, { method: "OPTIONS" }),
    );

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

    const paramSchema = new Schema([
      new Field("a", new Float64(), false),
      new Field("b", new Float64(), false),
    ]);
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
  });

  test("CORS preflight", async () => {
    const handlerWithCors = createHttpHandler(makeTestProtocol(), {
      prefix: "/vgi",
      corsOrigins: "*",
    });

    const res = await handlerWithCors(
      new Request(`${BASE}/vgi/add`, { method: "OPTIONS" }),
    );

    expect(res.status).toBe(204);
    expect(res.headers.get("Access-Control-Allow-Origin")).toBe("*");
    expect(res.headers.get("Access-Control-Allow-Methods")).toBe("POST, OPTIONS");
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
    // Should have 3 data batches (n=0,1,2)
    expect(batches.length).toBe(3);
    expect(batches[0].getChildAt(0)?.get(0)).toBe(0);
    expect(batches[1].getChildAt(0)?.get(0)).toBe(1);
    expect(batches[2].getChildAt(0)?.get(0)).toBe(2);
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
    const exchangeBody = buildRequestIpc(
      inputSchema,
      { value: [5] },
      "scale",
      exchangeMeta,
    );

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
    let token = initBatches[0].metadata?.get(STATE_KEY)!;

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
      token = batches[0].metadata?.get(STATE_KEY)!;
      expect(token).toBeDefined();
    }
  });
});
