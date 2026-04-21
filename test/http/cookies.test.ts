// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { beforeAll, describe, expect, test } from "bun:test";
import {
  Field,
  Int32,
  RecordBatch,
  RecordBatchReader,
  RecordBatchStreamWriter,
  recordBatchFromArrays,
  Schema,
  Utf8,
} from "@query-farm/apache-arrow";
import { REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY } from "../../src/constants.js";
import { ARROW_CONTENT_TYPE } from "../../src/http/common.js";
import { createHttpHandler, int32, Protocol, str } from "../../src/index.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function buildRequestIpc(
  schema: Schema,
  values: Record<string, any[]>,
  methodName: string,
): Uint8Array {
  const batch = recordBatchFromArrays(values, schema);
  const meta = new Map<string, string>();
  meta.set(RPC_METHOD_KEY, methodName);
  meta.set(REQUEST_VERSION_KEY, REQUEST_VERSION);
  const batchWithMeta = new RecordBatch(schema, batch.data, meta);

  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, schema);
  writer.write(batchWithMeta);
  writer.close();
  return writer.toUint8Array(true);
}

async function readResponseResult(response: Response): Promise<Record<string, any>> {
  const body = new Uint8Array(await response.arrayBuffer());
  const reader = await RecordBatchReader.from(body);
  const batches = reader.readAll();
  // Last batch is the result batch (preceded by log batches if any).
  const last = batches[batches.length - 1];
  const out: Record<string, any> = {};
  for (let i = 0; i < last.schema.fields.length; i++) {
    const field = last.schema.fields[i];
    out[field.name] = last.getChildAt(i)?.get(0);
  }
  return out;
}

// ---------------------------------------------------------------------------
// Test protocol
// ---------------------------------------------------------------------------

function makeCookieProtocol(): Protocol {
  const p = new Protocol("CookieService");

  p.unary("whoami_cookie", {
    params: {},
    result: { value: str },
    handler: (_params, ctx) => ({ value: ctx.cookies.get("sid") ?? "" }),
  });

  p.unary("set_sid", {
    params: { value: str, maxAge: int32 },
    result: { status: str },
    handler: (params, ctx) => {
      ctx.setCookie("sid", params.value, {
        maxAge: params.maxAge,
        httpOnly: true,
        path: "/",
        sameSite: "Lax",
      });
      return { status: "set" };
    },
  });

  p.unary("delete_sid", {
    params: {},
    result: { status: str },
    handler: (_params, ctx) => {
      ctx.deleteCookie("sid", { path: "/" });
      return { status: "deleted" };
    },
  });

  p.unary("fail_after_set", {
    params: {},
    result: { status: str },
    handler: (_params, ctx) => {
      ctx.setCookie("on_error", "yes", { maxAge: 60, path: "/" });
      throw new Error("boom after cookie");
    },
  });

  p.producer<{ count: number; current: number }>("stream_sets_cookie", {
    params: { count: int32 },
    outputSchema: { n: int32 },
    init: ({ count }) => ({ count, current: 0 }),
    produce: (state, out) => {
      // This must throw — setCookie is disallowed in streaming methods.
      (out as any).setCookie("leak", "no");
      if (state.current >= state.count) {
        out.finish();
        return;
      }
      out.emitRow({ n: state.current });
      state.current++;
    },
  });

  return p;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("HTTP cookies", () => {
  const BASE = "http://localhost:9999";
  let handler: (req: Request) => Response | Promise<Response>;

  beforeAll(() => {
    handler = createHttpHandler(makeCookieProtocol(), { serverId: "test-server" });
  });

  test("ctx.cookies surfaces incoming Cookie header", async () => {
    const body = buildRequestIpc(new Schema([]), {}, "whoami_cookie");
    const res = await handler(
      new Request(`${BASE}/whoami_cookie`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE, Cookie: "sid=abc; other=1" },
        body,
      }),
    );
    expect(res.status).toBe(200);
    const result = await readResponseResult(res);
    expect(result.value).toBe("abc");
  });

  test("ctx.cookies is empty when no Cookie header", async () => {
    const body = buildRequestIpc(new Schema([]), {}, "whoami_cookie");
    const res = await handler(
      new Request(`${BASE}/whoami_cookie`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );
    const result = await readResponseResult(res);
    expect(result.value).toBe("");
  });

  test("setCookie emits a Set-Cookie header with the expected attributes", async () => {
    const paramsSchema = new Schema([
      new Field("value", new Utf8(), false),
      new Field("maxAge", new Int32(), false),
    ]);
    const body = buildRequestIpc(paramsSchema, { value: ["xyz"], maxAge: [60] }, "set_sid");
    const res = await handler(
      new Request(`${BASE}/set_sid`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );
    expect(res.status).toBe(200);
    const setCookie = res.headers.get("Set-Cookie") ?? "";
    expect(setCookie).toContain("sid=xyz");
    expect(setCookie).toContain("Max-Age=60");
    expect(setCookie).toContain("Path=/");
    expect(setCookie).toContain("HttpOnly");
    expect(setCookie).toContain("SameSite=Lax");
  });

  test("deleteCookie emits a Set-Cookie header with Max-Age=0", async () => {
    const body = buildRequestIpc(new Schema([]), {}, "delete_sid");
    const res = await handler(
      new Request(`${BASE}/delete_sid`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );
    expect(res.status).toBe(200);
    const setCookie = res.headers.get("Set-Cookie") ?? "";
    expect(setCookie).toContain("sid=");
    expect(setCookie).toContain("Max-Age=0");
    expect(setCookie).toContain("Path=/");
  });

  test("cookies queued before an error are still emitted on the error response", async () => {
    const body = buildRequestIpc(new Schema([]), {}, "fail_after_set");
    const res = await handler(
      new Request(`${BASE}/fail_after_set`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );
    // Server errors come back as 200 with X-VGI-RPC-Error: true
    expect(res.status).toBe(200);
    expect(res.headers.get("X-VGI-RPC-Error")).toBe("true");
    const setCookie = res.headers.get("Set-Cookie") ?? "";
    expect(setCookie).toContain("on_error=yes");
    expect(setCookie).toContain("Max-Age=60");
  });

  test("setCookie from a streaming method throws", async () => {
    const paramsSchema = new Schema([new Field("count", new Int32(), false)]);
    const body = buildRequestIpc(paramsSchema, { count: [2] }, "stream_sets_cookie");
    const res = await handler(
      new Request(`${BASE}/stream_sets_cookie/init`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );
    // Producer errors surface as zero-row batches with error metadata in
    // the Arrow IPC stream (HTTP status 200).
    expect(res.status).toBe(200);
    const reader = await RecordBatchReader.from(new Uint8Array(await res.arrayBuffer()));
    const batches = reader.readAll();
    // The error batch carries metadata with vgi_rpc.log_level=EXCEPTION and
    // vgi_rpc.log_message carrying the error message.
    const errBatch = batches[batches.length - 1];
    const msg = errBatch.metadata?.get("vgi_rpc.log_message") ?? "";
    expect(msg.toLowerCase()).toContain("unary");
  });
});
