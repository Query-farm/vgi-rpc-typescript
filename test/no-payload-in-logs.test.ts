// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * No request value and no stream state ever reaches a log.
 *
 * The framework cannot know which parameters are secret: a VGI
 * `catalog_attach` carries API keys and passwords in its options. Up to 0.28
 * this port's access log wrote the whole request as base64 Arrow IPC
 * (`request_data`) at DEBUG, so those credentials were in the log of anyone
 * who turned DEBUG on.
 *
 * Here a sentinel secret rides in a request argument and in stream state, on
 * HTTP (unary, and a multi-turn producer whose state round-trips through the
 * sealed token) and on the raw byte-stream transport, with every knob at its
 * most verbose: the access log at the (now inert) `"DEBUG"` level and
 * `VGI_DISPATCH_DEBUG` set. Every access record, and everything written to the
 * console or stderr, must contain neither the sentinel nor any base64
 * alignment of it.
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import {
  Field,
  Int32,
  RecordBatch,
  RecordBatchReader,
  RecordBatchStreamWriter,
  recordBatchFromArrays,
  Schema,
} from "@query-farm/apache-arrow";
import { AccessLogHook } from "../src/access-log.js";
import { buildRequestIpc } from "../src/client/ipc.js";
import { PROTOCOL_KEY, REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY, STATE_KEY } from "../src/constants.js";
import { ARROW_CONTENT_TYPE, rpcPath } from "../src/http/common.js";
import { createHttpHandler } from "../src/http/handler.js";
import { int32, Protocol, str } from "../src/index.js";
import { VgiRpcServer } from "../src/server.js";

const SECRET = "sk-live-SENTINEL-8f3a91c2d7e64b05";
const NAME = "NoPayload";

/** The sentinel and its base64 renderings at all three byte alignments. */
function needles(): string[] {
  const out = [SECRET];
  for (const pad of ["", "x", "xy"]) {
    const b64 = Buffer.from(pad + SECRET).toString("base64");
    // Drop the chars that depend on the padding prefix and the tail.
    const start = Math.ceil((pad.length * 4) / 3) + 1;
    out.push(b64.slice(start, start + 16));
  }
  return out;
}

function protocol(): Protocol {
  return new Protocol(NAME)
    .unary("attach", {
      params: { api_key: str, count: int32 },
      result: { ok: int32 },
      handler: () => ({ ok: 1 }),
    })
    .producer<{ apiKey: string; n: number; count: number }>("produce", {
      params: { api_key: str, count: int32 },
      outputSchema: { n: int32 },
      init: ({ api_key, count }) => ({ apiKey: api_key as string, n: 0, count: count as number }),
      produce: (state, out) => {
        if (state.n >= state.count) {
          out.finish();
          return;
        }
        out.emitRow({ n: state.n });
        state.n++;
      },
    });
}

let captured: string[];
const saved: Record<string, unknown> = {};
const CONSOLE = ["log", "info", "warn", "error", "debug"] as const;

beforeEach(() => {
  captured = [];
  for (const k of CONSOLE) {
    saved[k] = console[k];
    console[k] = (...args: unknown[]) => {
      captured.push(args.map((a) => (a instanceof Error ? `${a.message}\n${a.stack}` : String(a))).join(" "));
    };
  }
  saved.stderr = process.stderr.write;
  process.stderr.write = ((chunk: unknown) => {
    captured.push(String(chunk));
    return true;
  }) as typeof process.stderr.write;
  saved.env = process.env.VGI_DISPATCH_DEBUG;
  process.env.VGI_DISPATCH_DEBUG = "1";
});

afterEach(() => {
  for (const k of CONSOLE) console[k] = saved[k] as (typeof console)[typeof k];
  process.stderr.write = saved.stderr as typeof process.stderr.write;
  if (saved.env === undefined) delete process.env.VGI_DISPATCH_DEBUG;
  else process.env.VGI_DISPATCH_DEBUG = saved.env as string;
});

function expectClean(lines: string[]): void {
  const text = lines.join("\n");
  for (const n of needles()) expect(text).not.toContain(n);
}

function hook(lines: string[]): AccessLogHook {
  return new AccessLogHook({ write: (l: string) => lines.push(l) }, { level: "DEBUG" });
}

function requestFor(method: string): Uint8Array {
  const m = protocol().getMethod(method)!;
  return buildRequestIpc(m.paramsSchema as any, { api_key: SECRET, count: 3 }, method, { protocol: NAME });
}

/** A continuation tick: the request framing plus the cursor, no secret. */
function tickWith(token: string): Uint8Array {
  const schema = new Schema([new Field("count", new Int32(), false)]);
  const data = recordBatchFromArrays({ count: [3] }, schema);
  const meta = new Map<string, string>([
    [RPC_METHOD_KEY, "produce"],
    [PROTOCOL_KEY, NAME],
    [REQUEST_VERSION_KEY, REQUEST_VERSION],
    [STATE_KEY, token],
  ]);
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, schema);
  writer.write(new RecordBatch(schema, data.data, meta));
  writer.close();
  return writer.toUint8Array(true);
}

async function cursorOf(res: Response): Promise<string | undefined> {
  const reader = RecordBatchReader.from(new Uint8Array(await res.arrayBuffer()));
  let token: string | undefined;
  for (const batch of reader) token = batch.metadata.get(STATE_KEY) ?? token;
  return token;
}

describe("no payload value reaches a log", () => {
  test("HTTP: unary argument and multi-turn stream state", async () => {
    const access: string[] = [];
    const handler = createHttpHandler(protocol(), { dispatchHook: hook(access) });
    const post = (url: string, body: Uint8Array) =>
      handler(
        new Request(`http://x${url}`, {
          method: "POST",
          headers: { "Content-Type": ARROW_CONTENT_TYPE },
          body: body as BodyInit,
        }),
      );

    expect((await post(rpcPath(NAME, "attach"), requestFor("attach"))).status).toBe(200);

    let token = await cursorOf(await post(`${rpcPath(NAME, "produce")}/init`, requestFor("produce")));
    let turns = 1;
    while (token && turns < 10) {
      // The tick carries no secret; the secret lives in the sealed state.
      const res = await post(`${rpcPath(NAME, "produce")}/exchange`, tickWith(token));
      expect(res.status).toBe(200);
      token = await cursorOf(res);
      turns++;
    }

    // Records exist and describe the calls: a clean log of nothing proves nothing.
    const records = access.map((l) => JSON.parse(l));
    expect(records.length).toBeGreaterThanOrEqual(3);
    const unary = records.find((r) => r.method === "attach");
    expect(unary.request_fields).toEqual([
      { name: "api_key", type: "utf8" },
      { name: "count", type: "int32" },
    ]);
    expect(unary.request_rows).toBe(1);
    const stream = records.filter((r) => r.method === "produce");
    expect(stream.length).toBeGreaterThan(1);
    expect(stream.some((r) => typeof r.request_state_bytes === "number")).toBe(true);
    for (const r of records) {
      expect(r.request_data).toBeUndefined();
      expect(r.request_state).toBeUndefined();
      expect(r.response_state).toBeUndefined();
    }

    expectClean(access);
    expectClean(captured);
  });

  test("byte-stream transport: unary argument", async () => {
    const access: string[] = [];
    const readable = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(requestFor("attach"));
        controller.close();
      },
    });
    const server = new VgiRpcServer(protocol(), { dispatchHook: hook(access) });
    await server.serveConnection(readable, { write() {} });

    const records = access.map((l) => JSON.parse(l));
    expect(records.length).toBe(1);
    expect(records[0].request_fields.map((f: { name: string }) => f.name)).toEqual(["api_key", "count"]);
    expect(records[0].request_data).toBeUndefined();
    expectClean(access);
    expectClean(captured);
  });
});
