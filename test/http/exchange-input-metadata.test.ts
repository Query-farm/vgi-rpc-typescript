// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * A stream method is handed its input's own custom metadata over HTTP, on
 * every turn -- and none of the transport's bookkeeping.
 *
 * That metadata is application data, per input: DuckDB puts the
 * conditional-revalidation validators (`vgi.cache.if_none_match`) on every
 * exchange input and dynamic-filter deltas on producer ticks, and a worker
 * reads them off `input.metadata` (or `ctx.inputMetadata`). The pipe transport
 * hands the batch over as it arrived. Over HTTP each turn is its own request
 * whose batch also carries the stream's cursor and call token, so what the
 * method sees has to be rebuilt -- and the exchange path got three things
 * wrong: it handed the request batch over whole (cursor and call token
 * included), it never set `ctx.inputMetadata` at all, and for an externalized
 * input it laid the pointer's metadata over the fetched payload's, so a key
 * the two disagree on read as the pointer's.
 *
 * The rule, from the reference (`_run_http_exchange_turn`, WIRE_PROTOCOL.md
 * §12 and "Stream exchange (HTTP)"): strip `vgi_rpc.stream_state#b64`,
 * `vgi_rpc.call_state#b64` and `vgi_rpc.cancel`, pass every other key; a
 * resolved input carries the payload's metadata plus the reader's
 * `vgi_rpc.location.source` / `vgi_rpc.location.fetch_ms`, never the pointer's.
 * A producer's continuation obeys the same strip, and its first turn -- run
 * inside `/init` -- carries the `/init` request's metadata.
 *
 * Driven through the real `createHttpHandler`; only requests are built here.
 * The payload store is a real HTTP server holding bytes this test uploaded.
 */

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import {
  Field,
  Float64,
  Int32,
  RecordBatch,
  RecordBatchReader,
  RecordBatchStreamWriter,
  recordBatchFromArrays,
  Schema,
} from "@query-farm/apache-arrow";
import {
  CALL_STATE_KEY,
  CANCEL_KEY,
  LOCATION_FETCH_MS_KEY,
  LOCATION_KEY,
  LOCATION_SHA256_KEY,
  LOCATION_SOURCE_KEY,
  PROTOCOL_KEY,
  REQUEST_VERSION,
  REQUEST_VERSION_KEY,
  RPC_METHOD_KEY,
  STATE_KEY,
} from "../../src/constants.js";
import type { ExternalStorage } from "../../src/external.js";
import { ARROW_CONTENT_TYPE } from "../../src/http/common.js";
import { createHttpHandler } from "../../src/http/handler.js";
import { float, int32, Protocol, str } from "../../src/index.js";

const PROTOCOL_NAME = "InputMetadata";
const RPC = `http://localhost:9999/vgi/${PROTOCOL_NAME}`;
const APP_KEY = "vgi.conformance.input";
const EXTRA_KEY = "vgi.conformance.extra";
const FRAMEWORK_KEYS = [STATE_KEY, CALL_STATE_KEY, CANCEL_KEY];

const INPUT_SCHEMA = new Schema([new Field("value", new Float64(), true)]);

/** Sorted keys, comma-joined; `<unset>` distinguishes an absent map from an empty one. */
function keysOf(metadata: ReadonlyMap<string, string> | null | undefined): string {
  return metadata ? [...metadata.keys()].sort().join(",") : "<unset>";
}

/** One row per turn: the app key's value and every key, as the batch and as
 *  `ctx.inputMetadata` each report them -- so a turn on which the two
 *  accessors disagree is visible rather than averaged away. */
function makeProtocol(): Protocol {
  return new Protocol(PROTOCOL_NAME)
    .exchange<Record<string, never>>("report_input", {
      params: {},
      inputSchema: { value: float },
      outputSchema: { seen: str, keys: str, ctx_keys: str },
      init: () => ({}),
      exchange: (_state, input, out) => {
        out.emitRow({
          seen: input.metadata?.get(APP_KEY) ?? "",
          keys: keysOf(input.metadata),
          ctx_keys: keysOf(out.inputMetadata),
        });
      },
    })
    .producer<{ count: number; current: number }>("report_tick", {
      params: { count: int32 },
      outputSchema: { seen: str, ctx_keys: str },
      init: ({ count }) => ({ count: count as number, current: 0 }),
      produce: (state, out) => {
        out.emitRow({ seen: out.inputMetadata?.get(APP_KEY) ?? "", ctx_keys: keysOf(out.inputMetadata) });
        state.current++;
        if (state.current >= state.count) out.finish();
      },
    });
}

/** Resolving an input needs no upload; the storage is here because the
 *  config type requires one. */
const NO_UPLOADS: ExternalStorage = {
  upload: async () => {
    throw new Error("this test uploads nothing server-side");
  },
};

let handler: (req: Request) => Response | Promise<Response>;
let payloadStore: ReturnType<typeof Bun.serve>;
const storedPayloads = new Map<string, Uint8Array>();

beforeAll(() => {
  handler = createHttpHandler(makeProtocol(), {
    prefix: "/vgi",
    // The payload store below is plain http on loopback; the default
    // validator is HTTPS-only.
    externalLocation: { storage: NO_UPLOADS, urlValidator: null },
  });
  payloadStore = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch(req) {
      const body = storedPayloads.get(new URL(req.url).pathname);
      return body ? new Response(body as BodyInit) : new Response("not found", { status: 404 });
    },
  });
});

afterAll(() => {
  payloadStore.stop(true);
});

function ipcStream(schema: Schema, batch: RecordBatch): Uint8Array {
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, schema);
  writer.write(batch);
  writer.close();
  return writer.toUint8Array(true);
}

/** A unary-shaped `/init` request, with the dispatch keys a client sends. */
function initBody(schema: Schema, values: Record<string, unknown[]>, method: string, extra?: Map<string, string>) {
  const meta = new Map(extra ?? []);
  meta.set(RPC_METHOD_KEY, method);
  meta.set(PROTOCOL_KEY, PROTOCOL_NAME);
  meta.set(REQUEST_VERSION_KEY, REQUEST_VERSION);
  return ipcStream(schema, new RecordBatch(schema, recordBatchFromArrays(values, schema).data, meta));
}

/** An exchange input as a client sends it: the input's own metadata, then the
 *  stream's cursor and call token. */
function inputBatch(value: number, metadata: Map<string, string>): RecordBatch {
  return new RecordBatch(INPUT_SCHEMA, recordBatchFromArrays({ value: [value] }, INPUT_SCHEMA).data, metadata);
}

async function post(url: string, body: Uint8Array): Promise<RecordBatch[]> {
  const response = await handler(
    new Request(url, { method: "POST", headers: { "Content-Type": ARROW_CONTENT_TYPE }, body: body as BodyInit }),
  );
  expect(response.status).toBe(200);
  const reader = await RecordBatchReader.from(new Uint8Array(await response.arrayBuffer()));
  return reader.readAll();
}

interface Tokens {
  cursor: string;
  call: string;
}

function tokensFrom(batches: RecordBatch[], call?: string): Tokens {
  const cursor = batches.map((b) => b.metadata?.get(STATE_KEY)).find((t) => t);
  const callToken = call ?? batches.map((b) => b.metadata?.get(CALL_STATE_KEY)).find((t) => t);
  expect(cursor).toBeDefined();
  expect(callToken).toBeDefined();
  return { cursor: cursor!, call: callToken! };
}

function withTokens(tokens: Tokens, metadata: Record<string, string> = {}): Map<string, string> {
  return new Map([...Object.entries(metadata), [STATE_KEY, tokens.cursor], [CALL_STATE_KEY, tokens.call]]);
}

/** The single data row of a turn's response, as strings. */
function row(batches: RecordBatch[]): Record<string, string> {
  const data = batches.filter((b) => b.numRows > 0);
  expect(data).toHaveLength(1);
  const out: Record<string, string> = {};
  for (const field of data[0].schema.fields) out[field.name] = data[0].getChild(field.name)?.get(0) as string;
  return out;
}

function expectNoFrameworkKeys(keys: string, turn: string): void {
  for (const key of FRAMEWORK_KEYS) {
    expect(keys.split(","), `${turn}: ${key} is transport bookkeeping`).not.toContain(key);
  }
}

describe("HTTP exchange: each input's own metadata reaches the method", () => {
  test("every turn, its own keys, never the cursor or call token -- on the batch and on ctx", async () => {
    let tokens = tokensFrom(await post(`${RPC}/report_input/init`, initBody(new Schema([]), {}, "report_input")));

    // Different metadata each turn, so a frozen first turn or a carried-over
    // key fails a later one; the last turn carries none and must see none.
    const turns: Array<Record<string, string>> = [
      { [APP_KEY]: "first" },
      { [APP_KEY]: "second", [EXTRA_KEY]: "1" },
      {},
    ];
    const observed: Array<Record<string, string>> = [];
    for (const [index, metadata] of turns.entries()) {
      const batches = await post(
        `${RPC}/report_input/exchange`,
        ipcStream(INPUT_SCHEMA, inputBatch(index, withTokens(tokens, metadata))),
      );
      observed.push(row(batches));
      tokens = tokensFrom(batches, tokens.call);
    }

    expect(observed.map((r) => r.seen)).toEqual(["first", "second", ""]);
    expect(observed.map((r) => r.keys)).toEqual([APP_KEY, [APP_KEY, EXTRA_KEY].sort().join(","), ""]);
    for (const [index, r] of observed.entries()) {
      expectNoFrameworkKeys(r.keys, `turn ${index + 1}`);
      // `ctx.inputMetadata` is the same metadata, not unset and not the envelope.
      expect(r.ctx_keys, `turn ${index + 1}: ctx.inputMetadata`).toBe(r.keys);
    }
  });

  test("an externalized input carries the payload's metadata and provenance, not the pointer's", async () => {
    const tokens = tokensFrom(await post(`${RPC}/report_input/init`, initBody(new Schema([]), {}, "report_input")));

    // The payload is what the client uploaded: the whole inline input,
    // tokens included, as the reference client externalizes it.
    const payload = ipcStream(INPUT_SCHEMA, inputBatch(1, withTokens(tokens, { [APP_KEY]: "from-payload" })));
    storedPayloads.set("/payload", payload);
    const url = `http://127.0.0.1:${payloadStore.port}/payload`;
    const sha256 = new Bun.CryptoHasher("sha256").update(payload).digest("hex");

    // The pointer disagrees with the payload on the application key, so a
    // server handing over the pointer's metadata -- or none -- is told which.
    const pointerMeta = withTokens(tokens, {
      [APP_KEY]: "from-pointer",
      [LOCATION_KEY]: url,
      [LOCATION_SHA256_KEY]: sha256,
    });
    const pointer = new RecordBatch(INPUT_SCHEMA, recordBatchFromArrays({ value: [] }, INPUT_SCHEMA).data, pointerMeta);
    const r = row(await post(`${RPC}/report_input/exchange`, ipcStream(INPUT_SCHEMA, pointer)));

    expect(r.seen).toBe("from-payload");
    const keys = r.keys.split(",");
    expect(keys).toContain(LOCATION_SOURCE_KEY);
    expect(keys).toContain(LOCATION_FETCH_MS_KEY);
    for (const absent of [LOCATION_KEY, LOCATION_SHA256_KEY, ...FRAMEWORK_KEYS]) {
      expect(keys, `${absent} must not reach application code`).not.toContain(absent);
    }
    expect(r.ctx_keys).toBe(r.keys);
  });
});

describe("HTTP producer: a tick's metadata is its request's, less the tokens", () => {
  const PARAMS = new Schema([new Field("count", new Int32(), false)]);

  test("/init's first tick carries the /init metadata; a continuation carries its own", async () => {
    // A cursor on an /init body is a crafted request -- an honest client has
    // none yet -- and must be stripped all the same.
    const init = await post(
      `${RPC}/report_tick/init`,
      initBody(
        PARAMS,
        { count: [2] },
        "report_tick",
        new Map([
          [APP_KEY, "at-init"],
          [STATE_KEY, "forged"],
        ]),
      ),
    );
    const first = row(init);
    expect(first.seen).toBe("at-init");
    expectNoFrameworkKeys(first.ctx_keys, "/init tick");

    const tokens = tokensFrom(init);
    const next = row(
      await post(
        `${RPC}/report_tick/exchange`,
        initBody(PARAMS, { count: [2] }, "report_tick", withTokens(tokens, { [APP_KEY]: "second" })),
      ),
    );
    expect(next.seen).toBe("second");
    expectNoFrameworkKeys(next.ctx_keys, "continuation tick");
  });
});
