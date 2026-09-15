// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * An HTTP stream emits one access record per turn — the `/init` and every
 * `/exchange` — and they are tied together by one `stream_id`.
 *
 * `docs/access-log-spec.md` is "one record per RPC call", and over HTTP a
 * stream *is* many requests: the init and each continuation are separate
 * round trips, so the rule resolves to one record each. This port satisfies
 * that because `createHttpHandler` builds its `DispatchInfo` on the single
 * path all three actions (`call`, `init`, `exchange`) flow through, rather
 * than in a unary-only branch.
 *
 * Nothing here was previously pinned end-to-end, and that is the whole
 * reason this file exists. `test/access-log.test.ts` drives `AccessLogHook`
 * with hand-built `DispatchInfo` values, so it proves what the hook does
 * with a `streamId` it is *handed* — not that the HTTP handler ever hands it
 * one, nor that it emits anything at all on the stream path. A port that
 * emitted zero records for streams passes every record-shape check ever
 * written, because a validator validates the records that exist: no records
 * means nothing to invalidate, and the silence reads as "clean" rather than
 * "unexamined". Two sibling ports were in exactly that state and it took
 * reading their source to find out. Streams are also the calls that run
 * longest and move the most data, so that particular silence drops precisely
 * the traffic an operator most wants.
 *
 * So these cases assert the records are *there*, from the real handler,
 * across a real multi-turn stream — the half no schema can check.
 */

import { beforeEach, describe, expect, test } from "bun:test";
import {
  Field,
  Int32,
  RecordBatch,
  RecordBatchReader,
  RecordBatchStreamWriter,
  recordBatchFromArrays,
  Schema,
} from "@query-farm/apache-arrow";
import { AccessLogHook, type AccessLogSink } from "../../src/access-log.js";
import { PROTOCOL_KEY, REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY, STATE_KEY } from "../../src/constants.js";
import { ARROW_CONTENT_TYPE } from "../../src/http/common.js";
import { createHttpHandler } from "../../src/http/handler.js";
import { int32, Protocol } from "../../src/index.js";

const PROTOCOL_NAME = "StreamLog";
const RPC = `http://localhost:9999/vgi/${PROTOCOL_NAME}`;

const PARAM_SCHEMA = new Schema([new Field("count", new Int32(), false)]);

function buildRequestIpc(
  values: Record<string, unknown[]>,
  methodName: string,
  extra?: Map<string, string>,
): Uint8Array {
  const batch = recordBatchFromArrays(values, PARAM_SCHEMA);
  const meta = extra ?? new Map<string, string>();
  meta.set(RPC_METHOD_KEY, methodName);
  if (!meta.has(PROTOCOL_KEY)) meta.set(PROTOCOL_KEY, PROTOCOL_NAME);
  meta.set(REQUEST_VERSION_KEY, REQUEST_VERSION);
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, PARAM_SCHEMA);
  writer.write(new RecordBatch(PARAM_SCHEMA, batch.data, meta));
  writer.close();
  return writer.toUint8Array(true);
}

function makeProtocol(): Protocol {
  return new Protocol(PROTOCOL_NAME)
    .unary("noop", {
      params: { count: int32 },
      result: { count: int32 },
      handler: ({ count }) => ({ count }),
    })
    .producer<{ count: number; current: number }>("produce_n", {
      params: { count: int32 },
      outputSchema: { n: int32 },
      init: ({ count }) => ({ count: count as number, current: 0 }),
      produce: (state, out) => {
        if (state.current >= state.count) {
          out.finish();
          return;
        }
        out.emitRow({ n: state.current });
        state.current++;
      },
    });
}

interface Record_ {
  method: string;
  method_type: string;
  stream_id?: string;
  request_data?: string;
  truncated?: string;
  http_status?: number;
  status: string;
}

let records: Record_[];
let handler: (req: Request) => Response | Promise<Response>;

function sink(): AccessLogSink {
  return { write: (line: string) => records.push(JSON.parse(line)) };
}

beforeEach(() => {
  records = [];
  // DEBUG so `request_data` survives onto the record: at INFO the hook
  // deliberately replaces the base64 payload with a `payload_omitted`
  // marker, and the assertion below is about *which turns carry the
  // request*, which the marker would obscure.
  handler = createHttpHandler(makeProtocol(), {
    prefix: "/vgi",
    dispatchHook: new AccessLogHook(sink(), { level: "DEBUG" }),
  });
});

async function post(url: string, body: Uint8Array): Promise<Response> {
  return handler(
    new Request(url, { method: "POST", headers: { "Content-Type": ARROW_CONTENT_TYPE }, body: body as BodyInit }),
  );
}

/** Drive `produce_n` from `/init` to termination, returning the turn count. */
async function driveStream(count: number): Promise<number> {
  const init = await post(`${RPC}/produce_n/init`, buildRequestIpc({ count: [count] }, "produce_n"));
  expect(init.status).toBe(200);

  let turns = 1;
  let token = await cursorOf(init);
  // Bounded: a producer that never terminates is a different bug, and an
  // unbounded loop here would hang the suite rather than report it.
  while (token && turns < count + 5) {
    const meta = new Map<string, string>([[STATE_KEY, token]]);
    const next = await post(`${RPC}/produce_n/exchange`, buildRequestIpc({ count: [count] }, "produce_n", meta));
    expect(next.status).toBe(200);
    turns++;
    token = await cursorOf(next);
  }
  return turns;
}

async function cursorOf(response: Response): Promise<string | undefined> {
  const reader = await RecordBatchReader.from(new Uint8Array(await response.arrayBuffer()));
  const batches = reader.readAll();
  return batches[batches.length - 1]?.metadata?.get(STATE_KEY);
}

describe("an HTTP stream emits one access record per turn", () => {
  test("init and every continuation are logged, sharing one stream_id", async () => {
    const turns = await driveStream(3);
    // More than one turn, or the multi-record claim is vacuous — a stream
    // that finished inside `/init` would make every assertion below pass
    // while proving nothing about continuations.
    expect(turns).toBeGreaterThan(1);

    const streamRecords = records.filter((r) => r.method === "produce_n");
    // The assertion the whole file exists for: records at all, and exactly
    // as many as there were HTTP turns.
    expect(streamRecords.length).toBe(turns);

    for (const rec of streamRecords) {
      expect(rec.method_type).toBe("stream");
      // 32 lowercase hex, no dashes -- the spec's shape, and not the
      // all-zeros sentinel reserved for a request that failed before a
      // stream existed.
      expect(rec.stream_id).toMatch(/^[0-9a-f]{32}$/);
      expect(rec.stream_id).not.toBe("0".repeat(32));
      expect(rec.http_status).toBe(200);
      expect(rec.status).toBe("ok");
    }

    expect(new Set(streamRecords.map((r) => r.stream_id)).size).toBe(1);
  });

  test("request_data rides on the init record and on no continuation", async () => {
    await driveStream(3);
    const streamRecords = records.filter((r) => r.method === "produce_n");
    const withRequest = streamRecords.filter((r) => r.request_data !== undefined);
    // Spec §4.3: the init carries the request batch; a continuation batch is
    // not a request batch and must not claim to be one.
    expect(withRequest.length).toBe(1);
    expect(streamRecords[0].request_data).toBeDefined();
    expect(streamRecords.slice(1).every((r) => r.request_data === undefined)).toBe(true);
  });

  test("two streams of the same method do not share an id", async () => {
    // A `stream_id` that is per-method, per-server, or a constant would pass
    // every assertion above. It has to separate concurrent calls or it joins
    // unrelated traffic into one apparent stream.
    await driveStream(3);
    const first = records.filter((r) => r.method === "produce_n").map((r) => r.stream_id);
    records = [];
    await driveStream(3);
    const second = records.filter((r) => r.method === "produce_n").map((r) => r.stream_id);
    expect(first[0]).toBeDefined();
    expect(second[0]).toBeDefined();
    expect(first[0]).not.toBe(second[0]);
  });

  test("a unary call on the same server is still labelled unary and carries no stream_id", async () => {
    // The stream fields must not leak onto every record: if `method_type`
    // were hardcoded to "stream", or `stream_id` stamped unconditionally,
    // the cases above would pass and the log would be wrong.
    await post(`${RPC}/noop`, buildRequestIpc({ count: [1] }, "noop"));
    const rec = records.find((r) => r.method === "noop");
    expect(rec?.method_type).toBe("unary");
    expect(rec?.stream_id).toBeUndefined();
    expect(rec?.request_data).toBeDefined();
  });
});
