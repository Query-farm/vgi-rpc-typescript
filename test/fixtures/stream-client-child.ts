// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Child process for test/flechette-stream-client.test.ts. `#vgi-rpc-arrow`
// resolves once per process, so the parent runs this under
// `--conditions=flechette` and under the default (arrow-js) and compares.
//
// Serves a small protocol in-process over HTTP and drives it with this port's
// HTTP client through every path that writes a batch the client built or was
// handed: exchange rows, a declared batch from this backend's facade, an
// arrow-js batch (the documented `ExchangeInput` type), a zero-row exchange,
// a raw exchange, producer continuations (iteration and an explicit tick with
// metadata), a cancel, and a raw unary call. Prints one JSON line.
//
// It also sends zero-column exchange inputs that carry a row count -- the
// shape DuckDB sends for a scalar whose arguments are all constants
// (`SELECT example.hash_seed(42)`). flechette cannot derive that count from
// columns, so its reader pins it as an own `numRows` over the prototype's
// getter-only accessor, and the server's per-input metadata rewrite then has
// to clone that batch without assigning to the accessor.

import {
  RecordBatch as ArrowRecordBatch,
  Field,
  Float64,
  makeData,
  Schema,
  Struct,
  vectorFromArray,
} from "@query-farm/apache-arrow";
import { backend, batchFromColumns, field, float64, schema, singleRowBatch, utf8 } from "#vgi-rpc-arrow";
import type { RawStreamSession } from "../../src/client/raw.js";
import { PROTOCOL_KEY, REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY } from "../../src/constants.js";
import { createHttpHandler } from "../../src/http/handler.js";
import { float, httpConnect, int32, Protocol, str } from "../../src/index.js";

let cancelsSeen = 0;

const protocol = new Protocol("demo.stream.v1")
  .unary("echo", {
    params: { text: str },
    result: { result: str },
    handler: async ({ text }) => ({ result: `echo:${text}` }),
  })
  .exchange<{ factor: number }>("scale", {
    params: { factor: float },
    inputSchema: { value: float },
    outputSchema: { value: float, app: str },
    init: async ({ factor }) => ({ factor }),
    exchange: async (state, input, out) => {
      const col = input.getChildAt(0);
      const values: number[] = [];
      for (let i = 0; i < input.numRows; i++) values.push(Number(col?.get(i)) * state.factor);
      out.emit({ value: values, app: values.map(() => input.metadata?.get("app") ?? "") });
    },
  })
  .exchange<Record<string, never>>("rowcount", {
    params: {},
    inputSchema: {},
    outputSchema: { n: int32, app: str },
    init: async () => ({}),
    exchange: async (_state, input, out) => {
      out.emit({ n: [input.numRows], app: [input.metadata?.get("app") ?? ""] });
    },
  })
  .producer<{ limit: number; current: number }>("count", {
    params: { limit: int32 },
    outputSchema: { n: int32, app: str },
    init: async ({ limit }) => ({ limit: limit as number, current: 0 }),
    // `cancel()` is best-effort and swallows a failed send, so the server's
    // hook is the evidence that the cancel batch actually arrived.
    onCancel: async () => {
      cancelsSeen++;
    },
    produce: async (state, out) => {
      if (state.current >= state.limit) {
        out.finish();
        return;
      }
      out.emitRow({ n: state.current, app: out.inputMetadata?.get("app") ?? "" });
      state.current++;
    },
  });

const server = Bun.serve({ port: 0, fetch: createHttpHandler(protocol, { prefix: "" }) });
const client = httpConnect(`http://127.0.0.1:${server.port}`);
const result: Record<string, unknown> = { backend: backend.name };
const values = (rows: Record<string, any>[]) => rows.map((r) => [Number(r.value), r.app ?? ""]);

/** Each path on its own stream, so one failure cannot hide the next. */
async function step(name: string, fn: () => Promise<unknown>): Promise<void> {
  try {
    result[name] = await fn();
  } catch (e) {
    result[name] = `error: ${String((e as Error)?.message ?? e)}`;
  }
}

async function scaleStream() {
  // A fresh exchange stream whose first turn has already happened, so the
  // step under test is a continuation.
  const s = await client.stream("scale", { factor: 2 });
  await s.exchange([{ value: 0 }]);
  return s;
}

try {
  await step("rows", async () => values(await (await scaleStream()).exchange([{ value: 1.5 }, { value: 2 }])));
  await step("facadeBatch", async () => {
    const batch = batchFromColumns(schema([field("value", float64(), true)]), { value: [5] });
    return values(await (await scaleStream()).exchange(batch as any));
  });
  await step("arrowBatch", async () => {
    const f = new Field("value", new Float64(), true);
    const batch = new ArrowRecordBatch(
      new Schema([f]),
      makeData({
        type: new Struct([f]),
        length: 1,
        nullCount: 0,
        children: [vectorFromArray([7], new Float64()).data[0]],
      }),
    );
    return values(await (await scaleStream()).exchange(batch));
  });
  await step("zeroRows", async () => values(await (await scaleStream()).exchange([])));
  // Zero columns, three rows: the input reaches the method with its row count,
  // both with no application metadata (the rewrite clears) and with some (it
  // attaches).
  const zeroColumnBatch = () =>
    new ArrowRecordBatch(new Schema([]), makeData({ type: new Struct([]), length: 3, nullCount: 0, children: [] }));
  await step("zeroColumnRows", async () =>
    (await (await client.stream("rowcount", {})).exchange(zeroColumnBatch())).map((r) => [Number(r.n), r.app]),
  );
  await step("zeroColumnRowsWithMetadata", async () => {
    const s = (await client.stream("rowcount", {})) as unknown as RawStreamSession;
    const raw = await s.exchangeRaw({ batch: zeroColumnBatch(), metadata: new Map([["app", "zc"]]) });
    return raw ? [Number(raw.batch.getChildAt(0)?.get(0)), String(raw.batch.getChildAt(1)?.get(0))] : null;
  });
  await step("rawExchange", async () => {
    const raw = await ((await scaleStream()) as unknown as RawStreamSession).exchangeRaw({
      batch: batchFromColumns(schema([field("value", float64(), true)]), { value: [10] }) as any,
      metadata: new Map([["app", "raw"]]),
    });
    return raw ? [Number(raw.batch.getChildAt(0)?.get(0)), String(raw.batch.getChildAt(1)?.get(0))] : null;
  });
  // Producer iteration: every batch after the first is a continuation.
  await step("produced", async () => {
    const produced: number[] = [];
    for await (const rows of await client.stream("count", { limit: 3 }))
      for (const row of rows) produced.push(Number(row.n));
    return produced;
  });
  // An explicit continuation tick carrying application metadata.
  await step("tickWithMetadata", async () => {
    const s = await client.stream("count", { limit: 5 });
    await s.tick();
    return (await s.tick(new Map([["app", "tick"]]))).map((r) => [Number(r.n), r.app]);
  });
  await step("cancelled", async () => {
    const s = await client.stream("count", { limit: 5 });
    await s.tick();
    await (s as any).cancel();
    return cancelsSeen === 1;
  });
  // A raw unary call carrying a facade batch.
  await step("rawCall", async () => {
    const echoed = await client.callRaw("echo", {
      batch: singleRowBatch(schema([field("text", utf8(), false)]), { text: "hi" }) as any,
      // The raw surface sends the caller's metadata verbatim, framing keys included.
      metadata: new Map([
        [RPC_METHOD_KEY, "echo"],
        [PROTOCOL_KEY, "demo.stream.v1"],
        [REQUEST_VERSION_KEY, REQUEST_VERSION],
      ]),
    });
    return echoed ? String(echoed.batch.getChildAt(0)?.get(0)) : null;
  });
} finally {
  server.stop(true);
}
console.log(JSON.stringify(result));
