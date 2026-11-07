// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// The `int64` client option: row-object results either narrow safe int64
// values to `number` (default "number-if-safe", which mixes number and bigint
// within one column) or keep every int64 a `bigint` ("bigint"). Exercised over
// HTTP (in-process handler) and the pipe transport (subprocess child), for a
// unary result, a stream header, and producer rows.

import { describe, expect, test } from "bun:test";
import type { RecordBatch } from "@query-farm/apache-arrow";
import { batchFromColumns, field, int64, schema } from "#vgi-rpc-arrow";
import { httpConnect, type RpcClient, subprocessConnect } from "../src/client/index.js";
import { extractBatchRows, type Int64Mode } from "../src/client/ipc.js";
import { createHttpHandler } from "../src/http/handler.js";
import { BIG, protocol } from "./fixtures/int64-protocol.js";

const CHILD = ["bun", "run", `${import.meta.dir}/fixtures/int64-protocol.ts`];

const lanes: Array<[string, (int64?: Int64Mode) => RpcClient]> = [
  [
    "http",
    (int64) => {
      const handler = createHttpHandler(protocol);
      return httpConnect("http://int64.test", { fetch: (input, init) => handler(new Request(input, init)), int64 });
    },
  ],
  ["pipe", (int64) => subprocessConnect(CHILD, { int64 })],
];

async function collect(client: RpcClient) {
  const unary = await client.call("pair", { label: "x" });
  const session = await client.stream("values", {});
  const header = session.header;
  const rows: Record<string, any>[] = [];
  for await (const batch of session) rows.push(...batch);
  session.close();
  return { unary, header, values: rows.map((r) => r.v) };
}

for (const [lane, connect] of lanes) {
  describe(`int64 client option (${lane})`, () => {
    test("default narrows safe values (mixed number/bigint)", async () => {
      const client = connect();
      try {
        const { unary, header, values } = await collect(client);
        expect(unary).toEqual({ small: 7, big: BIG, label: "x" });
        expect(header).toEqual({ small: 3, big: BIG });
        expect(values).toEqual([1, BIG, -5]);
      } finally {
        client.close();
      }
    }, 30_000);

    test('"number-if-safe" is the explicit spelling of the default', async () => {
      const client = connect("number-if-safe");
      try {
        expect((await collect(client)).values).toEqual([1, BIG, -5]);
      } finally {
        client.close();
      }
    }, 30_000);

    test('"bigint" keeps every int64 a bigint', async () => {
      const client = connect("bigint");
      try {
        const { unary, header, values } = await collect(client);
        expect(unary).toEqual({ small: 7n, big: BIG, label: "x" });
        expect(header).toEqual({ small: 3n, big: BIG });
        expect(values).toEqual([1n, BIG, -5n]);
        for (const v of values) expect(typeof v).toBe("bigint");
      } finally {
        client.close();
      }
    }, 30_000);
  });
}

describe("extractBatchRows int64 parameter", () => {
  const batch = batchFromColumns(schema([field("v", int64(), true)]), {
    v: [1n, BIG, null],
  }) as unknown as RecordBatch;

  test("default narrows safe values", () => {
    expect(extractBatchRows(batch).map((r) => r.v)).toEqual([1, BIG, null]);
  });

  test('"bigint" leaves values untouched', () => {
    expect(extractBatchRows(batch, "bigint").map((r) => r.v)).toEqual([1n, BIG, null]);
  });
});
