// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import {
  Decimal,
  Dictionary,
  Field,
  Float64,
  Int16,
  Int32,
  List,
  makeData,
  RecordBatch,
  Schema,
  Struct,
  TimestampMicrosecond,
  Utf8,
  vectorFromArray,
} from "@query-farm/apache-arrow";
import type { Subprocess } from "bun";
import { httpConnect } from "../src/client/connect.js";
import type { ServiceDescription } from "../src/client/introspect.js";

const PYTHON = process.env.VGI_RPC_PYTHON_BIN ?? "python3";
const canRunWorker = Bun.spawnSync([PYTHON, "-c", "import vgi_rpc.conformance.client_worker"]).exitCode === 0;
const nativeDescribe = canRunWorker ? describe : describe.skip;

const TYPED_EXCHANGE_SCHEMA = new Schema([
  new Field("nullable_float", new Float64(), true),
  new Field("tags", new List(new Field("item", new Utf8(), true)), true),
  new Field("category", new Dictionary(new Utf8(), new Int16()), true),
  new Field("event_time", new TimestampMicrosecond("UTC"), true),
  new Field("amount", new Decimal(4, 18, 128), true),
  new Field(
    "nested",
    new Struct([
      new Field("name", new Utf8(), true),
      new Field("scores", new List(new Field("item", new Int32(), true)), true),
    ]),
    true,
  ),
]);

const DESCRIPTION: ServiceDescription = {
  protocolName: "ClientConformanceService",
  protocolVersion: "",
  methods: [
    {
      name: "typed_exchange",
      type: "stream",
      paramsSchema: new Schema([]),
      resultSchema: TYPED_EXCHANGE_SCHEMA,
      inputSchema: TYPED_EXCHANGE_SCHEMA,
      outputSchema: TYPED_EXCHANGE_SCHEMA,
    },
  ],
};

function declaredBatch(columns: unknown[][]): RecordBatch {
  const length = columns[0]?.length ?? 0;
  const children = TYPED_EXCHANGE_SCHEMA.fields.map(
    (field, index) => vectorFromArray(columns[index] ?? [], field.type).data[0],
  );
  const data = makeData({
    type: new Struct(TYPED_EXCHANGE_SCHEMA.fields),
    length,
    children,
    nullCount: 0,
  });
  return new RecordBatch(TYPED_EXCHANGE_SCHEMA, data);
}

async function readPort(proc: Subprocess): Promise<string> {
  const reader = proc.stdout.getReader();
  const decoder = new TextDecoder();
  let output = "";
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    const { value, done } = await reader.read();
    if (done) break;
    output += decoder.decode(value);
    const match = output.match(/PORT:(\d+)/);
    if (match) {
      reader.releaseLock();
      const baseUrl = `http://127.0.0.1:${match[1]}`;
      // The Python launcher publishes the bound port immediately before
      // Waitress starts accepting connections. A fast CI runner can observe
      // that narrow window, so do not release the fixture until HTTP is live.
      while (Date.now() < deadline) {
        try {
          const response = await fetch(`${baseUrl}/health`);
          await response.arrayBuffer();
          return baseUrl;
        } catch {
          await Bun.sleep(25);
        }
      }
      throw new Error(`Python native-client worker did not accept HTTP connections at ${baseUrl}`);
    }
  }
  reader.releaseLock();
  throw new Error(`Python native-client worker did not announce a port: ${output}`);
}

nativeDescribe("native TypeScript client against the Python typed-exchange worker", () => {
  let proc: Subprocess;
  let baseUrl: string;

  beforeAll(async () => {
    proc = Bun.spawn([PYTHON, "-m", "vgi_rpc.conformance.client_worker", "--http", "0"], {
      stdout: "pipe",
      stderr: "pipe",
    });
    baseUrl = await readPort(proc);
  }, 10_000);

  afterAll(() => {
    proc?.kill();
  });

  test("preserves the declared schema for an all-null row", async () => {
    const client = httpConnect(baseUrl, { description: DESCRIPTION });
    const session = await client.stream("typed_exchange");
    try {
      const rows = await session.exchange(declaredBatch(TYPED_EXCHANGE_SCHEMA.fields.map(() => [null])));
      expect(rows).toEqual([
        {
          nullable_float: null,
          tags: null,
          category: null,
          event_time: null,
          amount: null,
          nested: null,
        },
      ]);
    } finally {
      session.close();
      client.close();
    }
  });

  test("preserves the complete declared schema for a zero-row batch", async () => {
    const client = httpConnect(baseUrl, { description: DESCRIPTION });
    const session = await client.stream("typed_exchange");
    try {
      expect(await session.exchange(declaredBatch(TYPED_EXCHANGE_SCHEMA.fields.map(() => [])))).toEqual([]);
    } finally {
      session.close();
      client.close();
    }
  });

  test("round-trips populated dictionary, temporal, decimal, list, and nested values", async () => {
    const client = httpConnect(baseUrl, { description: DESCRIPTION });
    const session = await client.stream("typed_exchange");
    try {
      const decimal = new Uint32Array([12_345_000, 0, 0, 0]);
      const rows = await session.exchange(
        declaredBatch([
          [1.5],
          [["alpha", null, "omega"]],
          ["blue"],
          [new Date("2026-08-18T12:34:56.000Z")],
          [decimal],
          [{ name: "sample", scores: [1, null, 3] }],
        ]),
      );

      expect(rows).toHaveLength(1);
      expect(rows[0].nullable_float).toBe(1.5);
      expect(rows[0].tags.toJSON()).toEqual(["alpha", null, "omega"]);
      expect(rows[0].category).toBe("blue");
      expect(rows[0].event_time).toBe(new Date("2026-08-18T12:34:56.000Z").valueOf());
      expect(Array.from(rows[0].amount)).toEqual(Array.from(decimal));
      const nested = rows[0].nested.toJSON();
      expect(nested.name).toBe("sample");
      expect(nested.scores.toJSON()).toEqual([1, null, 3]);
    } finally {
      session.close();
      client.close();
    }
  });
});
