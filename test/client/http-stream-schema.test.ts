// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { expect, test } from "bun:test";
import { Field, Float64, List, RecordBatchReader, Schema, Utf8 } from "@query-farm/apache-arrow";
import { HttpStreamSession } from "../../src/client/stream.js";
import { serializeIpcStream } from "../../src/http/common.js";

test("HTTP exchange encodes rows with the declared input schema", async () => {
  const inputSchema = new Schema([
    new Field("all_null_float", new Float64(), true),
    new Field("labels", new List(new Field("item", new Utf8(), true)), true),
  ]);
  const outputSchema = new Schema([]);
  let observed: Schema | undefined;
  const session = new HttpStreamSession({
    baseUrl: "https://rpc.example",
    prefix: "/vgi",
    method: "typed_exchange",
    stateToken: "cursor",
    outputSchema,
    inputSchema,
    pendingBatches: [],
    finished: false,
    header: null,
    postFn: async (_url, body) => {
      const reader = await RecordBatchReader.from(body);
      await reader.open();
      observed = reader.schema;
      return new Response(serializeIpcStream(outputSchema, []), { status: 200 });
    },
  });

  await session.exchange([{ all_null_float: null, labels: ["a", "b"] }]);
  expect(String(observed?.fields[0].type)).toBe(String(inputSchema.fields[0].type));
  expect(String(observed?.fields[1].type)).toBe(String(inputSchema.fields[1].type));
});
