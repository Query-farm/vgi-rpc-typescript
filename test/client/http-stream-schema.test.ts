// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { expect, test } from "bun:test";
import {
  Field,
  Float64,
  Int32,
  List,
  RecordBatchReader,
  recordBatchFromArrays,
  Schema,
  Utf8,
} from "@query-farm/apache-arrow";
import { HttpStreamSession } from "../../src/client/stream.js";
import { RpcError } from "../../src/errors.js";
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

const turnSchema = new Schema([new Field("n", new Int32(), false)]);
const twoBatchTurn = serializeIpcStream(turnSchema, [
  recordBatchFromArrays({ n: [1] }, turnSchema),
  recordBatchFromArrays({ n: [2] }, turnSchema),
]);

function malformedTurnSession(): HttpStreamSession {
  return new HttpStreamSession({
    baseUrl: "https://rpc.example",
    prefix: "/vgi",
    method: "bad_peer",
    stateToken: "cursor",
    outputSchema: turnSchema,
    inputSchema: turnSchema,
    pendingBatches: [],
    finished: false,
    header: null,
    postFn: async () => new Response(twoBatchTurn, { status: 200 }),
  });
}

test("HTTP producer clients reject multiple data batches in one tick", async () => {
  try {
    await malformedTurnSession().tick();
    throw new Error("expected malformed turn to fail");
  } catch (error) {
    expect(error).toBeInstanceOf(RpcError);
    expect((error as RpcError).errorType).toBe("ProtocolError");
  }
});

test("HTTP exchange clients reject multiple data batches in one turn", async () => {
  await expect(malformedTurnSession().exchange([{ n: 1 }])).rejects.toMatchObject({ errorType: "ProtocolError" });
});

test("HTTP producer iteration rejects multiple data batches in one continuation", async () => {
  const iterator = malformedTurnSession()[Symbol.asyncIterator]();
  await expect(iterator.next()).rejects.toMatchObject({ errorType: "ProtocolError" });
});

test("HTTP stream client rejects multiple init data batches", () => {
  const batches = [recordBatchFromArrays({ n: [1] }, turnSchema), recordBatchFromArrays({ n: [2] }, turnSchema)];
  expect(
    () =>
      new HttpStreamSession({
        baseUrl: "https://rpc.example",
        prefix: "/vgi",
        method: "bad_peer",
        stateToken: "cursor",
        outputSchema: turnSchema,
        pendingBatches: batches,
        finished: false,
        header: null,
      }),
  ).toThrow(/stream init returned more than one data batch/);
});
