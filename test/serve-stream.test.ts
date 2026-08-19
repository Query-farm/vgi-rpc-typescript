// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { expect, test } from "bun:test";
import { Field, RecordBatchReader, Schema, Utf8 } from "@query-farm/apache-arrow";
import { buildRequestIpc } from "../src/client/ipc.js";
import { LOG_MESSAGE_KEY } from "../src/constants.js";
import { float, Protocol } from "../src/index.js";
import { VgiRpcServer } from "../src/server.js";
import { type DispatchInfo, TransportKind } from "../src/types.js";

test("serveConnection threads its transport kind through hooks and call context", async () => {
  const protocol = new Protocol("transport-kind");
  let hookKind: TransportKind | undefined;
  let contextKind: TransportKind | undefined;
  const serveStartKinds: TransportKind[] = [];
  protocol.unary("echo", {
    params: { value: float },
    result: { value: float },
    handler: ({ value }, ctx) => {
      contextKind = ctx.kind;
      return { value };
    },
  });
  const method = protocol.getMethod("echo")!;
  const request = buildRequestIpc(method.paramsSchema as any, { value: 1 }, "echo");
  const readable = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(request);
      controller.enqueue(request);
      controller.close();
    },
  });
  const server = new VgiRpcServer(protocol, {
    onServeStart(kind) {
      serveStartKinds.push(kind);
    },
    dispatchHook: {
      onDispatchStart(info: DispatchInfo) {
        hookKind = info.kind;
      },
      onDispatchEnd() {},
    },
  });

  await server.serveConnection(readable, { write() {} }, TransportKind.TCP);
  expect(hookKind).toBe(TransportKind.TCP);
  expect(contextKind).toBe(TransportKind.TCP);
  expect(serveStartKinds).toEqual([TransportKind.TCP]);
});

test("serveConnection rejects a mismatched parameter schema before invoking the handler", async () => {
  const protocol = new Protocol("schema-contract");
  let invoked = false;
  protocol.unary("echo", {
    params: { value: float },
    result: { value: float },
    handler: ({ value }) => {
      invoked = true;
      return { value };
    },
  });
  const wrongSchema = new Schema([new Field("value", new Utf8(), false)]);
  const request = buildRequestIpc(wrongSchema, { value: "wrong" }, "echo");
  const readable = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(request);
      controller.close();
    },
  });
  const chunks: Uint8Array[] = [];
  await new VgiRpcServer(protocol).serveConnection(readable, {
    write(bytes) {
      chunks.push(new Uint8Array(bytes));
    },
  });

  const size = chunks.reduce((n, chunk) => n + chunk.byteLength, 0);
  const response = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    response.set(chunk, offset);
    offset += chunk.byteLength;
  }
  const reader = await RecordBatchReader.from(response);
  await reader.open();
  const [batch] = reader.readAll();
  expect(invoked).toBe(false);
  expect(batch.metadata.get(LOG_MESSAGE_KEY)).toContain("ProtocolError");
  expect(batch.metadata.get(LOG_MESSAGE_KEY)).toContain("Parameter schema mismatch");
});
