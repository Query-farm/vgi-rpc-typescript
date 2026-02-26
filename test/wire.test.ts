// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from "bun:test";
import {
  Schema,
  Field,
  Float64,
  Utf8,
  Int32,
  RecordBatch,
  RecordBatchStreamWriter,
  RecordBatchReader,
  vectorFromArray,
  makeData,
  Struct,
} from "apache-arrow";
import { buildResultBatch, buildErrorBatch, buildEmptyBatch } from "../src/wire/response.js";
import { parseRequest } from "../src/wire/request.js";
import { IpcStreamWriter } from "../src/wire/writer.js";
import { IpcStreamReader } from "../src/wire/reader.js";
import { RpcError, VersionError } from "../src/errors.js";
import {
  RPC_METHOD_KEY,
  REQUEST_VERSION_KEY,
  LOG_LEVEL_KEY,
  LOG_MESSAGE_KEY,
  SERVER_ID_KEY,
} from "../src/constants.js";
import { openSync, closeSync, readFileSync, unlinkSync } from "node:fs";
import { tmpdir } from "node:os";

describe("buildResultBatch", () => {
  it("builds a 1-row result batch with metadata", () => {
    const schema = new Schema([new Field("result", new Float64(), false)]);
    const batch = buildResultBatch(schema, { result: 42.0 }, "srv1", "req1");

    expect(batch.numRows).toBe(1);
    expect(batch.getChildAt(0)?.get(0)).toBe(42.0);
    expect(batch.metadata.get(SERVER_ID_KEY)).toBe("srv1");
  });

  it("builds an empty-schema result batch", () => {
    const schema = new Schema([]);
    const batch = buildResultBatch(schema, {}, "srv1", null);

    expect(batch.numRows).toBe(0);
    expect(batch.metadata.get(SERVER_ID_KEY)).toBe("srv1");
  });

  it("builds a string result batch", () => {
    const schema = new Schema([new Field("result", new Utf8(), false)]);
    const batch = buildResultBatch(schema, { result: "hello" }, "srv1", null);

    expect(batch.numRows).toBe(1);
    expect(batch.getChildAt(0)?.get(0)).toBe("hello");
  });
});

describe("buildErrorBatch", () => {
  it("builds a 0-row error batch with EXCEPTION metadata", () => {
    const schema = new Schema([new Field("result", new Float64(), false)]);
    const error = new Error("Something went wrong");
    const batch = buildErrorBatch(schema, error, "srv1", "req1");

    expect(batch.numRows).toBe(0);
    expect(batch.metadata.get(LOG_LEVEL_KEY)).toBe("EXCEPTION");
    expect(batch.metadata.get(LOG_MESSAGE_KEY)).toContain("Something went wrong");
    expect(batch.metadata.get(SERVER_ID_KEY)).toBe("srv1");

    const extra = JSON.parse(batch.metadata.get("vgi_rpc.log_extra")!);
    expect(extra.exception_type).toBe("Error");
    expect(extra.exception_message).toBe("Something went wrong");
    expect(extra.traceback).toContain("Error: Something went wrong");
  });
});

describe("parseRequest", () => {
  it("parses a valid request", () => {
    const schema = new Schema([
      new Field("a", new Float64(), false),
      new Field("b", new Float64(), false),
    ]);
    const md = new Map([
      [RPC_METHOD_KEY, "add"],
      [REQUEST_VERSION_KEY, "1"],
    ]);
    const aArr = vectorFromArray([3.0], new Float64());
    const bArr = vectorFromArray([4.0], new Float64());
    const data = makeData({
      type: new Struct(schema.fields),
      length: 1,
      children: [aArr.data[0], bArr.data[0]],
      nullCount: 0,
    });
    const batch = new RecordBatch(schema, data, md);

    const parsed = parseRequest(schema, batch);
    expect(parsed.methodName).toBe("add");
    expect(parsed.params.a).toBe(3.0);
    expect(parsed.params.b).toBe(4.0);
  });

  it("throws RpcError for missing method", () => {
    const schema = new Schema([]);
    const md = new Map([[REQUEST_VERSION_KEY, "1"]]);
    const data = makeData({ type: new Struct([]), length: 0, children: [], nullCount: 0 });
    const batch = new RecordBatch(schema, data, md);

    expect(() => parseRequest(schema, batch)).toThrow(RpcError);
  });

  it("throws VersionError for missing version", () => {
    const schema = new Schema([]);
    const md = new Map([[RPC_METHOD_KEY, "test"]]);
    const data = makeData({ type: new Struct([]), length: 0, children: [], nullCount: 0 });
    const batch = new RecordBatch(schema, data, md);

    expect(() => parseRequest(schema, batch)).toThrow(VersionError);
  });

  it("throws VersionError for wrong version", () => {
    const schema = new Schema([]);
    const md = new Map([
      [RPC_METHOD_KEY, "test"],
      [REQUEST_VERSION_KEY, "99"],
    ]);
    const data = makeData({ type: new Struct([]), length: 0, children: [], nullCount: 0 });
    const batch = new RecordBatch(schema, data, md);

    expect(() => parseRequest(schema, batch)).toThrow(VersionError);
  });
});

describe("IPC roundtrip", () => {
  it("writes and reads a complete IPC stream with metadata", async () => {
    const schema = new Schema([new Field("x", new Float64(), false)]);
    const md = new Map([["key", "value"]]);
    const arr = vectorFromArray([42.0], new Float64());
    const data = makeData({
      type: new Struct(schema.fields),
      length: 1,
      children: [arr.data[0]],
      nullCount: 0,
    });
    const batch = new RecordBatch(schema, data, md);

    // Write
    const writer = new RecordBatchStreamWriter();
    writer.reset(undefined, schema);
    writer.write(batch);
    writer.close();
    const bytes = writer.toUint8Array(true);

    // Read
    const reader = await RecordBatchReader.from(bytes);
    await reader.open({ autoDestroy: false });
    const result = await reader.next();

    expect(result.done).toBe(false);
    expect(result.value.numRows).toBe(1);
    expect(result.value.getChildAt(0)?.get(0)).toBe(42);
    expect(result.value.metadata.get("key")).toBe("value");
  });
});

describe("IpcStreamWriter", () => {
  it("writes a complete IPC stream to output", () => {
    const tmpPath = `${tmpdir()}/ipc-test-${Date.now()}.arrow`;
    const fd = openSync(tmpPath, "w");

    const schema = new Schema([new Field("x", new Float64(), false)]);
    const arr = vectorFromArray([1.0], new Float64());
    const data = makeData({
      type: new Struct(schema.fields),
      length: 1,
      children: [arr.data[0]],
      nullCount: 0,
    });
    const batch = new RecordBatch(schema, data);

    const writer = new IpcStreamWriter(fd);
    writer.writeStream(schema, [batch]);
    closeSync(fd);

    const bytes = readFileSync(tmpPath);
    expect(bytes.length).toBeGreaterThan(0);

    unlinkSync(tmpPath);
  });
});
