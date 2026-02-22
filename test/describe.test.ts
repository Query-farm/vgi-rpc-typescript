// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from "bun:test";
import { Schema, Field, Float64, Utf8 } from "apache-arrow";
import { Protocol } from "../src/protocol.js";
import { buildDescribeBatch, DESCRIBE_SCHEMA } from "../src/dispatch/describe.js";
import {
  PROTOCOL_NAME_KEY,
  DESCRIBE_VERSION_KEY,
  DESCRIBE_VERSION,
  REQUEST_VERSION_KEY,
  REQUEST_VERSION,
  SERVER_ID_KEY,
} from "../src/constants.js";

describe("buildDescribeBatch", () => {
  it("builds a describe batch with correct metadata", () => {
    const protocol = new Protocol("TestProtocol");
    protocol.unary("add", {
      params: new Schema([
        new Field("a", new Float64(), false),
        new Field("b", new Float64(), false),
      ]),
      result: new Schema([new Field("result", new Float64(), false)]),
      handler: async ({ a, b }) => ({ result: a + b }),
      doc: "Add two numbers.",
      paramTypes: { a: "float", b: "float" },
    });

    const { batch, metadata } = buildDescribeBatch(
      "TestProtocol",
      protocol.getMethods(),
      "test-server-id",
    );

    expect(metadata.get(PROTOCOL_NAME_KEY)).toBe("TestProtocol");
    expect(metadata.get(DESCRIBE_VERSION_KEY)).toBe(DESCRIBE_VERSION);
    expect(metadata.get(REQUEST_VERSION_KEY)).toBe(REQUEST_VERSION);
    expect(metadata.get(SERVER_ID_KEY)).toBe("test-server-id");
  });

  it("has one row per method", () => {
    const protocol = new Protocol("TestProtocol");
    protocol.unary("add", {
      params: new Schema([new Field("a", new Float64())]),
      result: new Schema([new Field("result", new Float64())]),
      handler: async ({ a }) => ({ result: a }),
    });
    protocol.unary("greet", {
      params: new Schema([new Field("name", new Utf8())]),
      result: new Schema([new Field("result", new Utf8())]),
      handler: async ({ name }) => ({ result: `Hello, ${name}!` }),
    });

    const { batch } = buildDescribeBatch(
      "TestProtocol",
      protocol.getMethods(),
      "srv1",
    );

    expect(batch.numRows).toBe(2);

    // Methods should be sorted by name
    const nameCol = batch.getChildAt(0)!;
    expect(nameCol.get(0)).toBe("add");
    expect(nameCol.get(1)).toBe("greet");
  });

  it("includes doc and param types", () => {
    const protocol = new Protocol("TestProtocol");
    protocol.unary("multiply", {
      params: new Schema([
        new Field("a", new Float64()),
        new Field("b", new Float64()),
      ]),
      result: new Schema([new Field("result", new Float64())]),
      handler: async ({ a, b }) => ({ result: a * b }),
      doc: "Multiply two numbers.",
      paramTypes: { a: "float", b: "float" },
    });

    const { batch } = buildDescribeBatch(
      "TestProtocol",
      protocol.getMethods(),
      "srv1",
    );

    // doc column (index 2)
    expect(batch.getChildAt(2)?.get(0)).toBe("Multiply two numbers.");

    // param_types_json column (index 6)
    const ptJson = batch.getChildAt(6)?.get(0);
    expect(ptJson).toBeTruthy();
    const pt = JSON.parse(ptJson);
    expect(pt.a).toBe("float");
    expect(pt.b).toBe("float");
  });

  it("has_return is true for unary with result schema", () => {
    const protocol = new Protocol("TestProtocol");
    protocol.unary("compute", {
      params: new Schema([new Field("x", new Float64())]),
      result: new Schema([new Field("result", new Float64())]),
      handler: async ({ x }) => ({ result: x }),
    });

    const { batch } = buildDescribeBatch(
      "TestProtocol",
      protocol.getMethods(),
      "srv1",
    );

    // has_return column (index 3)
    expect(batch.getChildAt(3)?.get(0)).toBe(true);
  });

  it("produces valid IPC schema bytes", () => {
    const protocol = new Protocol("TestProtocol");
    protocol.unary("test", {
      params: new Schema([new Field("x", new Float64())]),
      result: new Schema([new Field("result", new Float64())]),
      handler: async ({ x }) => ({ result: x }),
    });

    const { batch } = buildDescribeBatch(
      "TestProtocol",
      protocol.getMethods(),
      "srv1",
    );

    // params_schema_ipc column (index 4) should be a non-empty Uint8Array
    const schemaBytes = batch.getChildAt(4)?.get(0);
    expect(schemaBytes).toBeInstanceOf(Uint8Array);
    expect(schemaBytes.length).toBeGreaterThan(0);
  });
});
