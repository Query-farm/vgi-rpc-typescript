import { describe, it, expect } from "bun:test";
import { Schema, Field, Utf8, Int64, Float64, RecordBatch } from "apache-arrow";
import { OutputCollector } from "../src/types.js";
import { buildResultBatch } from "../src/wire/response.js";
import { SERVER_ID_KEY } from "../src/constants.js";

const textSchema = new Schema([
  new Field("name", new Utf8(), false),
  new Field("value", new Float64(), false),
]);

const int64Schema = new Schema([
  new Field("id", new Int64(), false),
  new Field("name", new Utf8(), false),
]);

describe("OutputCollector.emit(columns)", () => {
  it("builds batch from Record<string, any[]>", () => {
    const out = new OutputCollector(textSchema);
    out.emit({ name: ["alice", "bob"], value: [1.0, 2.0] });
    expect(out.batches).toHaveLength(1);
    expect(out.batches[0].batch.numRows).toBe(2);
  });

  it("coerces Number → BigInt for Int64 columns", () => {
    const out = new OutputCollector(int64Schema);
    out.emit({ id: [1, 2, 3], name: ["a", "b", "c"] });
    const batch = out.batches[0].batch;
    expect(batch.numRows).toBe(3);
    expect(batch.getChildAt(0)?.get(0)).toBe(1n);
    expect(batch.getChildAt(0)?.get(1)).toBe(2n);
  });

  it("throws on second data batch emission", () => {
    const out = new OutputCollector(textSchema);
    out.emit({ name: ["alice"], value: [1.0] });
    expect(() => out.emit({ name: ["bob"], value: [2.0] })).toThrow(
      "Only one data batch may be emitted per call",
    );
  });
});

describe("OutputCollector.emitRow", () => {
  it("produces a 1-row batch", () => {
    const out = new OutputCollector(textSchema);
    out.emitRow({ name: "alice", value: 42.0 });
    expect(out.batches).toHaveLength(1);
    expect(out.batches[0].batch.numRows).toBe(1);
    expect(out.batches[0].batch.getChildAt(0)?.get(0)).toBe("alice");
    expect(out.batches[0].batch.getChildAt(1)?.get(0)).toBe(42.0);
  });

  it("coerces Int64 values", () => {
    const out = new OutputCollector(int64Schema);
    out.emitRow({ id: 99, name: "test" });
    expect(out.batches[0].batch.getChildAt(0)?.get(0)).toBe(99n);
  });
});

describe("OutputCollector.emit(RecordBatch)", () => {
  it("passes RecordBatch through unchanged", () => {
    const out = new OutputCollector(textSchema);
    // Build a batch the manual way
    const inner = new OutputCollector(textSchema);
    inner.emit({ name: ["x"], value: [1.0] });
    const batch = inner.batches[0].batch;

    out.emit(batch);
    expect(out.batches).toHaveLength(1);
    expect(out.batches[0].batch).toBe(batch);
  });
});

describe("OutputCollector.finish", () => {
  it("sets finished flag in producer mode", () => {
    const out = new OutputCollector(textSchema, true);
    expect(out.finished).toBe(false);
    out.finish();
    expect(out.finished).toBe(true);
  });

  it("throws in exchange mode", () => {
    const out = new OutputCollector(textSchema, false);
    expect(() => out.finish()).toThrow("finish() is not allowed on exchange streams");
  });
});

describe("buildResultBatch validation", () => {
  it("throws on missing required field", () => {
    const schema = new Schema([
      new Field("name", new Utf8(), false),
      new Field("value", new Float64(), false),
    ]);
    expect(() => buildResultBatch(schema, { name: "alice" }, "srv", null)).toThrow(
      /missing required field 'value'/,
    );
  });

  it("includes got keys in error message", () => {
    const schema = new Schema([
      new Field("name", new Utf8(), false),
      new Field("value", new Float64(), false),
    ]);
    expect(() => buildResultBatch(schema, { naem: "typo" }, "srv", null)).toThrow(
      /Got keys: \[naem\]/,
    );
  });

  it("allows extra keys silently", () => {
    const schema = new Schema([new Field("name", new Utf8(), false)]);
    const batch = buildResultBatch(schema, { name: "alice", extra: "ignored" }, "srv", null);
    expect(batch.numRows).toBe(1);
  });

  it("allows undefined for nullable fields", () => {
    const schema = new Schema([
      new Field("name", new Utf8(), false),
      new Field("value", new Float64(), true),
    ]);
    const batch = buildResultBatch(schema, { name: "alice" }, "srv", null);
    expect(batch.numRows).toBe(1);
  });
});
