// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import { Readable } from "node:stream";
import { makeData, RecordBatch, Schema, Struct } from "@query-farm/apache-arrow";
import * as native from "@query-farm/flechette";
import * as arrowjs from "../src/arrow/impl-arrowjs/index.js";
import * as flechette from "../src/arrow/impl-flechette/index.js";
import { backend, serializeBatch } from "../src/arrow/index.js";
import { typeToken } from "../src/type-tokens.js";
import { IpcStreamReader } from "../src/wire/reader.js";
import { validateRequestSchema } from "../src/wire/request.js";

describe("request type contracts across Arrow backends", () => {
  const types = [
    native.int64(),
    native.uint64(),
    native.utf8(),
    native.float64(),
    native.fixedSizeBinary(4),
    native.fixedSizeBinary(8),
    native.fixedSizeList(native.field("item", native.int32(), true), 2),
    native.fixedSizeList(native.field("element", native.int32(), true), 3),
    native.timestamp(native.TimeUnit.MICROSECOND, "UTC"),
    native.timestamp(native.TimeUnit.MICROSECOND),
    native.timestamp(native.TimeUnit.NANOSECOND, "UTC"),
    native.decimal(18, 4),
    native.decimal(18, 5),
    native.interval(native.IntervalUnit.YEAR_MONTH),
    native.interval(native.IntervalUnit.DAY_TIME),
    native.interval(native.IntervalUnit.MONTH_DAY_NANO),
    native.dictionary(native.utf8(), native.int16(), false),
    native.dictionary(native.utf8(), native.int16(), true),
    native.struct([native.field("x", native.int32(), true)]),
    native.struct([native.field("x", native.int32(), false)]),
    native.struct([native.field("y", native.int32(), true)]),
  ];

  test("equivalent types agree, while sign, size, units, order and child contracts remain distinct", () => {
    const schemas = types.map((type) => flechette.schema([flechette.field("value", type)]));
    const decoded = schemas.map((schema) => arrowjs.deserializeSchema(flechette.serializeSchema(schema)));
    for (let i = 0; i < schemas.length; i++) {
      expect(typeToken(types[i]!)).toBe(typeToken(decoded[i]!.fields[0]!.type));
      for (let j = 0; j < schemas.length; j++) {
        for (const actual of [schemas[j]!, decoded[j]!]) {
          const check = () => validateRequestSchema(actual, schemas[i]!, "echo");
          if (i === j) expect(check).not.toThrow();
          else expect(check).toThrow("ProtocolError");
        }
      }
    }
  });

  test("fixed-size list normalization preserves the native stride", () => {
    const type = native.fixedSizeList(native.int32(), 2);
    const schema = flechette.schema([flechette.field("value", type)]);
    const batch = flechette.batchFromColumns(schema, {
      value: [
        [1, 2],
        [3, 4],
      ],
    });
    const decoded = flechette.deserializeBatch(flechette.serializeBatch(batch));
    expect(Array.from(decoded.getChildAt(0)!, (value) => Array.from(value as Int32Array))).toEqual([
      [1, 2],
      [3, 4],
    ]);
  });

  test("large-list offsets remain distinct from list offsets", () => {
    const item = native.field("item", native.int32(), true);
    const expected = flechette.schema([flechette.field("value", native.largeList(item))]);
    const actual = flechette.deserializeSchema(flechette.serializeSchema(expected));
    expect(typeToken(actual.fields[0]!.type)).toBe("large_list<item?:int32>");
    expect(() => validateRequestSchema(actual, expected, "echo")).not.toThrow();
    const small = flechette.schema([flechette.field("value", native.list(item))]);
    expect(() => validateRequestSchema(small, expected, "echo")).toThrow("ProtocolError");
  });
});

describe("stream reader backend boundary", () => {
  const schema = flechette.schema(
    [
      flechette.field("at", flechette.timestampMicro("UTC")),
      flechette.field("amount", flechette.decimal(18, 4)),
      flechette.field("category", flechette.dictionary(flechette.int16(), flechette.utf8())),
      flechette.field("items", flechette.list(flechette.field("item", flechette.int64()))),
    ],
    new Map([["schema-key", "schema-value"]]),
  );
  const columns = [
    [1700000000123456n, null, -1234567n],
    [123456789n, null, -123456789n],
    ["first", null, "second"],
    [[1n, null, 3n], null, []],
  ];
  const batch = flechette.withBatchMetadata(
    flechette.batchFromColumns(schema, Object.fromEntries(schema.fields.map((field, i) => [field.name, columns[i]!]))),
    new Map([["tick", "one"]]),
  );
  const bytes = flechette.serializeBatch(batch);

  for (const mode of ["whole", "incremental"] as const) {
    test(`${mode}: active backend preserves raw values, metadata, and stream boundaries`, async () => {
      const reader = await IpcStreamReader.create(Readable.from([bytes, bytes]), { useActiveBackend: true });
      try {
        for (let i = 0; i < 2; i++) {
          const stream = mode === "whole" ? await reader.readStream() : null;
          const actualSchema = stream?.schema ?? (await reader.openNextStream());
          const actual = stream?.batches[0] ?? (await reader.readNextBatch());
          expect(actualSchema!.metadata.get("schema-key")).toBe("schema-value");
          expect(actual!.metadata?.get("tick")).toBe("one");
          expect("children" in actual!).toBe(backend.name === "flechette");
          const decoded = flechette.deserializeBatch(serializeBatch(actual!));
          expect(schema.fields.map((_, index) => Array.from(decoded.getChildAt(index)!))).toEqual(columns);
          if (mode === "incremental") {
            expect(await reader.readNextBatch()).toBeNull();
            expect(await reader.readNextBatch()).toBeNull();
          }
        }
        expect(await reader.openNextStream()).toBeNull();
      } finally {
        await reader.cancel();
      }
    });
  }

  test("the client default still returns Arrow JS batches", async () => {
    const reader = await IpcStreamReader.create(Readable.from([bytes]));
    try {
      const stream = await reader.readStream();
      expect(stream!.batches[0]).toBeInstanceOf(RecordBatch);
      expect(stream!.batches[0]!.metadata?.get("tick")).toBe("one");
    } finally {
      await reader.cancel();
    }
  });

  test("zero-column input batches retain their row count", async () => {
    const emptySchema = new Schema([]);
    const batch = new RecordBatch(
      emptySchema,
      makeData({ type: new Struct([]), length: 3 }),
      new Map([["rows", "three"]]),
    );
    const reader = await IpcStreamReader.create(Readable.from([arrowjs.serializeBatch(batch)]), {
      useActiveBackend: true,
    });
    try {
      const stream = await reader.readStream();
      expect(stream!.batches[0]!.numRows).toBe(3);
      expect(stream!.batches[0]!.metadata?.get("rows")).toBe("three");
    } finally {
      await reader.cancel();
    }
  });
});
