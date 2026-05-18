// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Conformance protocol — 48-method reference RPC service exercising all framework
 * capabilities. Used by the Python CLI to verify wire-protocol compatibility.
 *
 * This module exports the Protocol instance so it can be reused by both the
 * stdio server (conformance.ts) and the HTTP conformance tests.
 */
// arrow-js helpers still used inside the rich-header data builders and the
// embedded-arrow / IPC-roundtrip handlers. Type *classes* used to live
// here too; the conformance protocol now constructs schemas/types via the
// vgi-rpc Arrow facade (imports below) so it can run on either backend.
import {
  Field as A_Field,
  Float64 as A_Float64,
  Int16 as A_Int16,
  Int32 as A_Int32,
  Int64 as A_Int64,
  List as A_List,
  Struct as A_Struct,
  Utf8 as A_Utf8,
  type Data,
  Map_,
  makeData,
  type RecordBatch,
  RecordBatchReader,
  RecordBatchStreamWriter,
  recordBatchFromArrays,
  vectorFromArray,
} from "@query-farm/apache-arrow";
import {
  binary,
  bool as boolType,
  dateDay,
  decimal,
  dictionary,
  durationMicro,
  field,
  fixedSizeBinary,
  float32 as float32Type,
  float64,
  int8 as int8Type,
  int16 as int16Type,
  int32 as int32Type,
  int64,
  largeBinary,
  largeUtf8,
  list,
  map as mapType,
  schema,
  struct,
  timeMicro,
  timestampMicro,
  utf8,
  type VgiBatch,
  type VgiField,
  type VgiSchema,
} from "../src/arrow/index.js";
import { Protocol } from "../src/index.js";
import {
  bool,
  bytes,
  float,
  float32,
  int,
  int8,
  int16,
  int32,
  str,
  uint8,
  uint16,
  uint32,
  uint64,
} from "../src/schema.js";

// ---------------------------------------------------------------------------
// Error classes
// ---------------------------------------------------------------------------

class ValueError extends Error {
  constructor(msg: string) {
    super(msg);
    this.name = "ValueError";
  }
}

class RuntimeError extends Error {
  constructor(msg: string) {
    super(msg);
    this.name = "RuntimeError";
  }
}

// Map types are constructed via the Arrow facade's `mapType(keyField,
// valueField)` (imported above). Both backends build the entries
// Struct child correctly — no per-backend patching needed.

// ---------------------------------------------------------------------------
// Shared schemas
// ---------------------------------------------------------------------------

const COUNTER_SCHEMA = schema([field("index", int64(), false), field("value", int64(), false)]);

const HEADER_SCHEMA = schema([field("total_expected", int64(), false), field("description", utf8(), false)]);

const SCALE_INPUT = schema([field("value", float64(), false)]);
const SCALE_OUTPUT = schema([field("value", float64(), false)]);

const ACCUM_INPUT = schema([field("value", float64(), false)]);
const ACCUM_OUTPUT = schema([field("running_sum", float64(), false), field("exchange_count", int64(), false)]);

// ---------------------------------------------------------------------------
// RichHeader schema — 18 fields matching Python's RichHeader dataclass
// ---------------------------------------------------------------------------

const POINT_FIELDS = [field("x", float64(), false), field("y", float64(), false)];
const POINT_STRUCT = struct(POINT_FIELDS);

const STATUS_CYCLE = ["PENDING", "ACTIVE", "CLOSED"];

const richMapStrInt = mapType(field("key", utf8(), false), field("value", int64(), false));

const richMapStrStr = mapType(field("key", utf8(), false), field("value", utf8(), false));

const RICH_HEADER_SCHEMA = schema([
  field("str_field", utf8(), false),
  field("bytes_field", binary(), false),
  field("int_field", int64(), false),
  field("float_field", float64(), false),
  field("bool_field", boolType(), false),
  field("list_of_int", list(field("item", int64(), false)), false),
  field("list_of_str", list(field("item", utf8(), false)), false),
  field("dict_field", richMapStrInt, false),
  field("enum_field", dictionary(int16Type(), utf8()), false),
  field("nested_point", POINT_STRUCT, false),
  field("optional_str", utf8(), true),
  field("optional_int", int64(), true),
  field("optional_nested", POINT_STRUCT, true),
  field("list_of_nested", list(field("item", POINT_STRUCT, false)), false),
  field("nested_list", list(field("item", list(field("item", int64(), false)), false)), false),
  field("annotated_int32", int32Type(), false),
  field("annotated_float32", float32Type(), false),
  field("dict_str_str", richMapStrStr, false),
]);

// ---------------------------------------------------------------------------
// Data builders for complex types in RichHeader
// ---------------------------------------------------------------------------

function buildStructPointData(x: number, y: number): Data {
  const xData = vectorFromArray([x], float64()).data[0];
  const yData = vectorFromArray([y], float64()).data[0];
  return makeData({
    type: POINT_STRUCT,
    length: 1,
    children: [xData, yData],
    nullCount: 0,
  });
}

function buildNullStructPointData(): Data {
  // PyArrow requires valid-sized child buffers even for null struct entries.
  // Use vectorFromArray to build proper Float64 children with valid buffers.
  const xData = vectorFromArray([0], float64()).data[0];
  const yData = vectorFromArray([0], float64()).data[0];
  return makeData({
    type: POINT_STRUCT,
    length: 1,
    children: [xData, yData],
    nullCount: 1,
    nullBitmap: new Uint8Array([0]),
  });
}

function buildListOfPointsData(points: { x: number; y: number }[]): Data {
  const offsets = new Int32Array([0, points.length]);
  const xData = vectorFromArray(
    points.map((p) => p.x),
    float64(),
  ).data[0];
  const yData = vectorFromArray(
    points.map((p) => p.y),
    float64(),
  ).data[0];
  const structData = makeData({
    type: POINT_STRUCT,
    length: points.length,
    children: [xData, yData],
    nullCount: 0,
  });
  const listType = list(field("item", POINT_STRUCT, false));
  return makeData({
    type: listType,
    length: 1,
    valueOffsets: offsets,
    child: structData,
    nullCount: 0,
  } as any);
}

function buildMapDataFromEntries(keyField: Field, valueField: Field, keys: any[], values: any[]): Data {
  const offsets = new Int32Array([0, keys.length]);
  const keyData = vectorFromArray(keys, keyField.type).data[0];
  const valData = vectorFromArray(values, valueField.type).data[0];
  const entriesStruct = struct([keyField, valueField]);
  const entriesData = makeData({
    type: entriesStruct,
    length: keys.length,
    children: [keyData, valData],
    nullCount: 0,
  });
  const mapT = mapType(keyField, valueField);
  return makeData({
    type: mapT,
    length: 1,
    valueOffsets: offsets,
    child: entriesData,
    nullCount: 0,
  } as any);
}

// ---------------------------------------------------------------------------
// buildRichHeader — deterministic header values matching Python exactly
// ---------------------------------------------------------------------------

function buildRichHeader(seed: number): Record<string, any> {
  const s = BigInt(seed);
  // Backend-agnostic header values: plain JS Maps, objects, and arrays
  // rather than arrow-js `Data` blobs from `makeData` / `vectorFromArray`.
  // Both arrow-js and flechette column builders accept the JS-native
  // shapes — Map → Map column, plain object → Struct column,
  // Array<object> → List<Struct> column.
  return {
    str_field: `seed-${seed}`,
    bytes_field: new Uint8Array([seed % 256, (seed + 1) % 256, (seed + 2) % 256]),
    int_field: seed * 7,
    float_field: seed * 1.5,
    bool_field: seed % 2 === 0,
    list_of_int: [s, s + 1n, s + 2n],
    list_of_str: [`item-${seed}`, `item-${seed + 1}`],
    dict_field: new Map<string, bigint>([
      ["a", s],
      ["b", s + 1n],
    ]),
    enum_field: STATUS_CYCLE[seed % 3],
    nested_point: { x: seed, y: seed * 2 },
    optional_str: seed % 2 === 0 ? `opt-${seed}` : null,
    optional_int: seed % 2 === 1 ? seed * 3 : null,
    optional_nested: seed % 3 === 0 ? { x: seed, y: 0 } : null,
    list_of_nested: [{ x: seed, y: seed + 1 }],
    nested_list: [[s, s + 1n], [s + 2n]],
    annotated_int32: seed % 1000,
    annotated_float32: seed / 3.0,
    dict_str_str: new Map<string, string>([["key", `val-${seed}`]]),
  };
}

// ---------------------------------------------------------------------------
// buildDynamicSchema — dynamic output schema for produce_dynamic_schema
// ---------------------------------------------------------------------------

function buildDynamicSchema(includeStrings: boolean, includeFloats: boolean): Schema {
  // Python pa.field() defaults to nullable=True, so we match that here.
  const fields: Field[] = [field("index", int64(), true)];
  if (includeStrings) fields.push(field("label", utf8(), true));
  if (includeFloats) fields.push(field("score", float64(), true));
  return schema(fields);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Format float matching Python repr: integers get .0 suffix. */
function formatFloat(n: number): string {
  if (Number.isFinite(n) && Number.isInteger(n)) return n.toFixed(1);
  return String(n);
}

/** Serialize an IPC stream with a single batch to bytes (for dataclass binary). */
function _serializeBatch(schema: Schema, batch: RecordBatch): Uint8Array {
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, schema);
  writer.write(batch);
  writer.close();
  return writer.toUint8Array(true);
}

// ---------------------------------------------------------------------------
// Protocol
// ---------------------------------------------------------------------------

export const protocol = new Protocol("Conformance");

// ===== Scalar Echo (5) =====

protocol.unary("echo_string", {
  params: { value: str },
  result: { result: str },
  handler: (p) => ({ result: p.value }),
});

const largeStringType = largeUtf8();
protocol.unary("echo_large_string", {
  params: schema([field("value", largeStringType, false)]),
  result: schema([field("result", largeStringType, false)]),
  handler: (p) => ({ result: p.value }),
});

protocol.unary("echo_bytes", {
  params: { data: bytes },
  result: { result: bytes },
  handler: (p) => ({ result: p.data }),
});

protocol.unary("oversized_unary", {
  params: { target_bytes: int },
  result: { result: bytes },
  handler: (p) => {
    const n = typeof p.target_bytes === "bigint" ? Number(p.target_bytes) : Number(p.target_bytes);
    if (n < 0) throw new ValueError("target_bytes must be non-negative");
    return { result: new Uint8Array(n) };
  },
  paramTypes: { target_bytes: "int" },
});

protocol.unary("echo_int", {
  params: { value: int },
  result: { result: int },
  handler: (p) => ({ result: p.value }),
});

protocol.unary("echo_float", {
  params: { value: float },
  result: { result: float },
  handler: (p) => ({ result: p.value }),
});

protocol.unary("echo_bool", {
  params: { value: bool },
  result: { result: bool },
  handler: (p) => ({ result: p.value }),
});

// ===== Void Returns (2) =====

protocol.unary("void_noop", {
  params: {},
  result: {},
  handler: () => ({}),
});

protocol.unary("void_with_param", {
  params: { value: int },
  result: {},
  handler: () => ({}),
});

// ===== Complex Type Echo (4) =====

const dictType = dictionary(int16Type(), utf8());

protocol.unary("echo_enum", {
  params: schema([field("status", dictType, false)]),
  result: schema([field("result", dictType, false)]),
  handler: (p) => ({ result: p.status }),
});

const listUtf8 = list(field("item", utf8(), false));

protocol.unary("echo_list", {
  params: schema([field("values", listUtf8, false)]),
  result: schema([field("result", listUtf8, false)]),
  handler: (p) => {
    // Backend-agnostic extraction: arrow-js returns a Vector with .get(i);
    // flechette returns a plain JS array. Both are iterable.
    const vec = p.values;
    const arr: string[] = Array.from(vec, (v: any) => (v?.get ? v : v));
    if (vec && typeof vec.get === "function" && typeof vec.length === "number") {
      arr.length = 0;
      for (let i = 0; i < vec.length; i++) arr.push(vec.get(i));
    }
    return { result: arr };
  },
});

const mapStrInt = mapType(field("key", utf8(), false), field("value", int64(), false));

protocol.unary("echo_dict", {
  params: schema([field("mapping", mapStrInt, false)]),
  result: schema([field("result", mapStrInt, false)]),
  // Map_ values are raw Data objects (passthrough) due to arrow-js bug
  handler: (p) => ({ result: p.mapping }),
});

const nestedList = list(field("item", list(field("item", int64(), false)), false));

protocol.unary("echo_nested_list", {
  params: schema([field("matrix", nestedList, false)]),
  result: schema([field("result", nestedList, false)]),
  handler: (p) => {
    // Backend-agnostic extraction: arrow-js returns nested Vectors with
    // `.get(i)`, flechette returns nested plain JS arrays. Materialise
    // either shape into Array<Array<bigint>>.
    const toArray = (v: any): any[] =>
      v && typeof v.get === "function" && typeof v.length === "number"
        ? Array.from({ length: v.length }, (_, k) => v.get(k))
        : Array.from(v ?? []);
    const rows: bigint[][] = toArray(p.matrix).map((inner: any) => toArray(inner));
    return { result: rows };
  },
});

// ===== Optional/Nullable (2) =====

protocol.unary("echo_optional_string", {
  params: schema([field("value", utf8(), true)]),
  result: schema([field("result", utf8(), true)]),
  handler: (p) => ({ result: p.value }),
});

protocol.unary("echo_optional_int", {
  params: schema([field("value", int64(), true)]),
  result: schema([field("result", int64(), true)]),
  handler: (p) => ({ result: p.value }),
});

// ===== Dataclass Round-trip (4) =====
// Dataclasses are serialized as binary blobs (IPC streams)

protocol.unary("echo_point", {
  params: { point: bytes },
  result: { result: bytes },
  handler: (p) => ({ result: p.point }),
  paramTypes: { point: "Point" },
});

protocol.unary("echo_all_types", {
  params: { data: bytes },
  result: { result: bytes },
  handler: (p) => ({ result: p.data }),
  paramTypes: { data: "AllTypes" },
});

protocol.unary("echo_bounding_box", {
  params: { box: bytes },
  result: { result: bytes },
  handler: (p) => ({ result: p.box }),
  paramTypes: { box: "BoundingBox" },
});

protocol.unary("inspect_point", {
  params: { point: bytes },
  result: { result: str },
  handler: async (p) => {
    // Deserialize the Point IPC binary blob
    const bytes = p.point as Uint8Array;
    const reader = await RecordBatchReader.from(bytes);
    const batches = reader.readAll();
    const batch = batches[0];
    // Point has fields: x (float64), y (float64)
    const x = batch.getChildAt(0)?.get(0) as number;
    const y = batch.getChildAt(1)?.get(0) as number;
    return { result: `Point(${formatFloat(x)}, ${formatFloat(y)})` };
  },
  paramTypes: { point: "Point" },
});

// ===== Annotated Types (2) =====

protocol.unary("echo_int32", {
  params: { value: int32 },
  result: { result: int32 },
  handler: (p) => ({ result: p.value }),
});

protocol.unary("echo_float32", {
  params: { value: float32 },
  result: { result: float32 },
  handler: (p) => ({ result: p.value }),
});

// ===== Wide Integer Types (6) =====

protocol.unary("echo_int8", {
  params: { value: int8 },
  result: { result: int8 },
  handler: (p) => ({ result: p.value }),
});

protocol.unary("echo_int16", {
  params: { value: int16 },
  result: { result: int16 },
  handler: (p) => ({ result: p.value }),
});

protocol.unary("echo_uint8", {
  params: { value: uint8 },
  result: { result: uint8 },
  handler: (p) => ({ result: p.value }),
});

protocol.unary("echo_uint16", {
  params: { value: uint16 },
  result: { result: uint16 },
  handler: (p) => ({ result: p.value }),
});

protocol.unary("echo_uint32", {
  params: { value: uint32 },
  result: { result: uint32 },
  handler: (p) => ({ result: p.value }),
});

protocol.unary("echo_uint64", {
  params: { value: uint64 },
  result: { result: uint64 },
  handler: (p) => ({ result: p.value }),
});

// ===== Wide Arrow Types (date/time/decimal/binary/dictionary) =====
// These use opaque-Data passthrough — the framework round-trips the underlying
// Arrow column without converting through JS values (avoids arrow-js conversion
// quirks for Date/Timestamp/Decimal/etc).

const dateType = dateDay();
protocol.unary("echo_date", {
  params: schema([field("value", dateType, false)]),
  result: schema([field("result", dateType, false)]),
  handler: (p) => ({ result: p.value }),
});

const timestampType = timestampMicro();
protocol.unary("echo_timestamp", {
  params: schema([field("value", timestampType, false)]),
  result: schema([field("result", timestampType, false)]),
  handler: (p) => ({ result: p.value }),
});

const timestampUtcType = timestampMicro("UTC");
protocol.unary("echo_timestamp_utc", {
  params: schema([field("value", timestampUtcType, false)]),
  result: schema([field("result", timestampUtcType, false)]),
  handler: (p) => ({ result: p.value }),
});

const timeType = timeMicro();
protocol.unary("echo_time", {
  params: schema([field("value", timeType, false)]),
  result: schema([field("result", timeType, false)]),
  handler: (p) => ({ result: p.value }),
});

const durationType = durationMicro();
protocol.unary("echo_duration", {
  params: schema([field("value", durationType, false)]),
  result: schema([field("result", durationType, false)]),
  handler: (p) => ({ result: p.value }),
});

const decimalType = decimal(20, 4, 128);
protocol.unary("echo_decimal", {
  params: schema([field("value", decimalType, false)]),
  result: schema([field("result", decimalType, false)]),
  handler: (p) => ({ result: p.value }),
});

const largeBinaryType = largeBinary();
protocol.unary("echo_large_binary", {
  params: schema([field("value", largeBinaryType, false)]),
  result: schema([field("result", largeBinaryType, false)]),
  handler: (p) => ({ result: p.value }),
});

const fixedBinaryType = fixedSizeBinary(8);
protocol.unary("echo_fixed_binary", {
  params: schema([field("value", fixedBinaryType, false)]),
  result: schema([field("result", fixedBinaryType, false)]),
  handler: (p) => ({ result: p.value }),
});

const dictStringType = dictionary(int16Type(), utf8());
protocol.unary("echo_dict_encoded_string", {
  params: schema([field("value", dictStringType, false)]),
  result: schema([field("result", dictStringType, false)]),
  handler: (p) => ({ result: p.value }),
});

// Dataclass round-trips — opaque IPC blob (bytes)
protocol.unary("echo_wide_types", {
  params: { data: bytes },
  result: { result: bytes },
  handler: (p) => ({ result: p.data }),
  paramTypes: { data: "WideTypes" },
});

protocol.unary("echo_container_wide_types", {
  params: { data: bytes },
  result: { result: bytes },
  handler: (p) => ({ result: p.data }),
  paramTypes: { data: "ContainerWideTypes" },
});

protocol.unary("echo_deep_nested", {
  params: { data: bytes },
  result: { result: bytes },
  handler: (p) => ({ result: p.data }),
  paramTypes: { data: "DeepNested" },
});

protocol.unary("echo_embedded_arrow", {
  params: { data: bytes },
  result: { result: bytes },
  handler: (p) => ({ result: p.data }),
  paramTypes: { data: "EmbeddedArrow" },
});

// ===== Multi-Param & Defaults (3) =====

protocol.unary("add_floats", {
  params: { a: float, b: float },
  result: { result: float },
  handler: (p) => ({ result: p.a + p.b }),
});

protocol.unary("concatenate", {
  params: { prefix: str, suffix: str, separator: str },
  result: { result: str },
  handler: (p) => ({
    result: `${p.prefix}${p.separator}${p.suffix}`,
  }),
  defaults: { separator: "-" },
});

protocol.unary("with_defaults", {
  params: { required: int, optional_str: str, optional_int: int },
  result: { result: str },
  handler: (p) => ({
    result: `required=${p.required}, optional_str=${p.optional_str}, optional_int=${p.optional_int}`,
  }),
  defaults: { optional_str: "default", optional_int: 42 },
});

// ===== Error Propagation (3) =====

protocol.unary("raise_value_error", {
  params: { message: str },
  result: { result: str },
  handler: (p) => {
    throw new ValueError(p.message);
  },
});

protocol.unary("raise_runtime_error", {
  params: { message: str },
  result: { result: str },
  handler: (p) => {
    throw new RuntimeError(p.message);
  },
});

protocol.unary("raise_type_error", {
  params: { message: str },
  result: { result: str },
  handler: (p) => {
    throw new TypeError(p.message);
  },
});

// ===== Client-Directed Logging (3) =====

protocol.unary("echo_with_info_log", {
  params: { value: str },
  result: { result: str },
  handler: (p, ctx) => {
    ctx.clientLog("INFO", `info: ${p.value}`);
    return { result: p.value };
  },
});

protocol.unary("echo_with_multi_logs", {
  params: { value: str },
  result: { result: str },
  handler: (p, ctx) => {
    ctx.clientLog("DEBUG", `debug: ${p.value}`);
    ctx.clientLog("INFO", `info: ${p.value}`);
    ctx.clientLog("WARN", `warn: ${p.value}`);
    return { result: p.value };
  },
});

protocol.unary("echo_with_log_extras", {
  params: { value: str },
  result: { result: str },
  handler: (p, ctx) => {
    ctx.clientLog("INFO", `info: ${p.value}`, {
      source: "conformance",
      detail: p.value,
    });
    return { result: p.value };
  },
});

protocol.unary("echo_with_all_log_levels", {
  params: { value: str },
  result: { result: str },
  handler: (p, ctx) => {
    ctx.clientLog("TRACE", `trace: ${p.value}`);
    ctx.clientLog("DEBUG", `debug: ${p.value}`);
    ctx.clientLog("INFO", `info: ${p.value}`);
    ctx.clientLog("WARN", `warn: ${p.value}`);
    ctx.clientLog("ERROR", `error: ${p.value}`);
    return { result: p.value };
  },
});

// ===== Producer Streams (7) =====

protocol.producer<{ count: number; current: number }>("produce_n", {
  params: { count: int },
  outputSchema: COUNTER_SCHEMA,
  init: ({ count }) => ({ count, current: 0 }),
  produce: (state, out) => {
    if (state.current >= state.count) {
      out.finish();
      return;
    }
    out.emitRow({ index: state.current, value: state.current * 10 });
    state.current++;
  },
  paramTypes: { count: "int" },
});

protocol.producer<Record<string, never>>("produce_empty", {
  params: {},
  outputSchema: COUNTER_SCHEMA,
  init: () => ({}),
  produce: (_state, out) => {
    out.finish();
  },
});

protocol.producer<{ emitted: boolean }>("produce_single", {
  params: {},
  outputSchema: COUNTER_SCHEMA,
  init: () => ({ emitted: false }),
  produce: (state, out) => {
    if (state.emitted) {
      out.finish();
      return;
    }
    state.emitted = true;
    out.emitRow({ index: 0, value: 0 });
  },
});

protocol.producer<{ rowsPerBatch: number; batchCount: number; current: number }>("produce_large_batches", {
  params: { rows_per_batch: int, batch_count: int },
  outputSchema: COUNTER_SCHEMA,
  init: ({ rows_per_batch, batch_count }) => ({
    rowsPerBatch: rows_per_batch,
    batchCount: batch_count,
    current: 0,
  }),
  produce: (state, out) => {
    if (state.current >= state.batchCount) {
      out.finish();
      return;
    }
    const offset = state.current * state.rowsPerBatch;
    const indices: number[] = [];
    const values: number[] = [];
    for (let i = 0; i < state.rowsPerBatch; i++) {
      indices.push(offset + i);
      values.push((offset + i) * 10);
    }
    out.emit({ index: indices, value: values });
    state.current++;
  },
  paramTypes: { rows_per_batch: "int", batch_count: "int" },
});

protocol.producer<{ count: number; current: number }>("produce_with_logs", {
  params: { count: int },
  outputSchema: COUNTER_SCHEMA,
  init: ({ count }) => ({ count, current: 0 }),
  produce: (state, out) => {
    if (state.current >= state.count) {
      out.finish();
      return;
    }
    out.clientLog("INFO", `producing batch ${state.current}`);
    out.emitRow({ index: state.current, value: state.current * 10 });
    state.current++;
  },
  paramTypes: { count: "int" },
});

protocol.producer<{ emitBeforeError: number; current: number }>("produce_error_mid_stream", {
  params: { emit_before_error: int },
  outputSchema: COUNTER_SCHEMA,
  init: ({ emit_before_error }) => ({ emitBeforeError: emit_before_error, current: 0 }),
  produce: (state, out) => {
    if (state.current >= state.emitBeforeError) {
      throw new RuntimeError(`intentional error after ${state.emitBeforeError} batches`);
    }
    out.emitRow({ index: state.current, value: state.current * 10 });
    state.current++;
  },
  paramTypes: { emit_before_error: "int" },
});

protocol.producer<{ rowsPerBatch: number; emitted: boolean }>("produce_oversized_batch", {
  params: { rows_per_batch: int },
  outputSchema: COUNTER_SCHEMA,
  init: ({ rows_per_batch }) => ({ rowsPerBatch: rows_per_batch, emitted: false }),
  produce: (state, out) => {
    if (state.emitted) {
      out.finish();
      return;
    }
    const indices: number[] = [];
    const values: number[] = [];
    for (let i = 0; i < state.rowsPerBatch; i++) {
      indices.push(i);
      values.push(i * 10);
    }
    out.emit({ index: indices, value: values });
    state.emitted = true;
  },
  paramTypes: { rows_per_batch: "int" },
});

protocol.exchange<{ rowsPerBatch: number }>("exchange_oversized", {
  params: { rows_per_batch: int },
  inputSchema: SCALE_INPUT,
  outputSchema: COUNTER_SCHEMA,
  init: ({ rows_per_batch }) => ({ rowsPerBatch: rows_per_batch }),
  exchange: (state, _input: RecordBatch, out) => {
    const indices: number[] = [];
    const values: number[] = [];
    for (let i = 0; i < state.rowsPerBatch; i++) {
      indices.push(i);
      values.push(i * 10);
    }
    out.emit({ index: indices, value: values });
  },
  paramTypes: { rows_per_batch: "int" },
});

protocol.producer<never>("produce_error_on_init", {
  params: {},
  outputSchema: COUNTER_SCHEMA,
  init: () => {
    throw new RuntimeError("intentional init error");
  },
  produce: (_state, _out) => {
    // never reached
  },
});

// ===== Producer Streams With Headers (2) =====

protocol.producer<{ count: number; current: number }>("produce_with_header", {
  params: { count: int },
  outputSchema: COUNTER_SCHEMA,
  headerSchema: HEADER_SCHEMA,
  headerInit: (params) => ({
    total_expected: params.count,
    description: `producing ${params.count} batches`,
  }),
  init: ({ count }) => ({ count, current: 0 }),
  produce: (state, out) => {
    if (state.current >= state.count) {
      out.finish();
      return;
    }
    out.emitRow({ index: state.current, value: state.current * 10 });
    state.current++;
  },
  paramTypes: { count: "int" },
});

protocol.producer<{ count: number; current: number }>("produce_with_header_and_logs", {
  params: { count: int },
  outputSchema: COUNTER_SCHEMA,
  headerSchema: HEADER_SCHEMA,
  headerInit: (params, _state, ctx) => {
    ctx.clientLog("INFO", "stream init log");
    return {
      total_expected: params.count,
      description: `producing ${params.count} with logs`,
    };
  },
  init: ({ count }) => ({ count, current: 0 }),
  produce: (state, out) => {
    if (state.current >= state.count) {
      out.finish();
      return;
    }
    out.emitRow({ index: state.current, value: state.current * 10 });
    state.current++;
  },
  paramTypes: { count: "int" },
});

// ===== Exchange Streams (5) =====

protocol.exchange<{ factor: number }>("exchange_scale", {
  params: { factor: float },
  inputSchema: SCALE_INPUT,
  outputSchema: SCALE_OUTPUT,
  init: ({ factor }) => ({ factor }),
  exchange: (state, input: RecordBatch, out) => {
    const col = input.getChildAt(0)!;
    const values: number[] = [];
    for (let i = 0; i < input.numRows; i++) values.push(col.get(i) * state.factor);
    out.emit({ value: values });
  },
});

protocol.exchange<{ runningSum: number; exchangeCount: number }>("exchange_accumulate", {
  params: {},
  inputSchema: ACCUM_INPUT,
  outputSchema: ACCUM_OUTPUT,
  init: () => ({ runningSum: 0, exchangeCount: 0 }),
  exchange: (state, input: RecordBatch, out) => {
    const col = input.getChildAt(0)!;
    let sum = 0;
    for (let i = 0; i < input.numRows; i++) sum += col.get(i) as number;
    state.runningSum += sum;
    state.exchangeCount++;
    out.emitRow({ running_sum: state.runningSum, exchange_count: state.exchangeCount });
  },
});

protocol.exchange<Record<string, never>>("exchange_with_logs", {
  params: {},
  inputSchema: SCALE_INPUT,
  outputSchema: SCALE_OUTPUT,
  init: () => ({}),
  exchange: (_state, input: RecordBatch, out) => {
    out.clientLog("INFO", "exchange processing");
    out.clientLog("DEBUG", "exchange debug");
    out.emit(input);
  },
});

protocol.exchange<{ failOn: number; exchangeCount: number }>("exchange_error_on_nth", {
  params: { fail_on: int },
  inputSchema: SCALE_INPUT,
  outputSchema: SCALE_OUTPUT,
  init: ({ fail_on }) => ({ failOn: fail_on, exchangeCount: 0 }),
  exchange: (state, input: RecordBatch, out) => {
    state.exchangeCount++;
    if (state.exchangeCount >= state.failOn) {
      throw new RuntimeError(`intentional error on exchange ${state.exchangeCount}`);
    }
    out.emit(input);
  },
  paramTypes: { fail_on: "int" },
});

const EMPTY_EXCHANGE_SCHEMA = schema([]);

protocol.exchange<{ callCount: number }>("exchange_zero_columns", {
  params: {},
  inputSchema: EMPTY_EXCHANGE_SCHEMA,
  outputSchema: EMPTY_EXCHANGE_SCHEMA,
  init: () => ({ callCount: 0 }),
  exchange: (state, _input: RecordBatch, out) => {
    state.callCount++;
    // Build a zero-column batch via the facade so the same code works
    // on arrow-js and flechette. The OutputCollector validates that
    // exactly one data batch is emitted per call — emit({}) is the
    // zero-column equivalent.
    out.emit({});
  },
});

protocol.exchange<{ factor: number }>("exchange_cast_compatible", {
  params: {},
  inputSchema: SCALE_INPUT,
  outputSchema: SCALE_OUTPUT,
  init: () => ({ factor: 1.0 }),
  exchange: (state, input: RecordBatch, out) => {
    // Cast-compatible exchange: the client may send int64 to a float64
    // input. arrow-js's conformBatchToSchema casts in-place; flechette's
    // facade leaves the batch untouched. Coerce BigInt → Number here
    // so the multiplication doesn't mix types.
    const col = input.getChildAt(0)!;
    const values: number[] = [];
    for (let i = 0; i < input.numRows; i++) {
      const raw = col.get(i);
      const num = typeof raw === "bigint" ? Number(raw) : Number(raw);
      values.push(num * state.factor);
    }
    out.emit({ value: values });
  },
});

protocol.exchange<never>("exchange_error_on_init", {
  params: {},
  inputSchema: SCALE_INPUT,
  outputSchema: SCALE_OUTPUT,
  init: () => {
    throw new RuntimeError("intentional exchange init error");
  },
  exchange: (_state, _input: RecordBatch, _out) => {
    // never reached
  },
});

// ===== Exchange Streams With Headers (1) =====

protocol.exchange<{ factor: number }>("exchange_with_header", {
  params: { factor: float },
  inputSchema: SCALE_INPUT,
  outputSchema: SCALE_OUTPUT,
  headerSchema: HEADER_SCHEMA,
  headerInit: (params) => ({
    total_expected: 0,
    description: `scale by ${formatFloat(params.factor)}`,
  }),
  init: ({ factor }) => ({ factor }),
  exchange: (state, input: RecordBatch, out) => {
    const col = input.getChildAt(0)!;
    const values: number[] = [];
    for (let i = 0; i < input.numRows; i++) values.push(col.get(i) * state.factor);
    out.emit({ value: values });
  },
});

// ===== Dynamic Streams With Rich Multi-Type Headers (3) =====

protocol.producer<{ count: number; current: number }>("produce_with_rich_header", {
  params: { seed: int, count: int },
  outputSchema: COUNTER_SCHEMA,
  headerSchema: RICH_HEADER_SCHEMA,
  headerInit: (params) => buildRichHeader(params.seed),
  init: ({ count }) => ({ count, current: 0 }),
  produce: (state, out) => {
    if (state.current >= state.count) {
      out.finish();
      return;
    }
    out.emitRow({ index: state.current, value: state.current * 10 });
    state.current++;
  },
  paramTypes: { seed: "int", count: "int" },
});

protocol.producer<{
  count: number;
  current: number;
  includeStrings: boolean;
  includeFloats: boolean;
  __outputSchema: Schema;
}>("produce_dynamic_schema", {
  params: { seed: int, count: int, include_strings: bool, include_floats: bool },
  outputSchema: COUNTER_SCHEMA, // default, overridden by __outputSchema
  headerSchema: RICH_HEADER_SCHEMA,
  headerInit: (params) => buildRichHeader(params.seed),
  init: (p) => ({
    count: p.count,
    current: 0,
    includeStrings: p.include_strings,
    includeFloats: p.include_floats,
    __outputSchema: buildDynamicSchema(p.include_strings, p.include_floats),
  }),
  produce: (state, out) => {
    if (state.current >= state.count) {
      out.finish();
      return;
    }
    const row: Record<string, any> = { index: state.current };
    if (state.includeStrings) row.label = `row-${state.current}`;
    if (state.includeFloats) row.score = state.current * 1.5;
    out.emitRow(row);
    state.current++;
  },
  paramTypes: { seed: "int", count: "int", include_strings: "bool", include_floats: "bool" },
});

protocol.exchange<{ factor: number }>("exchange_with_rich_header", {
  params: { seed: int, factor: float },
  inputSchema: SCALE_INPUT,
  outputSchema: SCALE_OUTPUT,
  headerSchema: RICH_HEADER_SCHEMA,
  headerInit: (params) => buildRichHeader(params.seed),
  init: ({ factor }) => ({ factor }),
  exchange: (state, input: RecordBatch, out) => {
    const col = input.getChildAt(0)!;
    const values: number[] = [];
    for (let i = 0; i < input.numRows; i++) values.push(col.get(i) * state.factor);
    out.emit({ value: values });
  },
});

// ===== Cancellation (4) =====
// Process-wide counters observed by the cancel conformance tests. They live in
// the server process, so the same counters are visible across pipe, subprocess,
// and HTTP transports (read back via cancel_probe_counters).
const cancelProbe = { produceCalls: 0, exchangeCalls: 0, onCancelCalls: 0 };

protocol.producer<{ current: number }>("cancellable_producer", {
  params: {},
  outputSchema: COUNTER_SCHEMA,
  init: () => ({ current: 0 }),
  produce: (state, out) => {
    cancelProbe.produceCalls++;
    out.emitRow({ index: state.current, value: state.current * 10 });
    state.current++;
  },
  onCancel: () => {
    cancelProbe.onCancelCalls++;
  },
});

protocol.exchange<Record<string, never>>("cancellable_exchange", {
  params: {},
  inputSchema: SCALE_INPUT,
  outputSchema: SCALE_OUTPUT,
  init: () => ({}),
  exchange: (_state, input: RecordBatch, out) => {
    cancelProbe.exchangeCalls++;
    out.emit(input);
  },
  onCancel: () => {
    cancelProbe.onCancelCalls++;
  },
});

protocol.unary("cancel_probe_counters", {
  params: {},
  result: schema([field("result", list(field("item", int64(), false)), false)]),
  handler: () => ({
    result: [BigInt(cancelProbe.produceCalls), BigInt(cancelProbe.exchangeCalls), BigInt(cancelProbe.onCancelCalls)],
  }),
});

protocol.unary("reset_cancel_probe", {
  params: {},
  result: {},
  handler: () => {
    cancelProbe.produceCalls = 0;
    cancelProbe.exchangeCalls = 0;
    cancelProbe.onCancelCalls = 0;
    return {};
  },
});

// ===== Sticky Sessions (3) =====
//
// `open_counter` / `increment_counter` / `close_counter` exercise the
// runtime API `ctx.openSession` / `ctx.session` / `ctx.closeSession`.
// Tests live in `vgi_rpc.conformance._pytest_suite::TestSticky` and run
// only against HTTP servers that advertise `VGI-Sticky-Enabled: true`.
class StickyCounter {
  value: number;
  closed = false;
  constructor(initial: number) {
    this.value = initial;
  }
  close(): void {
    this.closed = true;
  }
}

protocol.unary("open_counter", {
  params: { initial: int },
  result: { result: int },
  handler: (p, ctx) => {
    const cc = ctx as unknown as import("../src/types.js").CallContext;
    cc.openSession(new StickyCounter(Number(p.initial)));
    return { result: p.initial };
  },
});

protocol.unary("increment_counter", {
  params: { by: int },
  result: { result: int },
  handler: (p, ctx) => {
    const cc = ctx as unknown as import("../src/types.js").CallContext;
    const counter = cc.session;
    if (!(counter instanceof StickyCounter)) {
      throw new RuntimeError("no sticky counter bound to this request");
    }
    counter.value += Number(p.by);
    return { result: BigInt(counter.value) };
  },
});

protocol.unary("close_counter", {
  params: {},
  result: { result: int },
  handler: (_p, ctx) => {
    const cc = ctx as unknown as import("../src/types.js").CallContext;
    const counter = cc.session;
    if (!(counter instanceof StickyCounter)) {
      throw new RuntimeError("no sticky counter bound to this request");
    }
    const final = counter.value;
    cc.closeSession();
    return { result: BigInt(final) };
  },
});

// Streaming sticky-session methods — share the same `StickyCounter` bound
// by `open_counter`. The producer increments and emits; the exchange adds
// the input column sum and emits. Both use `ctx.session` per turn so the
// session must be re-resolved by the sticky middleware on every HTTP
// request. Mirrors `SessionCounterProducerState`/`SessionCounterExchangeState`
// in vgi_rpc.conformance.
const _SESSION_COUNTER_OUTPUT = schema([field("value", int64(), false)]);
const _SESSION_COUNTER_EXCHANGE_INPUT = schema([field("by", int64(), false)]);

protocol.producer<{ count: number; current: number }>("stream_session_counter", {
  params: { count: int },
  outputSchema: _SESSION_COUNTER_OUTPUT,
  init: (p) => ({ count: Number(p.count), current: 0 }),
  produce: (state, out) => {
    if (state.current >= state.count) {
      out.finish();
      return;
    }
    const cc = out as unknown as import("../src/types.js").CallContext;
    const counter = cc.session;
    if (!(counter instanceof StickyCounter)) {
      throw new RuntimeError("no sticky counter bound to this request");
    }
    counter.value += 1;
    out.emitRow({ value: BigInt(counter.value) });
    state.current += 1;
  },
});

protocol.exchange<Record<string, never>>("exchange_session_counter", {
  params: {},
  inputSchema: _SESSION_COUNTER_EXCHANGE_INPUT,
  outputSchema: _SESSION_COUNTER_OUTPUT,
  init: () => ({}),
  exchange: (_state, input: RecordBatch, out) => {
    const cc = out as unknown as import("../src/types.js").CallContext;
    const counter = cc.session;
    if (!(counter instanceof StickyCounter)) {
      throw new RuntimeError("no sticky counter bound to this request");
    }
    // Sum the "by" column across the input batch.
    const col = input.getChild("by");
    let total = 0n;
    if (col) {
      for (let i = 0; i < col.length; i++) {
        const v = col.get(i);
        total += typeof v === "bigint" ? v : BigInt(v ?? 0);
      }
    }
    counter.value += Number(total);
    out.emitRow({ value: BigInt(counter.value) });
  },
});
