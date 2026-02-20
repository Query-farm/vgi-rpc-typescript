/**
 * Conformance protocol — 43-method reference RPC service exercising all framework
 * capabilities. Used by the Python CLI to verify wire-protocol compatibility.
 *
 * This module exports the Protocol instance so it can be reused by both the
 * stdio server (conformance.ts) and the HTTP conformance tests.
 */
import {
  Schema,
  Field,
  Utf8,
  Binary,
  Int64,
  Int32,
  Int16,
  Float64,
  Float32,
  Bool,
  List,
  Map_,
  Dictionary,
  RecordBatch,
  RecordBatchStreamWriter,
  RecordBatchReader,
  recordBatchFromArrays,
  Struct,
} from "apache-arrow";
import { Protocol, type OutputCollector, type LogContext } from "../src/index.js";
import { str, bytes, int, float, float32, int32, bool } from "../src/schema.js";

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

// ---------------------------------------------------------------------------
// Map_ fix: arrow-js Map_ constructor creates broken children structure.
// We need to patch it to produce IPC-compatible schemas for PyArrow.
// ---------------------------------------------------------------------------

function makeMapType(keyField: Field, valueField: Field): Map_ {
  const m = new Map_(keyField, valueField);
  const entriesStruct = new Struct([keyField, valueField]);
  const entriesField = new Field("entries", entriesStruct, false);
  (m as any).children = [entriesField];
  return m;
}

// ---------------------------------------------------------------------------
// Shared schemas
// ---------------------------------------------------------------------------

const COUNTER_SCHEMA = new Schema([
  new Field("index", new Int64(), false),
  new Field("value", new Int64(), false),
]);

const HEADER_SCHEMA = new Schema([
  new Field("total_expected", new Int64(), false),
  new Field("description", new Utf8(), false),
]);

const SCALE_INPUT = new Schema([new Field("value", new Float64(), false)]);
const SCALE_OUTPUT = new Schema([new Field("value", new Float64(), false)]);

const ACCUM_INPUT = new Schema([new Field("value", new Float64(), false)]);
const ACCUM_OUTPUT = new Schema([
  new Field("running_sum", new Float64(), false),
  new Field("exchange_count", new Int64(), false),
]);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Format float matching Python repr: integers get .0 suffix. */
function formatFloat(n: number): string {
  if (Number.isFinite(n) && Number.isInteger(n)) return n.toFixed(1);
  return String(n);
}

/** Serialize an IPC stream with a single batch to bytes (for dataclass binary). */
function serializeBatch(schema: Schema, batch: RecordBatch): Uint8Array {
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

protocol.unary("echo_bytes", {
  params: { data: bytes },
  result: { result: bytes },
  handler: (p) => ({ result: p.data }),
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

const dictType = new Dictionary(new Utf8(), new Int16());

protocol.unary("echo_enum", {
  params: new Schema([new Field("status", dictType, false)]),
  result: new Schema([new Field("result", dictType, false)]),
  handler: (p) => ({ result: p.status }),
});

const listUtf8 = new List(new Field("item", new Utf8(), false));

protocol.unary("echo_list", {
  params: new Schema([new Field("values", listUtf8, false)]),
  result: new Schema([new Field("result", listUtf8, false)]),
  handler: (p) => {
    // List.get() returns a Vector — convert to JS array for vectorFromArray
    const vec = p.values;
    const arr: string[] = [];
    for (let i = 0; i < vec.length; i++) arr.push(vec.get(i));
    return { result: arr };
  },
});

const mapStrInt = makeMapType(
  new Field("key", new Utf8(), false),
  new Field("value", new Int64(), false),
);

protocol.unary("echo_dict", {
  params: new Schema([new Field("mapping", mapStrInt, false)]),
  result: new Schema([new Field("result", mapStrInt, false)]),
  // Map_ values are raw Data objects (passthrough) due to arrow-js bug
  handler: (p) => ({ result: p.mapping }),
});

const nestedList = new List(
  new Field("item", new List(new Field("item", new Int64(), false)), false),
);

protocol.unary("echo_nested_list", {
  params: new Schema([new Field("matrix", nestedList, false)]),
  result: new Schema([new Field("result", nestedList, false)]),
  handler: (p) => {
    // Nested list: outer Vector of inner Vectors → JS array of arrays
    const outer = p.matrix;
    const rows: bigint[][] = [];
    for (let i = 0; i < outer.length; i++) {
      const inner = outer.get(i);
      const row: bigint[] = [];
      for (let j = 0; j < inner.length; j++) row.push(inner.get(j));
      rows.push(row);
    }
    return { result: rows };
  },
});

// ===== Optional/Nullable (2) =====

protocol.unary("echo_optional_string", {
  params: new Schema([new Field("value", new Utf8(), true)]),
  result: new Schema([new Field("result", new Utf8(), true)]),
  handler: (p) => ({ result: p.value }),
});

protocol.unary("echo_optional_int", {
  params: new Schema([new Field("value", new Int64(), true)]),
  result: new Schema([new Field("result", new Int64(), true)]),
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

protocol.producer<{}>("produce_empty", {
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
      throw new RuntimeError(
        `intentional error after ${state.emitBeforeError} batches`,
      );
    }
    out.emitRow({ index: state.current, value: state.current * 10 });
    state.current++;
  },
  paramTypes: { emit_before_error: "int" },
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
    const value = input.getChildAt(0)?.get(0) as number;
    out.emitRow({ value: value * state.factor });
  },
});

protocol.exchange<{ runningSum: number; exchangeCount: number }>("exchange_accumulate", {
  params: {},
  inputSchema: ACCUM_INPUT,
  outputSchema: ACCUM_OUTPUT,
  init: () => ({ runningSum: 0, exchangeCount: 0 }),
  exchange: (state, input: RecordBatch, out) => {
    const value = input.getChildAt(0)?.get(0) as number;
    state.runningSum += value;
    state.exchangeCount++;
    out.emitRow({ running_sum: state.runningSum, exchange_count: state.exchangeCount });
  },
});

protocol.exchange<{}>("exchange_with_logs", {
  params: {},
  inputSchema: SCALE_INPUT,
  outputSchema: SCALE_OUTPUT,
  init: () => ({}),
  exchange: (_state, input: RecordBatch, out) => {
    out.clientLog("INFO", "exchange processing");
    out.clientLog("DEBUG", "exchange debug");
    const value = input.getChildAt(0)?.get(0) as number;
    out.emitRow({ value });
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
      throw new RuntimeError(
        `intentional error on exchange ${state.exchangeCount}`,
      );
    }
    const value = input.getChildAt(0)?.get(0) as number;
    out.emitRow({ value });
  },
  paramTypes: { fail_on: "int" },
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
    const value = input.getChildAt(0)?.get(0) as number;
    out.emitRow({ value: value * state.factor });
  },
});
