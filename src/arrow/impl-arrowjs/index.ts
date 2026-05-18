// arrow-js backend for vgi-rpc-typescript's Arrow facade.

import {
  Binary as A_Binary,
  Bool as A_Bool,
  type DataType as A_DataType,
  DateDay as A_DateDay,
  Decimal as A_Decimal,
  Dictionary as A_Dictionary,
  DurationMicrosecond as A_DurationMicrosecond,
  Field as A_Field,
  FixedSizeBinary as A_FixedSizeBinary,
  Float32 as A_Float32,
  Float64 as A_Float64,
  Int8 as A_Int8,
  Int16 as A_Int16,
  Int32 as A_Int32,
  Int64 as A_Int64,
  LargeBinary as A_LargeBinary,
  LargeUtf8 as A_LargeUtf8,
  List as A_List,
  Map_ as A_Map,
  Null as A_Null,
  RecordBatch as A_RecordBatch,
  Schema as A_Schema,
  Struct as A_Struct,
  TimeMicrosecond as A_TimeMicrosecond,
  Timestamp as A_Timestamp,
  TimeUnit as A_TimeUnit,
  Type as A_Type,
  Uint8 as A_Uint8,
  Uint16 as A_Uint16,
  Uint32 as A_Uint32,
  Uint64 as A_Uint64,
  Utf8 as A_Utf8,
  makeData as a_makeData,
  vectorFromArray as a_vectorFromArray,
  RecordBatchReader,
  RecordBatchStreamWriter,
} from "@query-farm/apache-arrow";

// Local type-only helpers used by conformBatchToSchema.
type _NeedsCast = (src: A_DataType, dst: A_DataType) => boolean;

import type { VgiBackendInfo, VgiBatch, VgiColumnData, VgiDataType, VgiField, VgiSchema } from "../types.js";

export const backend: VgiBackendInfo = { name: "arrow-js" };

// ----- Type factories ------------------------------------------------------

export const nullType = (): VgiDataType => new A_Null() as unknown as VgiDataType;
export const bool = (): VgiDataType => new A_Bool() as unknown as VgiDataType;
export const int8 = (): VgiDataType => new A_Int8() as unknown as VgiDataType;
export const int16 = (): VgiDataType => new A_Int16() as unknown as VgiDataType;
export const int32 = (): VgiDataType => new A_Int32() as unknown as VgiDataType;
export const int64 = (): VgiDataType => new A_Int64() as unknown as VgiDataType;
export const uint8 = (): VgiDataType => new A_Uint8() as unknown as VgiDataType;
export const uint16 = (): VgiDataType => new A_Uint16() as unknown as VgiDataType;
export const uint32 = (): VgiDataType => new A_Uint32() as unknown as VgiDataType;
export const uint64 = (): VgiDataType => new A_Uint64() as unknown as VgiDataType;
export const float32 = (): VgiDataType => new A_Float32() as unknown as VgiDataType;
export const float64 = (): VgiDataType => new A_Float64() as unknown as VgiDataType;
export const utf8 = (): VgiDataType => new A_Utf8() as unknown as VgiDataType;
export const binary = (): VgiDataType => new A_Binary() as unknown as VgiDataType;

/** Microsecond Timestamp with optional timezone. */
export const timestampMicro = (timezone: string | null = null): VgiDataType =>
  new A_Timestamp(A_TimeUnit.MICROSECOND, timezone) as unknown as VgiDataType;

/** Date32 with day resolution. */
export const dateDay = (): VgiDataType => new A_DateDay() as unknown as VgiDataType;
/** Time64 with microsecond resolution. */
export const timeMicro = (): VgiDataType => new A_TimeMicrosecond() as unknown as VgiDataType;
/** Duration with microsecond resolution. */
export const durationMicro = (): VgiDataType => new A_DurationMicrosecond() as unknown as VgiDataType;
/** Decimal128 by default; pass bitWidth=256 for Decimal256. */
export const decimal = (precision: number, scale: number, bitWidth: 128 | 256 = 128): VgiDataType =>
  new A_Decimal(scale, precision, bitWidth) as unknown as VgiDataType;
/** FixedSizeBinary with the given byte width. */
export const fixedSizeBinary = (byteWidth: number): VgiDataType =>
  new A_FixedSizeBinary(byteWidth) as unknown as VgiDataType;
/** LargeUtf8 — 64-bit-offset UTF-8 string. */
export const largeUtf8 = (): VgiDataType => new A_LargeUtf8() as unknown as VgiDataType;
/** LargeBinary — 64-bit-offset binary blob. */
export const largeBinary = (): VgiDataType => new A_LargeBinary() as unknown as VgiDataType;
/** List of `child` items. The child field carries name + nullability + type. */
export const list = (child: VgiField): VgiDataType => new A_List(child as unknown as A_Field) as unknown as VgiDataType;
/** Struct of `fields`. */
export const struct = (fields: readonly VgiField[]): VgiDataType =>
  new A_Struct(fields as unknown as A_Field[]) as unknown as VgiDataType;
/** Map (key → value) carried as a List<Struct<key,value>>. arrow-js's Map_
 *  constructor takes a child Field whose type is a Struct of [key, value]. */
export const map = (keyField: VgiField, valueField: VgiField, keysSorted = false): VgiDataType => {
  const k = keyField as unknown as A_Field;
  const v = valueField as unknown as A_Field;
  const entriesField = new A_Field("entries", new A_Struct([k, v]), /* nullable */ false);
  return new A_Map(entriesField, keysSorted) as unknown as VgiDataType;
};
/** Dictionary-encoded type. `indices` must be an integer type.
 *
 *  `id` is left undefined by default so arrow-js's internal `getId()`
 *  counter assigns a fresh unique id per Dictionary instance. Passing
 *  `-1` (or any concrete number) here would short-circuit that counter
 *  and produce id collisions when multiple Dictionary types are used. */
export const dictionary = (indices: VgiDataType, values: VgiDataType, id?: number, ordered = false): VgiDataType =>
  new A_Dictionary(values as A_DataType, indices as any, id, ordered) as unknown as VgiDataType;

export function field(name: string, type: VgiDataType, nullable = true, metadata?: Map<string, string>): VgiField {
  return new A_Field(name, type as A_DataType, nullable, metadata ?? new Map()) as unknown as VgiField;
}

export function schema(fields: readonly VgiField[], metadata?: Map<string, string>): VgiSchema {
  return new A_Schema(fields as A_Field[], metadata ?? new Map()) as unknown as VgiSchema;
}

// ----- IPC -----------------------------------------------------------------

export function serializeSchema(s: VgiSchema): Uint8Array {
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, s as unknown as A_Schema);
  writer.close();
  return writer.toUint8Array(true);
}

export function deserializeSchema(bytes: Uint8Array): VgiSchema {
  const reader = RecordBatchReader.from(bytes);
  const batches = [...reader];
  if (batches.length > 0) return batches[0].schema as unknown as VgiSchema;
  if (reader.schema) return reader.schema as unknown as VgiSchema;
  throw new Error("Cannot deserialize schema from empty IPC stream");
}

export function serializeBatch(batch: VgiBatch): Uint8Array {
  const a = batch as unknown as A_RecordBatch;
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, a.schema);
  (writer as any)._writeRecordBatch(a);
  writer.close();
  return writer.toUint8Array(true);
}

export function deserializeBatch(bytes: Uint8Array): VgiBatch {
  const reader = RecordBatchReader.from(bytes);
  const batches = [...reader];
  if (batches.length === 0) {
    const sch = reader.schema ?? new A_Schema([]);
    // Build an empty batch matching the schema. arrow-js quirk: empty IPC
    // streams may not give us a Schema; default to no fields.
    const structType = new A_Struct(sch.fields);
    const data = a_makeData({ type: structType, length: 0, children: [], nullCount: 0 });
    return new A_RecordBatch(sch, data) as unknown as VgiBatch;
  }
  return batches[0] as unknown as VgiBatch;
}

// ----- Construction --------------------------------------------------------

export function columnFromArray(values: any[], type: VgiDataType): VgiColumnData {
  return a_vectorFromArray(values, type as A_DataType).data[0] as VgiColumnData;
}

/** Build a 1-row batch from {colName: value} dict (Int64 numbers auto-coerced). */
export function singleRowBatch(s: VgiSchema, values: Record<string, any>): VgiBatch {
  const a = s as unknown as A_Schema;
  const children = a.fields.map((f) => {
    let val = values[f.name];
    if (f.type.typeId === A_Type.Int && (f.type as any).bitWidth === 64) {
      if (typeof val === "number") val = BigInt(val);
    }
    return a_vectorFromArray([val], f.type).data[0];
  });
  const structType = new A_Struct(a.fields);
  const data = a_makeData({ type: structType, length: 1, children, nullCount: 0 });
  return new A_RecordBatch(a, data) as unknown as VgiBatch;
}

/** Build an N-row batch from columnar arrays. */
export function batchFromColumns(s: VgiSchema, columns: Record<string, any[]>): VgiBatch {
  const a = s as unknown as A_Schema;
  const numRows = a.fields.length > 0 ? (columns[a.fields[0].name]?.length ?? 0) : 0;
  const children = a.fields.map((f) => {
    const vals = columns[f.name];
    if (!vals) return a_makeData({ type: f.type, length: numRows, nullCount: numRows });
    return a_vectorFromArray(vals, f.type).data[0];
  });
  const structType = new A_Struct(a.fields);
  const data = a_makeData({ type: structType, length: numRows, children, nullCount: 0 });
  return new A_RecordBatch(a, data) as unknown as VgiBatch;
}

/** Build a batch from pre-built column-data handles + schema. */
export function batchFromColumnData(
  s: VgiSchema,
  numRows: number,
  columnData: VgiColumnData[],
  metadata?: Map<string, string>,
): VgiBatch {
  const a = s as unknown as A_Schema;
  const structType = new A_Struct(a.fields);
  const data = a_makeData({
    type: structType,
    length: numRows,
    children: columnData as any[],
    nullCount: 0,
  });
  // arrow-js Schema doesn't carry batch-level metadata; attach it when present
  // by cloning with a fresh metadata map.
  const finalSchema = metadata && metadata.size > 0 ? new A_Schema(a.fields, metadata) : a;
  return new A_RecordBatch(finalSchema, data) as unknown as VgiBatch;
}

/** Empty-Data builder used when assembling batches with pre-built children. */
export function emptyColumnData(type: VgiDataType): VgiColumnData {
  return makeEmptyDataRecursive(type as A_DataType) as VgiColumnData;
}

/**
 * 0-row batch with optional batch-level metadata (used for log/error/empty
 * tombstone batches by the wire layer).
 */
export function emptyBatchWithMetadata(s: VgiSchema, metadata?: Map<string, string>): VgiBatch {
  const a = s as unknown as A_Schema;
  const children = a.fields.map((f) => makeEmptyDataRecursive(f.type));
  const structType = new A_Struct(a.fields);
  const data = a_makeData({ type: structType, length: 0, children, nullCount: 0 });
  return new A_RecordBatch(a, data, metadata) as unknown as VgiBatch;
}

/** Recursive empty-Data builder — needed for nested types so arrow-js's
 *  IPC writer doesn't crash on List/Map/Struct/Union with absent children. */
function makeEmptyDataRecursive(type: A_DataType): any {
  const M = require("@query-farm/apache-arrow");
  if (M.DataType.isStruct(type)) {
    const children = (type as any).children.map((f: any) => makeEmptyDataRecursive(f.type));
    return a_makeData({ type, length: 0, children, nullCount: 0 } as any);
  }
  if (M.DataType.isList(type)) {
    const childData = makeEmptyDataRecursive((type as any).children[0].type);
    return a_makeData({ type, length: 0, child: childData, nullCount: 0, valueOffsets: new Int32Array([0]) } as any);
  }
  if (M.DataType.isFixedSizeList(type)) {
    const childData = makeEmptyDataRecursive((type as any).children[0].type);
    return a_makeData({ type, length: 0, child: childData, nullCount: 0 } as any);
  }
  if (M.DataType.isMap(type)) {
    const entryType = (type as any).children[0]?.type;
    const entryData = entryType
      ? makeEmptyDataRecursive(entryType)
      : a_makeData({ type: new A_Struct([]), length: 0, children: [], nullCount: 0 });
    return a_makeData({ type, length: 0, child: entryData, nullCount: 0, valueOffsets: new Int32Array([0]) } as any);
  }
  if (M.DataType.isUnion(type)) {
    const children = (type as any).children.map((f: any) => makeEmptyDataRecursive(f.type));
    if (M.DataType.isDenseUnion(type)) {
      return a_makeData({
        type,
        length: 0,
        typeIds: new Int8Array(0),
        valueOffsets: new Int32Array(0),
        children,
        nullCount: 0,
      } as any);
    }
    return a_makeData({
      type,
      length: 0,
      typeIds: new Int8Array(0),
      children,
      nullCount: 0,
    } as any);
  }
  return a_makeData({ type, length: 0, nullCount: 0 });
}

/** 1-row result batch: vectorFromArray each value, support raw Data
 *  passthrough (for Map/opaque types whose .get(0) is unreliable). */
export function singleRowBatchWithMetadata(
  s: VgiSchema,
  values: Record<string, any>,
  metadata?: Map<string, string>,
): VgiBatch {
  const a = s as unknown as A_Schema;
  const M = require("@query-farm/apache-arrow");
  const children = a.fields.map((f) => {
    const val = values[f.name];
    if (val instanceof M.Data) return val;
    return a_vectorFromArray([val], f.type).data[0];
  });
  const structType = new A_Struct(a.fields);
  const data = a_makeData({ type: structType, length: 1, children, nullCount: 0 });
  return new A_RecordBatch(a, data, metadata) as unknown as VgiBatch;
}

/** Tag a value as a raw Data passthrough. arrow-js: returns true if the
 *  value is an arrow-js Data instance. flechette: always false (the
 *  flechette backend doesn't surface this opaque-type quirk). */
export function isOpaqueData(val: unknown): boolean {
  const M = require("@query-farm/apache-arrow");
  return val instanceof M.Data;
}

/** Re-emit a batch with a different metadata map (same schema + data). */
export function withBatchMetadata(batch: VgiBatch, metadata: Map<string, string>): VgiBatch {
  const a = batch as unknown as A_RecordBatch;
  return new A_RecordBatch(a.schema, a.data, metadata) as unknown as VgiBatch;
}

/**
 * Serialize a sequence of batches into a single multi-batch IPC stream.
 * arrow-js's `RecordBatchStreamWriter` writes schema + N batches + EOS
 * incrementally — exactly what consumers expect for a producer/exchange
 * response that emits more than one batch.
 */
export function serializeBatches(schema: VgiSchema, batches: VgiBatch[]): Uint8Array {
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, schema as unknown as A_Schema);
  for (const batch of batches) {
    (writer as any)._writeRecordBatch(batch as unknown as A_RecordBatch);
  }
  writer.close();
  return writer.toUint8Array(true);
}

/**
 * Rebuild a batch's data to match a target schema's field types.
 *
 * Lives on the backend because every line touches arrow-js internals
 * (Data.children, Data.clone, vectorFromArray, makeData with nullBitmap
 * passthrough). flechette's IPC reader produces specific types upfront, so
 * its `conformBatchToSchema` is a no-op — keeping this code out of the
 * shared util/ tree is what lets the worker-cf bundle drop arrow-js.
 */
const _needsValueCast: _NeedsCast = (src, dst) => {
  if (src.typeId === dst.typeId) return false;
  if (src.constructor === dst.constructor) return false;
  return true;
};

const _isNumeric = (t: A_DataType): boolean => t.typeId === A_Type.Int || t.typeId === A_Type.Float;

export function conformBatchToSchema(batch: VgiBatch, schema: VgiSchema): VgiBatch {
  const a = batch as unknown as A_RecordBatch;
  if (a.numRows === 0) return batch;
  const s = schema as unknown as A_Schema;

  if (a.schema.fields.length !== s.fields.length) {
    throw new TypeError(`Field count mismatch: expected ${s.fields.length}, got ${a.schema.fields.length}`);
  }
  for (let i = 0; i < s.fields.length; i++) {
    if (a.schema.fields[i].name !== s.fields[i].name) {
      throw new TypeError(
        `Field name mismatch at index ${i}: expected '${s.fields[i].name}', got '${a.schema.fields[i].name}'`,
      );
    }
  }

  const children = s.fields.map((f, i) => {
    const srcChild = a.data.children[i];
    const srcType = srcChild.type;
    const dstType = f.type;

    if (!_needsValueCast(srcType, dstType)) {
      return srcChild.clone(dstType);
    }
    if (_isNumeric(srcType) && _isNumeric(dstType)) {
      const col = a.getChildAt(i)!;
      const values: number[] = [];
      for (let r = 0; r < a.numRows; r++) {
        const v = col.get(r);
        values.push(typeof v === "bigint" ? Number(v) : (v as number));
      }
      return a_vectorFromArray(values, dstType).data[0];
    }
    return srcChild.clone(dstType);
  });

  const structType = new A_Struct(s.fields);
  const data = a_makeData({
    type: structType,
    length: a.numRows,
    children,
    nullCount: a.data.nullCount,
    nullBitmap: a.data.nullBitmap,
  });
  return new A_RecordBatch(s, data, a.metadata) as unknown as VgiBatch;
}
