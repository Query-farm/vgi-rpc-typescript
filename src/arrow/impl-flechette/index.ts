// flechette backend for vgi-rpc-typescript's Arrow facade.

import {
  type Column,
  TimeUnit as F_TimeUnit,
  binary as f_binary,
  bool as f_bool,
  columnFromArray as f_columnFromArray,
  dateDay as f_dateDay,
  decimal as f_decimal,
  dictionary as f_dictionary,
  duration as f_duration,
  field as f_field,
  fixedSizeBinary as f_fixedSizeBinary,
  float32 as f_float32,
  float64 as f_float64,
  int8 as f_int8,
  int16 as f_int16,
  int32 as f_int32,
  int64 as f_int64,
  largeBinary as f_largeBinary,
  largeUtf8 as f_largeUtf8,
  list as f_list,
  map as f_map,
  nullType as f_nullType,
  struct as f_struct,
  timeMicrosecond as f_timeMicrosecond,
  timestamp as f_timestamp,
  uint8 as f_uint8,
  uint16 as f_uint16,
  uint32 as f_uint32,
  uint64 as f_uint64,
  utf8 as f_utf8,
  Table,
  tableFromColumns,
  tableFromIPC,
  tablesToIPC,
  tableToIPC,
} from "@uwdata/flechette";

import type {
  IncrementalEncoder,
  VgiBackendInfo,
  VgiBatch,
  VgiColumnData,
  VgiDataType,
  VgiField,
  VgiSchema,
} from "../types.js";
import { readFirstRecordBatchMeta } from "./message-meta.js";

export const backend: VgiBackendInfo = { name: "flechette" };

const EXTRACT_OPTS = {
  useBigInt: true,
  useBigIntTimestamp: true,
  useDecimalInt: true,
  useMap: false,
} as const;

// ----- Type factories ------------------------------------------------------

export const nullType = (): VgiDataType => f_nullType() as unknown as VgiDataType;
export const bool = (): VgiDataType => f_bool() as unknown as VgiDataType;
export const int8 = (): VgiDataType => f_int8() as unknown as VgiDataType;
export const int16 = (): VgiDataType => f_int16() as unknown as VgiDataType;
export const int32 = (): VgiDataType => f_int32() as unknown as VgiDataType;
export const int64 = (): VgiDataType => f_int64() as unknown as VgiDataType;
export const uint8 = (): VgiDataType => f_uint8() as unknown as VgiDataType;
export const uint16 = (): VgiDataType => f_uint16() as unknown as VgiDataType;
export const uint32 = (): VgiDataType => f_uint32() as unknown as VgiDataType;
export const uint64 = (): VgiDataType => f_uint64() as unknown as VgiDataType;
export const float32 = (): VgiDataType => f_float32() as unknown as VgiDataType;
export const float64 = (): VgiDataType => f_float64() as unknown as VgiDataType;
export const utf8 = (): VgiDataType => f_utf8() as unknown as VgiDataType;
export const binary = (): VgiDataType => f_binary() as unknown as VgiDataType;

/** Microsecond Timestamp with optional timezone. */
export const timestampMicro = (timezone: string | null = null): VgiDataType =>
  f_timestamp(F_TimeUnit.MICROSECOND, timezone) as unknown as VgiDataType;

/** Date32 with day resolution. */
export const dateDay = (): VgiDataType => f_dateDay() as unknown as VgiDataType;
/** Time64 with microsecond resolution. */
export const timeMicro = (): VgiDataType => f_timeMicrosecond() as unknown as VgiDataType;
/** Duration with microsecond resolution. */
export const durationMicro = (): VgiDataType => f_duration(F_TimeUnit.MICROSECOND) as unknown as VgiDataType;
/** Decimal128 by default; pass bitWidth=256 for Decimal256. */
export const decimal = (precision: number, scale: number, bitWidth: 128 | 256 = 128): VgiDataType =>
  f_decimal(precision, scale, bitWidth) as unknown as VgiDataType;
/** FixedSizeBinary with the given byte width. */
export const fixedSizeBinary = (byteWidth: number): VgiDataType =>
  f_fixedSizeBinary(byteWidth) as unknown as VgiDataType;
/** LargeUtf8 — 64-bit-offset UTF-8 string. */
export const largeUtf8 = (): VgiDataType => f_largeUtf8() as unknown as VgiDataType;
/** LargeBinary — 64-bit-offset binary blob. */
export const largeBinary = (): VgiDataType => f_largeBinary() as unknown as VgiDataType;
/** List of `child` items. */
export const list = (child: VgiField): VgiDataType => f_list(child as any) as unknown as VgiDataType;
/** Struct of `fields`. */
export const struct = (fields: readonly VgiField[]): VgiDataType => f_struct(fields as any) as unknown as VgiDataType;
/** Map (key → value). flechette's `map(keyField, valueField, keysSorted)`
 *  builds the entries Struct internally — same shape as arrow-js's Map_
 *  but a less verbose API. */
export const map = (keyField: VgiField, valueField: VgiField, keysSorted = false): VgiDataType =>
  f_map(keyField as any, valueField as any, keysSorted) as unknown as VgiDataType;
/** Dictionary-encoded type. `indices` must be an integer type. */
export const dictionary = (indices: VgiDataType, values: VgiDataType, id = -1, ordered = false): VgiDataType =>
  f_dictionary(values as any, indices as any, ordered, id) as unknown as VgiDataType;

export function field(name: string, type: VgiDataType, nullable = true, metadata?: Map<string, string>): VgiField {
  return f_field(name, type as any, nullable, metadata ?? new Map()) as unknown as VgiField;
}

export function schema(fields: readonly VgiField[], metadata?: Map<string, string>): VgiSchema {
  return {
    fields,
    metadata: metadata ?? new Map(),
  } as VgiSchema;
}

// ----- IPC -----------------------------------------------------------------

export function serializeSchema(s: VgiSchema): Uint8Array {
  // Build directly so per-field nullable/metadata round-trip (same
  // reason `batchFromColumns` goes through `buildTablePreservingNullable`
  // below) AND so empty schemas stay empty on the wire — the previous
  // version of this function injected a `__placeholder` column for
  // empty schemas, which then leaked into state-token-embedded schema
  // bytes and made `exchange_zero_columns` etc. see a 1-column schema
  // on the next round-trip.
  const cols = s.fields.map((f) => f_columnFromArray([], f.type as any));
  const table = buildTablePreservingNullable(s, cols) as any;
  return tableToIPC(table, { format: "stream" }) as Uint8Array;
}

export function deserializeSchema(bytes: Uint8Array): VgiSchema {
  return tableFromIPC(bytes, EXTRACT_OPTS).schema as unknown as VgiSchema;
}

export function serializeBatch(batch: VgiBatch): Uint8Array {
  return tableToIPC(batch as any, { format: "stream" }) as Uint8Array;
}

export function deserializeBatch(bytes: Uint8Array): VgiBatch {
  const table: any = tableFromIPC(bytes, EXTRACT_OPTS);
  // The patched flechette decoder surfaces per-record-batch custom_metadata
  // on `_vgiRecordMetadata`. flechette still doesn't carry RecordBatch
  // length when the schema has zero columns (it derives numRows from the
  // first column's batch length, which doesn't exist), so we still
  // re-parse the FlatBuffer to recover the row count for that one
  // shape. See message-meta.ts.
  const recordMd: Map<string, string> | undefined = table._vgiRecordMetadata;
  const wantMeta = !!recordMd && recordMd.size > 0;
  // numRows fallback is only needed for zero-column batches.
  const needRowsFallback = table.numRows === 0 && table.schema.fields.length === 0;
  const meta = needRowsFallback ? readFirstRecordBatchMeta(bytes) : null;
  const wantRows = meta !== null && meta.numRows > 0;
  if (!wantRows && !wantMeta) return table as VgiBatch;
  return new Proxy(table, {
    get(target, prop, receiver) {
      if (wantRows && prop === "numRows") return meta!.numRows;
      if (wantMeta && prop === "metadata") return recordMd;
      return Reflect.get(target, prop, receiver);
    },
  }) as unknown as VgiBatch;
}

// ----- Construction --------------------------------------------------------

export function columnFromArray(values: any[], type: VgiDataType): VgiColumnData {
  return f_columnFromArray(values, type as any, EXTRACT_OPTS) as VgiColumnData;
}

// flechette's `tableFromColumns` discards per-field `nullable`/`metadata` —
// it always builds nullable=true fields. The vgi-rpc wire protocol cares:
// the C++ extension validates response schemas exactly, and a `nullable`
// mismatch on a `not null` field rejects the whole batch. Build the Table
// directly with a schema that preserves the source VgiSchema's flags.
function buildTablePreservingNullable(s: VgiSchema, cols: Column<any>[]): VgiBatch {
  const fields = s.fields.map((f, i) =>
    f_field(f.name, cols[i].type as any, (f as any).nullable ?? true, (f as any).metadata ?? null),
  );
  const flechSchema = {
    version: 5,
    endianness: 0,
    fields,
    metadata: (s as any).metadata ?? null,
  };
  return new Table(flechSchema as any, cols) as unknown as VgiBatch;
}

// flechette's Map builder iterates values via for-of and rejects plain
// objects (`{}`) with "value is not iterable". Coerce Map-typed inputs so
// producer code passing `{}` (legal under arrow-js) keeps working.
function isMapType(t: VgiDataType): boolean {
  return (t as any)?.typeId === 17;
}
function coerceForMap(v: any): any {
  if (v == null || v instanceof Map) return v;
  if (Array.isArray(v)) return new Map(v);
  if (typeof v === "object") return new Map(Object.entries(v));
  return v;
}
function coerceValuesForType(values: any[], type: VgiDataType): any[] {
  return isMapType(type) ? values.map(coerceForMap) : values;
}

export function singleRowBatch(s: VgiSchema, values: Record<string, any>): VgiBatch {
  const cols: Column<any>[] = [];
  for (const f of s.fields) {
    let val = values[f.name];
    if (f.type.typeId === 2 /* Int */ && (f.type as any).bitWidth === 64 && typeof val === "number") {
      val = BigInt(val);
    }
    cols.push(f_columnFromArray(coerceValuesForType([val], f.type), f.type as any, EXTRACT_OPTS));
  }
  return buildTablePreservingNullable(s, cols);
}

export function batchFromColumns(s: VgiSchema, columns: Record<string, any[]>): VgiBatch {
  const numRows = s.fields.length > 0 ? (columns[s.fields[0].name]?.length ?? 0) : 0;
  const cols: Column<any>[] = [];
  for (const f of s.fields) {
    const vals = columns[f.name] ?? new Array(numRows).fill(null);
    cols.push(f_columnFromArray(coerceValuesForType(vals, f.type), f.type as any, EXTRACT_OPTS));
  }
  return buildTablePreservingNullable(s, cols);
}

export function batchFromColumnData(
  s: VgiSchema,
  _numRows: number,
  columnData: VgiColumnData[],
  _metadata?: Map<string, string>,
): VgiBatch {
  // flechette's Column objects ARE the column-data handles; build directly so
  // we preserve the schema's per-field nullable/metadata flags.
  return buildTablePreservingNullable(s, columnData as any);
}

export function emptyColumnData(type: VgiDataType): VgiColumnData {
  return f_columnFromArray([], type as any, EXTRACT_OPTS) as VgiColumnData;
}

/**
 * 0-row batch carrying per-record-batch (NOT schema) custom metadata.
 *
 * The vgi-rpc wire protocol puts per-call metadata (`vgi_rpc.log_level`,
 * `vgi_rpc.server_id`, `vgi_rpc.request_id`, etc.) on the RecordBatch
 * message — not on the Schema message. We stash it on the Table via
 * `_vgiRecordMetadata`; our patched `tablesToIPC` picks it up and emits
 * it as the Message FlatBuffer's `custom_metadata` field.
 */
export function emptyBatchWithMetadata(s: VgiSchema, metadata?: Map<string, string>): VgiBatch {
  const cols: Record<string, Column<any>> = {};
  for (const f of s.fields) {
    cols[f.name] = f_columnFromArray([], f.type as any, EXTRACT_OPTS);
  }
  const t = tableFromColumns(cols) as any;
  attachBatchMetadata(t, metadata);
  return t as unknown as VgiBatch;
}

/** 1-row result batch with optional per-record-batch metadata. */
export function singleRowBatchWithMetadata(
  s: VgiSchema,
  values: Record<string, any>,
  metadata?: Map<string, string>,
): VgiBatch {
  const cols: Record<string, Column<any>> = {};
  for (const f of s.fields) {
    let val = values[f.name];
    if (f.type.typeId === 2 /* Int */ && (f.type as any).bitWidth === 64 && typeof val === "number") {
      val = BigInt(val);
    }
    cols[f.name] = f_columnFromArray([val], f.type as any, EXTRACT_OPTS);
  }
  const t = tableFromColumns(cols) as any;
  attachBatchMetadata(t, metadata);
  return t as unknown as VgiBatch;
}

/** Pin per-record-batch metadata so that:
 *  - `batch.metadata` surfaces it to consumer code (matching arrow-js's
 *    RecordBatch.metadata getter behavior).
 *  - The patched flechette encoder picks it up via `_vgiRecordMetadata`
 *    and emits it as the IPC Message's `custom_metadata` field. */
function attachBatchMetadata(t: any, metadata?: Map<string, string>): void {
  if (!metadata || metadata.size === 0) return;
  t._vgiRecordMetadata = metadata;
  // Surface as `batch.metadata` so wire/dispatch code (which expects the
  // arrow-js shape) works uniformly. flechette's Table class has no
  // top-level `metadata` property, so this assignment is non-shadowing.
  t.metadata = metadata;
}

/** flechette has no Data passthrough concept (no opaque-type quirk). */
export function isOpaqueData(_val: unknown): boolean {
  return false;
}

/** Re-emit a batch with a different per-record-batch metadata map (same
 *  schema + data). Shallow-clones the Table so the caller's reference is
 *  not mutated. */
export function withBatchMetadata(batch: VgiBatch, metadata: Map<string, string>): VgiBatch {
  const t = batch as any;
  const clone = Object.assign(Object.create(Object.getPrototypeOf(t)), t);
  attachBatchMetadata(clone, metadata);
  return clone as unknown as VgiBatch;
}

/**
 * Serialize a sequence of batches into a single multi-batch IPC stream.
 * Uses our flechette fork's `tablesToIPC` (added in
 * github:Query-farm/flechette#fix/timestamp-bigint-encode) to do the
 * concat-then-encode atomically — naive concatenation of multiple
 * `tableToIPC` outputs produces multiple EOS markers, dropping batches
 * past the first.
 */
export function serializeBatches(_schema: VgiSchema, batches: VgiBatch[]): Uint8Array {
  if (batches.length === 0) {
    // 0-batch case: emit just the schema with EOS so readers don't choke.
    return serializeSchema(_schema);
  }
  return tablesToIPC(batches as any[], { format: "stream" }) as Uint8Array;
}

/** Arrow Type ids: Int=2, Float=3. */
function isNumericTypeId(typeId: number): boolean {
  return typeId === 2 || typeId === 3;
}

/** Materialize a source column's values cast to a numeric destination type:
 *  int64/uint64 want BigInt, everything else (smaller ints, floats) wants
 *  Number. Nulls pass through. */
function castNumericValues(col: any, dstType: VgiDataType): any[] {
  const wantsBigInt = dstType.typeId === 2 && (dstType as any).bitWidth === 64;
  const out: any[] = [];
  for (let r = 0; r < col.length; r++) {
    const v = col.get(r);
    if (v == null) {
      out.push(v);
    } else if (wantsBigInt) {
      out.push(typeof v === "bigint" ? v : BigInt(Math.trunc(Number(v))));
    } else {
      out.push(typeof v === "bigint" ? Number(v) : v);
    }
  }
  return out;
}

/**
 * Rebuild a batch's columns to match a target schema's field types.
 *
 * flechette's IPC reader produces specific types upfront (no Int_/Float_
 * generic-type quirk that arrow-js exhibits), but a cast-compatible client
 * can still send e.g. int64 where the method declares float64. Mirror the
 * arrow-js backend: materialize+rebuild numeric columns whose type differs
 * from the expected field type. Non-numeric mismatches (and matching types)
 * keep their existing column. Field-name / field-count mismatches surface as
 * TypeErrors so the dispatch layer can convert them to RpcError.
 */
export function conformBatchToSchema(batch: VgiBatch, schema: VgiSchema): VgiBatch {
  const t = batch as any;
  const batchSchema = t?.schema;
  if (!batchSchema || !schema) return batch;
  const batchFields = batchSchema.fields ?? [];
  const expectedFields = schema.fields ?? [];
  if (batchFields.length !== expectedFields.length) {
    throw new TypeError(`Batch has ${batchFields.length} fields, expected ${expectedFields.length}.`);
  }
  for (let i = 0; i < expectedFields.length; i++) {
    const got = batchFields[i]?.name;
    const want = expectedFields[i]?.name;
    if (got !== want) {
      throw new TypeError(`Batch field[${i}] is '${got}', expected '${want}'.`);
    }
  }

  let mutated = false;
  const cols: Column<any>[] = expectedFields.map((f: VgiField, i: number) => {
    const srcCol = t.getChildAt(i);
    const srcType = srcCol.type as VgiDataType;
    const dstType = f.type;
    if (srcType.typeId === dstType.typeId) return srcCol;
    if (isNumericTypeId(srcType.typeId) && isNumericTypeId(dstType.typeId)) {
      mutated = true;
      return f_columnFromArray(castNumericValues(srcCol, dstType), dstType as any, EXTRACT_OPTS);
    }
    return srcCol;
  });

  if (!mutated) return batch;
  const rebuilt = buildTablePreservingNullable(schema, cols) as any;
  if (t._vgiRecordMetadata) attachBatchMetadata(rebuilt, t._vgiRecordMetadata);
  return rebuilt as unknown as VgiBatch;
}

/**
 * Not supported on flechette: the lockstep stdio exchange protocol needs an
 * incremental IPC writer (emit each batch's framing bytes before reading
 * the next input), and flechette only exposes a one-shot table encoder.
 * The flechette backend is selected for workerd / browser / worker, which
 * are HTTP-only (no stdio), so this factory is never reached there. The
 * stdio server requires the arrow-js backend.
 */
export function createIncrementalEncoder(_s: VgiSchema): IncrementalEncoder {
  throw new Error(
    "Incremental IPC streaming (stdio producer/exchange) is not supported on the flechette " +
      "Arrow backend. Use the arrow-js backend for the stdio transport, or the HTTP transport " +
      "on workerd/browser builds.",
  );
}
