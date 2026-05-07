// flechette backend for vgi-rpc-typescript's Arrow facade.

import {
  type Column,
  TimeUnit as F_TimeUnit,
  binary as f_binary,
  bool as f_bool,
  columnFromArray as f_columnFromArray,
  field as f_field,
  float32 as f_float32,
  float64 as f_float64,
  int8 as f_int8,
  int16 as f_int16,
  int32 as f_int32,
  int64 as f_int64,
  nullType as f_nullType,
  timestamp as f_timestamp,
  uint8 as f_uint8,
  uint16 as f_uint16,
  uint32 as f_uint32,
  uint64 as f_uint64,
  utf8 as f_utf8,
  tableFromColumns,
  tableFromIPC,
  tablesToIPC,
  tableToIPC,
} from "@uwdata/flechette";

import type { VgiBackendInfo, VgiBatch, VgiColumnData, VgiDataType, VgiField, VgiSchema } from "../types.js";

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
  // Empty 0-row table carries the schema. Need at least one column for
  // flechette to encode; fall back to a placeholder when fields=[].
  if (s.fields.length === 0) {
    const t = tableFromColumns({ __placeholder: f_columnFromArray([], f_utf8()) });
    return tableToIPC(t, { format: "stream" }) as Uint8Array;
  }
  const cols: Record<string, Column<any>> = {};
  for (const f of s.fields) {
    cols[f.name] = f_columnFromArray([], f.type as any);
  }
  return tableToIPC(tableFromColumns(cols), { format: "stream" }) as Uint8Array;
}

export function deserializeSchema(bytes: Uint8Array): VgiSchema {
  return tableFromIPC(bytes, EXTRACT_OPTS).schema as unknown as VgiSchema;
}

export function serializeBatch(batch: VgiBatch): Uint8Array {
  return tableToIPC(batch as any, { format: "stream" }) as Uint8Array;
}

export function deserializeBatch(bytes: Uint8Array): VgiBatch {
  return tableFromIPC(bytes, EXTRACT_OPTS) as unknown as VgiBatch;
}

// ----- Construction --------------------------------------------------------

export function columnFromArray(values: any[], type: VgiDataType): VgiColumnData {
  return f_columnFromArray(values, type as any, EXTRACT_OPTS) as VgiColumnData;
}

export function singleRowBatch(s: VgiSchema, values: Record<string, any>): VgiBatch {
  const cols: Record<string, Column<any>> = {};
  for (const f of s.fields) {
    let val = values[f.name];
    if (f.type.typeId === 2 /* Int */ && (f.type as any).bitWidth === 64 && typeof val === "number") {
      val = BigInt(val);
    }
    cols[f.name] = f_columnFromArray([val], f.type as any, EXTRACT_OPTS);
  }
  return tableFromColumns(cols) as unknown as VgiBatch;
}

export function batchFromColumns(s: VgiSchema, columns: Record<string, any[]>): VgiBatch {
  const numRows = s.fields.length > 0 ? (columns[s.fields[0].name]?.length ?? 0) : 0;
  const cols: Record<string, Column<any>> = {};
  for (const f of s.fields) {
    const vals = columns[f.name] ?? new Array(numRows).fill(null);
    cols[f.name] = f_columnFromArray(vals, f.type as any, EXTRACT_OPTS);
  }
  return tableFromColumns(cols) as unknown as VgiBatch;
}

export function batchFromColumnData(
  s: VgiSchema,
  _numRows: number,
  columnData: VgiColumnData[],
  _metadata?: Map<string, string>,
): VgiBatch {
  // flechette's Column objects ARE the column-data handles; assemble via
  // tableFromColumns keyed by schema field names.
  const cols: Record<string, any> = {};
  for (let i = 0; i < s.fields.length; i++) {
    cols[s.fields[i].name] = columnData[i];
  }
  return tableFromColumns(cols) as unknown as VgiBatch;
}

export function emptyColumnData(type: VgiDataType): VgiColumnData {
  return f_columnFromArray([], type as any, EXTRACT_OPTS) as VgiColumnData;
}

/**
 * 0-row batch carrying schema-level metadata. flechette's tableFromColumns
 * doesn't accept metadata directly — we construct via batchFromColumns, then
 * patch the schema.metadata onto the returned table.
 */
export function emptyBatchWithMetadata(s: VgiSchema, metadata?: Map<string, string>): VgiBatch {
  const cols: Record<string, Column<any>> = {};
  for (const f of s.fields) {
    cols[f.name] = f_columnFromArray([], f.type as any, EXTRACT_OPTS);
  }
  const t = tableFromColumns(cols) as any;
  if (metadata && metadata.size > 0) {
    // flechette Schema.metadata is mutable.
    t.schema.metadata = metadata;
  }
  return t as unknown as VgiBatch;
}

/** 1-row result batch with optional metadata. */
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
  if (metadata && metadata.size > 0) {
    t.schema.metadata = metadata;
  }
  return t as unknown as VgiBatch;
}

/** flechette has no Data passthrough concept (no opaque-type quirk). */
export function isOpaqueData(_val: unknown): boolean {
  return false;
}

/** Re-emit a batch with a different metadata map (same schema + data).
 *  flechette stores metadata on Schema; we patch a shallow copy so the
 *  caller's reference isn't mutated. */
export function withBatchMetadata(batch: VgiBatch, metadata: Map<string, string>): VgiBatch {
  const t = batch as any;
  // Clone the schema with new metadata; reuse the underlying batches.
  const newSchema = { ...t.schema, metadata };
  return { ...t, schema: newSchema } as unknown as VgiBatch;
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

/**
 * No-op under flechette: the IPC reader already produces specific types
 * (no Int_/Float_ generic-type quirk that arrow-js exhibits). Field-name
 * validation is still useful but is delegated to downstream consumers
 * since flechette doesn't expose `batch.data.children` for column-by-column
 * type rewriting anyway.
 */
export function conformBatchToSchema(batch: VgiBatch, _schema: VgiSchema): VgiBatch {
  return batch;
}
