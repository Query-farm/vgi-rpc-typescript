import type { IncrementalEncoder, VgiBackendInfo, VgiBatch, VgiColumnData, VgiDataType, VgiField, VgiSchema } from "../types.js";
export declare const backend: VgiBackendInfo;
export declare const nullType: () => VgiDataType;
export declare const bool: () => VgiDataType;
export declare const int8: () => VgiDataType;
export declare const int16: () => VgiDataType;
export declare const int32: () => VgiDataType;
export declare const int64: () => VgiDataType;
export declare const uint8: () => VgiDataType;
export declare const uint16: () => VgiDataType;
export declare const uint32: () => VgiDataType;
export declare const uint64: () => VgiDataType;
export declare const float32: () => VgiDataType;
export declare const float64: () => VgiDataType;
export declare const utf8: () => VgiDataType;
export declare const binary: () => VgiDataType;
/** Microsecond Timestamp with optional timezone. */
export declare const timestampMicro: (timezone?: string | null) => VgiDataType;
/** Date32 with day resolution. */
export declare const dateDay: () => VgiDataType;
/** Time64 with microsecond resolution. */
export declare const timeMicro: () => VgiDataType;
/** Duration with microsecond resolution. */
export declare const durationMicro: () => VgiDataType;
/** Decimal128 by default; pass bitWidth=256 for Decimal256. */
export declare const decimal: (precision: number, scale: number, bitWidth?: 128 | 256) => VgiDataType;
/** FixedSizeBinary with the given byte width. */
export declare const fixedSizeBinary: (byteWidth: number) => VgiDataType;
/** LargeUtf8 — 64-bit-offset UTF-8 string. */
export declare const largeUtf8: () => VgiDataType;
/** LargeBinary — 64-bit-offset binary blob. */
export declare const largeBinary: () => VgiDataType;
/** List of `child` items. The child field carries name + nullability + type. */
export declare const list: (child: VgiField) => VgiDataType;
/** Struct of `fields`. */
export declare const struct: (fields: readonly VgiField[]) => VgiDataType;
/** Map (key → value) carried as a List<Struct<key,value>>. arrow-js's Map_
 *  constructor takes a child Field whose type is a Struct of [key, value]. */
export declare const map: (keyField: VgiField, valueField: VgiField, keysSorted?: boolean) => VgiDataType;
/** Dictionary-encoded type. `indices` must be an integer type.
 *
 *  `id` is left undefined by default so arrow-js's internal `getId()`
 *  counter assigns a fresh unique id per Dictionary instance. Passing
 *  `-1` (or any concrete number) here would short-circuit that counter
 *  and produce id collisions when multiple Dictionary types are used. */
export declare const dictionary: (indices: VgiDataType, values: VgiDataType, id?: number, ordered?: boolean) => VgiDataType;
export declare function field(name: string, type: VgiDataType, nullable?: boolean, metadata?: Map<string, string>): VgiField;
export declare function schema(fields: readonly VgiField[], metadata?: Map<string, string>): VgiSchema;
export declare function serializeSchema(s: VgiSchema): Uint8Array;
export declare function deserializeSchema(bytes: Uint8Array): VgiSchema;
export declare function serializeBatch(batch: VgiBatch): Uint8Array;
/**
 * Incremental IPC encoder over arrow-js's `RecordBatchStreamWriter`. Each
 * call drains the writer's internal sink queue and returns the new bytes,
 * so the caller can flush them synchronously between lockstep turns.
 *
 * `_writeRecordBatch` is called directly (rather than the public `write`)
 * to bypass arrow-js's schema comparison, which would auto-close the
 * writer and silently drop a batch whose schema differs only in
 * nullability — our output schema is fixed at open time and all batches
 * are structurally compatible.
 */
export declare function createIncrementalEncoder(s: VgiSchema): IncrementalEncoder;
export declare function deserializeBatch(bytes: Uint8Array): VgiBatch;
export declare function columnFromArray(values: any[], type: VgiDataType): VgiColumnData;
/** Build a 1-row batch from {colName: value} dict (Int64 numbers auto-coerced). */
export declare function singleRowBatch(s: VgiSchema, values: Record<string, any>): VgiBatch;
/** Build an N-row batch from columnar arrays. */
export declare function batchFromColumns(s: VgiSchema, columns: Record<string, any[]>): VgiBatch;
/** Build a batch from pre-built column-data handles + schema. */
export declare function batchFromColumnData(s: VgiSchema, numRows: number, columnData: VgiColumnData[], metadata?: Map<string, string>): VgiBatch;
/** Empty-Data builder used when assembling batches with pre-built children. */
export declare function emptyColumnData(type: VgiDataType): VgiColumnData;
/**
 * 0-row batch with optional batch-level metadata (used for log/error/empty
 * tombstone batches by the wire layer).
 */
export declare function emptyBatchWithMetadata(s: VgiSchema, metadata?: Map<string, string>): VgiBatch;
/** 1-row result batch: vectorFromArray each value, support raw Data
 *  passthrough (for Map/opaque types whose .get(0) is unreliable). */
export declare function singleRowBatchWithMetadata(s: VgiSchema, values: Record<string, any>, metadata?: Map<string, string>): VgiBatch;
/** Tag a value as a raw Data passthrough. arrow-js: returns true if the
 *  value is an arrow-js Data instance. flechette: always false (the
 *  flechette backend doesn't surface this opaque-type quirk). */
export declare function isOpaqueData(val: unknown): boolean;
/** Re-emit a batch with a different metadata map (same schema + data). */
export declare function withBatchMetadata(batch: VgiBatch, metadata: Map<string, string>): VgiBatch;
/**
 * Serialize a sequence of batches into a single multi-batch IPC stream.
 * arrow-js's `RecordBatchStreamWriter` writes schema + N batches + EOS
 * incrementally — exactly what consumers expect for a producer/exchange
 * response that emits more than one batch.
 */
export declare function serializeBatches(schema: VgiSchema, batches: VgiBatch[]): Uint8Array;
export declare function conformBatchToSchema(batch: VgiBatch, schema: VgiSchema): VgiBatch;
//# sourceMappingURL=index.d.ts.map