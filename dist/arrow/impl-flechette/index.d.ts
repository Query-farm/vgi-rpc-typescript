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
/** List of `child` items. */
export declare const list: (child: VgiField) => VgiDataType;
/** Struct of `fields`. */
export declare const struct: (fields: readonly VgiField[]) => VgiDataType;
/** Map (key → value). flechette's `map(keyField, valueField, keysSorted)`
 *  builds the entries Struct internally — same shape as arrow-js's Map_
 *  but a less verbose API. */
export declare const map: (keyField: VgiField, valueField: VgiField, keysSorted?: boolean) => VgiDataType;
/** Dictionary-encoded type. `indices` must be an integer type. */
export declare const dictionary: (indices: VgiDataType, values: VgiDataType, id?: number, ordered?: boolean) => VgiDataType;
export declare function field(name: string, type: VgiDataType, nullable?: boolean, metadata?: Map<string, string>): VgiField;
export declare function schema(fields: readonly VgiField[], metadata?: Map<string, string>): VgiSchema;
export declare function serializeSchema(s: VgiSchema): Uint8Array;
export declare function deserializeSchema(bytes: Uint8Array): VgiSchema;
export declare function serializeBatch(batch: VgiBatch): Uint8Array;
export declare function deserializeBatch(bytes: Uint8Array): VgiBatch;
export declare function columnFromArray(values: any[], type: VgiDataType): VgiColumnData;
export declare function singleRowBatch(s: VgiSchema, values: Record<string, any>): VgiBatch;
export declare function batchFromColumns(s: VgiSchema, columns: Record<string, any[]>): VgiBatch;
export declare function batchFromColumnData(s: VgiSchema, _numRows: number, columnData: VgiColumnData[], _metadata?: Map<string, string>): VgiBatch;
export declare function emptyColumnData(type: VgiDataType): VgiColumnData;
/**
 * 0-row batch carrying per-record-batch (NOT schema) custom metadata.
 *
 * The vgi-rpc wire protocol puts per-call metadata (`vgi_rpc.log_level`,
 * `vgi_rpc.server_id`, `vgi_rpc.request_id`, etc.) on the RecordBatch
 * message — not on the Schema message. We stash it on the Table via
 * `_vgiRecordMetadata`; our patched `tablesToIPC` picks it up and emits
 * it as the Message FlatBuffer's `custom_metadata` field.
 */
export declare function emptyBatchWithMetadata(s: VgiSchema, metadata?: Map<string, string>): VgiBatch;
/** 1-row result batch with optional per-record-batch metadata. */
export declare function singleRowBatchWithMetadata(s: VgiSchema, values: Record<string, any>, metadata?: Map<string, string>): VgiBatch;
/** flechette has no Data passthrough concept (no opaque-type quirk). */
export declare function isOpaqueData(_val: unknown): boolean;
/** Re-emit a batch with a different per-record-batch metadata map (same
 *  schema + data). Shallow-clones the Table so the caller's reference is
 *  not mutated. */
export declare function withBatchMetadata(batch: VgiBatch, metadata: Map<string, string>): VgiBatch;
/**
 * Serialize a sequence of batches into a single multi-batch IPC stream.
 * Uses our flechette fork's `tablesToIPC` (added in
 * github:Query-farm/flechette#fix/timestamp-bigint-encode) to do the
 * concat-then-encode atomically — naive concatenation of multiple
 * `tableToIPC` outputs produces multiple EOS markers, dropping batches
 * past the first.
 */
export declare function serializeBatches(_schema: VgiSchema, batches: VgiBatch[]): Uint8Array;
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
export declare function conformBatchToSchema(batch: VgiBatch, schema: VgiSchema): VgiBatch;
/**
 * Not supported on flechette: the lockstep stdio exchange protocol needs an
 * incremental IPC writer (emit each batch's framing bytes before reading
 * the next input), and flechette only exposes a one-shot table encoder.
 * The flechette backend is selected for workerd / browser / worker, which
 * are HTTP-only (no stdio), so this factory is never reached there. The
 * stdio server requires the arrow-js backend.
 */
export declare function createIncrementalEncoder(_s: VgiSchema): IncrementalEncoder;
//# sourceMappingURL=index.d.ts.map