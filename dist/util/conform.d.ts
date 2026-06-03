import type { VgiBatch, VgiSchema } from "../arrow/index.js";
/**
 * Rebuild a batch's data to match the given schema's field types.
 *
 * Batches deserialized from IPC streams (e.g., from PyArrow) may use generic
 * types (Float) instead of specific ones (Float64).  Arrow-JS's
 * RecordBatchStreamWriter silently drops batches whose child Data types don't
 * match the writer's schema.  Cloning each child Data with the schema's field
 * type fixes the type metadata while preserving the underlying buffers.
 *
 * This is also used to cast compatible input types (e.g., int32→float64,
 * float32→float64) when the input batch schema doesn't exactly match the
 * method's declared input schema.  When the underlying buffer layout differs
 * (e.g., 4-byte int32 vs 8-byte float64), we read the values and build a
 * new vector with the target type.
 */
export declare function conformBatchToSchema(batch: VgiBatch, schema: VgiSchema): VgiBatch;
//# sourceMappingURL=conform.d.ts.map