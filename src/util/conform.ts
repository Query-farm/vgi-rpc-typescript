// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { RecordBatch, Struct, makeData, type Schema } from "@query-farm/apache-arrow";

/**
 * Rebuild a batch's data to match the given schema's field types.
 *
 * Batches deserialized from IPC streams (e.g., from PyArrow) may use generic
 * types (Float) instead of specific ones (Float64).  Arrow-JS's
 * RecordBatchStreamWriter silently drops batches whose child Data types don't
 * match the writer's schema.  Cloning each child Data with the schema's field
 * type fixes the type metadata while preserving the underlying buffers.
 *
 * This is also used to cast compatible input types (e.g., decimal→double,
 * int32→int64) when the input batch schema doesn't exactly match the method's
 * declared input schema.
 */
export function conformBatchToSchema(batch: RecordBatch, schema: Schema): RecordBatch {
  if (batch.numRows === 0) return batch;
  const children = schema.fields.map((f, i) => batch.data.children[i].clone(f.type));
  const structType = new Struct(schema.fields);
  const data = makeData({
    type: structType,
    length: batch.numRows,
    children,
    nullCount: batch.data.nullCount,
    nullBitmap: batch.data.nullBitmap,
  });
  return new RecordBatch(schema, data, batch.metadata);
}
