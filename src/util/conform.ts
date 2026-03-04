// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import {
  type DataType,
  makeData,
  RecordBatch,
  type Schema,
  Struct,
  Type,
  vectorFromArray,
} from "@query-farm/apache-arrow";

/** Return true when the source type's values can be losslessly read and
 *  re-encoded into the target type (e.g., int32 → float64). */
function needsValueCast(src: DataType, dst: DataType): boolean {
  if (src.typeId === dst.typeId) return false;
  // Same broad family (e.g. Float → Float64) — clone is sufficient.
  if (src.constructor === dst.constructor) return false;
  return true;
}

/** Check if a type is a numeric type we can cast between.
 *  Uses typeId instead of instanceof because IPC-deserialized types
 *  may be generic (e.g., Int_ instead of Int64). */
function isNumeric(t: DataType): boolean {
  return t.typeId === Type.Int || t.typeId === Type.Float;
}

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
export function conformBatchToSchema(batch: RecordBatch, schema: Schema): RecordBatch {
  if (batch.numRows === 0) return batch;

  // Validate field count and names match before attempting any cast.
  if (batch.schema.fields.length !== schema.fields.length) {
    throw new TypeError(`Field count mismatch: expected ${schema.fields.length}, got ${batch.schema.fields.length}`);
  }
  for (let i = 0; i < schema.fields.length; i++) {
    if (batch.schema.fields[i].name !== schema.fields[i].name) {
      throw new TypeError(
        `Field name mismatch at index ${i}: expected '${schema.fields[i].name}', got '${batch.schema.fields[i].name}'`,
      );
    }
  }

  const children = schema.fields.map((f, i) => {
    const srcChild = batch.data.children[i];
    const srcType = srcChild.type;
    const dstType = f.type;

    if (!needsValueCast(srcType, dstType)) {
      return srcChild.clone(dstType);
    }

    // Numeric → numeric: read values and rebuild with target type.
    if (isNumeric(srcType) && isNumeric(dstType)) {
      // Read source values via the batch's column vector.
      const col = batch.getChildAt(i)!;
      const values: number[] = [];
      for (let r = 0; r < batch.numRows; r++) {
        const v = col.get(r);
        values.push(typeof v === "bigint" ? Number(v) : (v as number));
      }
      return vectorFromArray(values, dstType).data[0];
    }

    // Fallback: clone type metadata (works for same-layout types).
    return srcChild.clone(dstType);
  });

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
