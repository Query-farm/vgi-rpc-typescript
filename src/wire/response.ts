// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import {
  RecordBatch,
  Schema,
  Field,
  Data,
  DataType,
  makeData,
  Struct,
  vectorFromArray,
} from "apache-arrow";
import {
  LOG_LEVEL_KEY,
  LOG_MESSAGE_KEY,
  LOG_EXTRA_KEY,
  SERVER_ID_KEY,
  REQUEST_ID_KEY,
} from "../constants.js";

/**
 * Coerce values for Int64 schema fields from Number to BigInt.
 * Handles both single values and arrays. Returns a new record with coerced values.
 */
export function coerceInt64(schema: Schema, values: Record<string, any>): Record<string, any> {
  const result: Record<string, any> = { ...values };
  for (const field of schema.fields) {
    const val = result[field.name];
    if (val === undefined) continue;
    if (!DataType.isInt(field.type) || (field.type as any).bitWidth !== 64) continue;

    if (Array.isArray(val)) {
      result[field.name] = val.map((v: any) => (typeof v === "number" ? BigInt(v) : v));
    } else if (typeof val === "number") {
      result[field.name] = BigInt(val);
    }
  }
  return result;
}

/**
 * Build a 1-row result batch with optional metadata.
 * For unary methods, `values` maps field names to single values.
 */
export function buildResultBatch(
  schema: Schema,
  values: Record<string, any>,
  serverId: string,
  requestId: string | null,
): RecordBatch {
  const metadata = new Map<string, string>();
  metadata.set(SERVER_ID_KEY, serverId);
  if (requestId !== null) {
    metadata.set(REQUEST_ID_KEY, requestId);
  }

  if (schema.fields.length === 0) {
    return buildEmptyBatch(schema, metadata);
  }

  // Validate required fields
  for (const field of schema.fields) {
    if (values[field.name] === undefined && !field.nullable) {
      const got = Object.keys(values);
      throw new TypeError(
        `Handler result missing required field '${field.name}'. Got keys: [${got.join(", ")}]`,
      );
    }
  }

  const coerced = coerceInt64(schema, values);

  const children = schema.fields.map((f: Field) => {
    let val = coerced[f.name];
    // Raw Data passthrough for Map_ types (whose .get() is broken in arrow-js)
    if (val instanceof Data) {
      return val;
    }
    const arr = vectorFromArray([val], f.type);
    return arr.data[0];
  });

  const structType = new Struct(schema.fields);
  const data = makeData({
    type: structType,
    length: 1,
    children,
    nullCount: 0,
  });

  return new RecordBatch(schema, data, metadata);
}

/**
 * Build a 0-row error batch with EXCEPTION metadata matching Python's Message.from_exception().
 */
export function buildErrorBatch(
  schema: Schema,
  error: Error,
  serverId: string,
  requestId: string | null,
): RecordBatch {
  const metadata = new Map<string, string>();
  metadata.set(LOG_LEVEL_KEY, "EXCEPTION");
  metadata.set(LOG_MESSAGE_KEY, `${error.constructor.name}: ${error.message}`);

  const extra: Record<string, any> = {
    exception_type: error.constructor.name,
    exception_message: error.message,
    traceback: error.stack ?? "",
  };
  metadata.set(LOG_EXTRA_KEY, JSON.stringify(extra));
  metadata.set(SERVER_ID_KEY, serverId);
  if (requestId !== null) {
    metadata.set(REQUEST_ID_KEY, requestId);
  }

  return buildEmptyBatch(schema, metadata);
}

/**
 * Build a 0-row log batch.
 */
export function buildLogBatch(
  schema: Schema,
  level: string,
  message: string,
  extra?: Record<string, any>,
  serverId?: string,
  requestId?: string | null,
): RecordBatch {
  const metadata = new Map<string, string>();
  metadata.set(LOG_LEVEL_KEY, level);
  metadata.set(LOG_MESSAGE_KEY, message);
  if (extra) {
    metadata.set(LOG_EXTRA_KEY, JSON.stringify(extra));
  }
  if (serverId != null) {
    metadata.set(SERVER_ID_KEY, serverId);
  }
  if (requestId != null) {
    metadata.set(REQUEST_ID_KEY, requestId);
  }

  return buildEmptyBatch(schema, metadata);
}

/**
 * Build a 0-row batch from a schema with metadata.
 * Used for error/log batches.
 */
export function buildEmptyBatch(
  schema: Schema,
  metadata?: Map<string, string>,
): RecordBatch {
  const children = schema.fields.map((f: Field) => {
    return makeData({ type: f.type, length: 0, nullCount: 0 });
  });

  if (schema.fields.length === 0) {
    const structType = new Struct(schema.fields);
    const data = makeData({
      type: structType,
      length: 0,
      children: [],
      nullCount: 0,
    });
    return new RecordBatch(schema, data, metadata);
  }

  const structType = new Struct(schema.fields);
  const data = makeData({
    type: structType,
    length: 0,
    children,
    nullCount: 0,
  });

  return new RecordBatch(schema, data, metadata);
}
