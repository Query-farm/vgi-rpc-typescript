// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import {
  type VgiBatch,
  type VgiSchema,
  isInt,
  emptyBatchWithMetadata,
  singleRowBatchWithMetadata,
} from "../arrow/index.js";
import { LOG_EXTRA_KEY, LOG_LEVEL_KEY, LOG_MESSAGE_KEY, REQUEST_ID_KEY, SERVER_ID_KEY } from "../constants.js";

/**
 * Coerce values for Int64 schema fields from Number to BigInt.
 * Handles both single values and arrays. Returns a new record with coerced values.
 */
export function coerceInt64(schema: VgiSchema, values: Record<string, any>): Record<string, any> {
  const result: Record<string, any> = { ...values };
  for (const f of schema.fields) {
    const val = result[f.name];
    if (val === undefined) continue;
    if (!isInt(f.type) || (f.type as any).bitWidth !== 64) continue;

    if (Array.isArray(val)) {
      result[f.name] = val.map((v: any) => (typeof v === "number" ? BigInt(v) : v));
    } else if (typeof val === "number") {
      result[f.name] = BigInt(val);
    }
  }
  return result;
}

/**
 * Build a 1-row result batch with optional metadata.
 * For unary methods, `values` maps field names to single values.
 */
export function buildResultBatch(
  schema: VgiSchema,
  values: Record<string, any>,
  serverId: string,
  requestId: string | null,
): VgiBatch {
  const metadata = new Map<string, string>();
  metadata.set(SERVER_ID_KEY, serverId);
  if (requestId !== null) {
    metadata.set(REQUEST_ID_KEY, requestId);
  }

  if (schema.fields.length === 0) {
    return buildEmptyBatch(schema, metadata);
  }

  // Validate required fields
  for (const f of schema.fields) {
    if (values[f.name] === undefined && !f.nullable) {
      const got = Object.keys(values);
      throw new TypeError(`Handler result missing required field '${f.name}'. Got keys: [${got.join(", ")}]`);
    }
  }

  const coerced = coerceInt64(schema, values);
  return singleRowBatchWithMetadata(schema, coerced, metadata);
}

/**
 * Build a 0-row error batch with EXCEPTION metadata matching Python's Message.from_exception().
 */
export function buildErrorBatch(
  schema: VgiSchema,
  error: Error,
  serverId: string,
  requestId: string | null,
): VgiBatch {
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
  schema: VgiSchema,
  level: string,
  message: string,
  extra?: Record<string, any>,
  serverId?: string,
  requestId?: string | null,
): VgiBatch {
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
export function buildEmptyBatch(schema: VgiSchema, metadata?: Map<string, string>): VgiBatch {
  return emptyBatchWithMetadata(schema, metadata);
}
