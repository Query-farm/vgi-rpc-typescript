// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import {
  emptyBatchWithMetadata,
  isInt,
  singleRowBatchWithMetadata,
  type VgiBatch,
  type VgiSchema,
} from "../arrow/index.js";
import {
  ERROR_KIND_KEY,
  LOG_EXTRA_KEY,
  LOG_LEVEL_KEY,
  LOG_MESSAGE_KEY,
  REQUEST_ID_KEY,
  SERVER_ID_KEY,
} from "../constants.js";

/**
 * Names of the Int64 fields in a schema, computed once per schema object.
 * `VgiSchema` is treated as immutable everywhere, so identity-keying is safe.
 */
const _int64FieldsCache = new WeakMap<VgiSchema, readonly string[]>();
function int64FieldNames(schema: VgiSchema): readonly string[] {
  let names = _int64FieldsCache.get(schema);
  if (names === undefined) {
    const out: string[] = [];
    for (const f of schema.fields) {
      if (isInt(f.type) && (f.type as any).bitWidth === 64) out.push(f.name);
    }
    names = out;
    _int64FieldsCache.set(schema, names);
  }
  return names;
}

/**
 * Coerce values for Int64 schema fields from Number to BigInt.
 * Handles both single values and arrays. Returns a new record with coerced
 * values, or the original record untouched when no coercion is needed.
 */
export function coerceInt64(schema: VgiSchema, values: Record<string, any>): Record<string, any> {
  const int64Fields = int64FieldNames(schema);
  if (int64Fields.length === 0) return values;

  let result: Record<string, any> | null = null;
  for (const name of int64Fields) {
    const val = values[name];
    if (val === undefined) continue;

    if (Array.isArray(val)) {
      // Clone lazily and only map when a Number element is actually present.
      let mapped: any[] | null = null;
      for (let i = 0; i < val.length; i++) {
        if (typeof val[i] === "number") {
          if (mapped === null) mapped = val.slice();
          mapped[i] = BigInt(val[i]);
        }
      }
      if (mapped !== null) {
        result ??= { ...values };
        result[name] = mapped;
      }
    } else if (typeof val === "number") {
      result ??= { ...values };
      result[name] = BigInt(val);
    }
  }
  return result ?? values;
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
export function buildErrorBatch(schema: VgiSchema, error: Error, serverId: string, requestId: string | null): VgiBatch {
  const metadata = new Map<string, string>();
  metadata.set(LOG_LEVEL_KEY, "EXCEPTION");
  // Prefer the standard `error.name` property (which user classes can set
  // via `this.name = "Foo"` even after a bundler renames the class) over
  // `constructor.name`, which is fragile under minification.
  const rpcErrorType = (error as { errorType?: unknown }).errorType;
  const exceptionType =
    typeof rpcErrorType === "string" && rpcErrorType.length > 0
      ? rpcErrorType
      : typeof error.name === "string" && error.name !== "Error"
        ? error.name
        : error.constructor.name;
  const rpcErrorMessage = (error as { errorMessage?: unknown }).errorMessage;
  const exceptionMessage = typeof rpcErrorMessage === "string" ? rpcErrorMessage : error.message;
  metadata.set(LOG_MESSAGE_KEY, `${exceptionType}: ${exceptionMessage}`);

  // Hoist `errorKind` (typed-exception marker) into the EXCEPTION batch
  // metadata as a top-level `vgi_rpc.error_kind` field so clients can
  // branch on the kind without parsing the log_extra JSON blob. Mirrors
  // Python's `Message.from_exception()` + `add_to_metadata()` hoisting.
  const errorKind =
    (error as { errorKind?: unknown }).errorKind ??
    ((error.constructor as { errorKind?: unknown }).errorKind as unknown);
  if (typeof errorKind === "string" && errorKind.length > 0) {
    metadata.set(ERROR_KIND_KEY, errorKind);
  }

  const extra: Record<string, any> = {
    exception_type: exceptionType,
    exception_message: exceptionMessage,
    traceback: error.stack ?? "",
  };
  if (typeof errorKind === "string" && errorKind.length > 0) {
    extra.error_kind = errorKind;
  }
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
