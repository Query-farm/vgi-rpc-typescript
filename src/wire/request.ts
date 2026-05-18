// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { backend, isMap, type VgiBatch, type VgiSchema } from "../arrow/index.js";
import { REQUEST_ID_KEY, REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY } from "../constants.js";
import { RpcError, VersionError } from "../errors.js";
import { isOpaquePassthroughType } from "./opaque.js";

export interface ParsedRequest {
  methodName: string;
  requestVersion: string;
  requestId: string | null;
  schema: VgiSchema;
  params: Record<string, any>;
  rawMetadata: Map<string, string>;
}

/**
 * Parse a request from a RecordBatch with metadata.
 * Extracts method name, version, and params from the batch.
 */
export function parseRequest(schema: VgiSchema, batch: VgiBatch): ParsedRequest {
  const metadata: Map<string, string> = batch.metadata ?? new Map();

  const methodName = metadata.get(RPC_METHOD_KEY);
  if (methodName === undefined) {
    throw new RpcError(
      "ProtocolError",
      "Missing 'vgi_rpc.method' in request batch custom_metadata. " +
        "Each request batch must carry a 'vgi_rpc.method' key in its Arrow IPC custom_metadata " +
        "with the method name as a UTF-8 string.",
      "",
    );
  }

  const version = metadata.get(REQUEST_VERSION_KEY);
  if (version === undefined) {
    throw new VersionError(
      "Missing 'vgi_rpc.request_version' in request batch custom_metadata. " +
        `Set the 'vgi_rpc.request_version' custom_metadata value to '${REQUEST_VERSION}'.`,
    );
  }
  if (version !== REQUEST_VERSION) {
    throw new VersionError(
      `Unsupported request version '${version}', expected '${REQUEST_VERSION}'. ` +
        `Set the 'vgi_rpc.request_version' custom_metadata value to '${REQUEST_VERSION}'.`,
    );
  }

  const requestId = metadata.get(REQUEST_ID_KEY) ?? null;

  // Extract params from single-row batch
  const params: Record<string, any> = {};
  if (schema.fields.length > 0 && batch.numRows !== 1) {
    throw new RpcError(
      "ProtocolError",
      `Expected 1 row in request batch, got ${batch.numRows}. ` +
        "Each parameter is a column (not a row). The batch should have exactly 1 row.",
      "",
    );
  }

  // Map_ + Date/Time/Timestamp/Duration/Decimal/LargeUtf8/LargeBinary/
  // FixedSizeBinary/Dictionary all need passthrough on arrow-js because
  // its `.get(0)` round-trip is unreliable for those types. On flechette
  // those same types extract cleanly via `col.get(0)`, and `col.data[0]`
  // is a Batch (not a Data) so the arrow-js passthrough trick doesn't
  // apply. Gate on backend.
  const useOpaquePassthrough = backend.name === "arrow-js";
  for (let i = 0; i < schema.fields.length; i++) {
    const field = schema.fields[i];
    if (useOpaquePassthrough && (isMap(field.type) || isOpaquePassthroughType(field.type))) {
      const col = batch.getChildAt(i)!;
      params[field.name] = (col as any).data?.[0] ?? col.get(0);
      continue;
    }
    let value = batch.getChildAt(i)?.get(0);
    // Convert BigInt to Number when safe
    if (typeof value === "bigint") {
      if (value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER)) {
        value = Number(value);
      }
    }
    params[field.name] = value;
  }

  return {
    methodName,
    requestVersion: version,
    requestId,
    schema,
    params,
    rawMetadata: metadata,
  };
}

/**
 * Fill in `defaults` for any params that arrived as null/undefined.
 * The slim DESCRIBE_VERSION 4 wire format no longer carries defaults to the
 * client, so default substitution must happen server-side: the client sends
 * a null in any column it didn't supply, and dispatch swaps in the registered
 * default before invoking the handler.
 */
export function applyDefaults(
  params: Record<string, any>,
  defaults: Record<string, any> | undefined,
): Record<string, any> {
  if (!defaults) return params;
  for (const key of Object.keys(defaults)) {
    if (params[key] == null) {
      params[key] = defaults[key];
    }
  }
  return params;
}
