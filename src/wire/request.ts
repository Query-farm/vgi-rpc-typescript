// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { backend, isMap, type VgiBatch, type VgiSchema } from "../arrow/index.js";
import { REQUEST_ID_KEY, REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY } from "../constants.js";
import { RpcError, VersionError } from "../errors.js";
import { isOpaquePassthroughType } from "./opaque.js";

// Safe-integer bounds as BigInt, hoisted so per-field parsing doesn't
// re-allocate them on every bigint-valued parameter of every request.
const MIN_SAFE_BIG = BigInt(Number.MIN_SAFE_INTEGER);
const MAX_SAFE_BIG = BigInt(Number.MAX_SAFE_INTEGER);

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
  // apply. The backend advertises whether passthrough is needed.
  const useOpaquePassthrough = backend.opaquePassthrough;
  for (let i = 0; i < schema.fields.length; i++) {
    const field = schema.fields[i];
    if (useOpaquePassthrough && (isMap(field.type) || isOpaquePassthroughType(field.type))) {
      const col = batch.getChildAt(i)!;
      params[field.name] = (col as any).data?.[0] ?? col.get(0);
      continue;
    }
    let value = batch.getChildAt(i)?.get(0);
    // Normalize arrow-js DecimalBigNum wrappers to primitive BigInt.
    //
    // TODO: remove once the stdio transport reads through a facade-aware
    // reader. The stdio reader is arrow-js-coupled (the facade exposes no
    // streaming reader), so under the flechette facade we still receive
    // arrow-js batches; an opaque Decimal column then yields a
    // `DecimalBigNum` (a Uint32Array subclass whose `.toString()` is the
    // numeric value). Downstream construction goes through the flechette
    // facade and expects a primitive — without this the encoder treats the
    // BigNum as a Number and loses precision.
    //
    // Detect structurally via `instanceof Uint32Array` rather than by
    // constructor name (which minifies away): DecimalBigNum is the only
    // opaque type whose `.get(0)` returns a Uint32Array, so this also
    // excludes binary `Uint8Array` values from being mis-parsed as BigInt.
    if (value instanceof Uint32Array && isOpaquePassthroughType(field.type)) {
      // BigInt(decimalBigNum) triggers arrow-js's
      // Symbol.toPrimitive('number') path which throws for values
      // outside the safe-integer range. Go through `.toString()`
      // instead — it handles arbitrary precision.
      try {
        value = BigInt((value as unknown as { toString(): string }).toString());
      } catch {
        // leave as-is on unexpected shape
      }
    }
    // Convert BigInt to Number when safe — but NOT for types whose
    // BigInt-encoded value is type-scaled (Decimal: unscaled integer;
    // Date32: ms-since-epoch; Timestamp/Time/Duration: native-unit
    // ticks). Re-encoding a Number where a BigInt is expected would
    // make the builder apply a `*scale` multiplication (Decimal) or
    // `* 10^unit` (Time/Timestamp), corrupting the value.
    if (typeof value === "bigint" && !isOpaquePassthroughType(field.type)) {
      if (value >= MIN_SAFE_BIG && value <= MAX_SAFE_BIG) {
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
