// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import {
  type VgiBatch,
  type VgiSchema,
  conformBatchToSchema,
  deserializeBatch,
  serializeBatches,
} from "../arrow/index.js";
import { RPC_ERROR_HEADER } from "../constants.js";
import type { CookieSpec } from "../types.js";

export const ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream";

/** Serialize a CookieSpec into a Set-Cookie header value. */
export function formatSetCookieHeader(c: CookieSpec): string {
  const parts: string[] = [];
  if (c.delete) {
    parts.push(`${c.name}=`);
    parts.push("Max-Age=0");
  } else {
    parts.push(`${c.name}=${c.value}`);
    if (c.maxAge !== undefined) parts.push(`Max-Age=${c.maxAge}`);
    if (c.expires) parts.push(`Expires=${c.expires.toUTCString()}`);
  }
  if (c.path) parts.push(`Path=${c.path}`);
  if (c.domain) parts.push(`Domain=${c.domain}`);
  if (c.secure) parts.push("Secure");
  if (c.httpOnly) parts.push("HttpOnly");
  if (c.sameSite) parts.push(`SameSite=${c.sameSite}`);
  if (c.partitioned) parts.push("Partitioned");
  return parts.join("; ");
}

/** Append Set-Cookie headers for each queued CookieSpec onto an existing Headers object. */
export function appendCookieHeaders(headers: Headers, cookies: readonly CookieSpec[]): void {
  for (const c of cookies) {
    headers.append("Set-Cookie", formatSetCookieHeader(c));
  }
}

export class HttpRpcError extends Error {
  constructor(
    message: string,
    public readonly statusCode: number,
  ) {
    super(message);
    this.name = "HttpRpcError";
  }
}

/**
 * Serialize a schema + batches into a complete IPC stream as Uint8Array.
 *
 * A single IPC stream is `[schema_msg, batch_msg, batch_msg, ..., EOS]`.
 * Each backend implements `serializeBatches` to write that atomically —
 * arrow-js via `RecordBatchStreamWriter`, flechette via `tablesToIPC`
 * (added in our flechette fork). Naive concatenation of per-batch streams
 * produces multiple EOS markers and breaks readers.
 */
export function serializeIpcStream(schema: VgiSchema, batches: VgiBatch[]): Uint8Array {
  const conformed = batches.map((b) => conformBatchToSchema(b, schema));
  return serializeBatches(schema, conformed);
}

/**
 * Create a Response with Arrow IPC content type.
 *
 * Server errors (status 500) are translated to HTTP 200 with an
 * ``X-VGI-RPC-Error: true`` header so that clients which discard
 * response bodies on 5xx still receive the Arrow IPC error metadata.
 * Client errors (400, 401, 404, 415) are passed through unchanged.
 */
export function arrowResponse(body: Uint8Array, status = 200, extraHeaders?: Headers): Response {
  const headers = extraHeaders ?? new Headers();
  headers.set("Content-Type", ARROW_CONTENT_TYPE);
  if (status === 500) {
    headers.set(RPC_ERROR_HEADER, "true");
    return new Response(body as unknown as BodyInit, { status: 200, headers });
  }
  return new Response(body as unknown as BodyInit, { status, headers });
}

/** Read schema + first batch from an IPC stream body via the facade. */
export async function readRequestFromBody(
  body: Uint8Array,
): Promise<{ schema: VgiSchema; batch: VgiBatch }> {
  const batch = deserializeBatch(body);
  if (batch.schema.fields.length === 0 && batch.numRows === 0) {
    throw new HttpRpcError("Empty IPC stream: no schema", 400);
  }
  return { schema: batch.schema, batch };
}
