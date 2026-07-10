// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import {
  conformBatchToSchema,
  deserializeBatch,
  field,
  int64,
  schema as makeSchema,
  serializeBatches,
  timestampMicro,
  utf8,
  type VgiBatch,
  type VgiSchema,
} from "../arrow/index.js";
import { RPC_ERROR_HEADER } from "../constants.js";
import type { CookieSpec } from "../types.js";
import { gzipDecompress } from "../util/gzip.js";
import { zstdDecompress } from "../util/zstd.js";

/** MIME type for Arrow IPC stream request and response bodies. */
export const ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream";

// --- The `__upload_url__` wire contract ------------------------------------
// Public so an intermediary that terminates or serves the upload-URL flow
// doesn't have to copy the method name and schemas.

/** Synthetic method name for the pre-signed upload-URL endpoint. */
export const UPLOAD_URL_METHOD = "__upload_url__";

/** Server-side ceiling on `count` in one `__upload_url__` request. */
export const MAX_UPLOAD_URL_COUNT = 100;

/** Request schema for `__upload_url__`: how many URL pairs to vend. */
export const UPLOAD_URL_PARAMS_SCHEMA: VgiSchema = makeSchema([field("count", int64(), true)]);

/** Response schema for `__upload_url__`. */
export const UPLOAD_URL_RESPONSE_SCHEMA: VgiSchema = makeSchema([
  field("upload_url", utf8(), false),
  field("download_url", utf8(), false),
  field("expires_at", timestampMicro("UTC"), false),
]);

/**
 * Decode an HTTP body per its `Content-Encoding`, or return it unchanged.
 *
 * Handles the codings vgi-rpc speaks (`zstd`, `gzip`); the header may list
 * several applied in order, which are decoded in reverse. Unknown / `identity`
 * codings are left as-is. Intended for an intermediary (proxy, gateway) that
 * must read a compressed request or response body to inspect or rewrite it.
 */
export async function decodeContentEncoding(
  data: Uint8Array,
  contentEncoding: string | null | undefined,
  maxOutputSize?: number,
): Promise<Uint8Array> {
  if (!contentEncoding) return data;
  const codings = contentEncoding
    .split(",")
    .map((c) => c.trim().toLowerCase())
    .filter((c) => c.length > 0)
    .reverse();
  let result = data;
  for (const coding of codings) {
    if (coding === "zstd") result = await zstdDecompress(result, maxOutputSize);
    else if (coding === "gzip") result = await gzipDecompress(result, maxOutputSize);
    // identity / unknown coding — leave as-is
  }
  return result;
}

// Sticky session header conventions (HTTP-only). Mirrors Python's
// `vgi_rpc.http._common`. Headers — not cookies — so multiple concurrent
// sessions to one host from a single client multiplex correctly.
export const SESSION_HEADER = "VGI-Session";
export const SESSION_ACCEPT_HEADER = "VGI-Session-Accept";
export const SESSION_CLOSE_HEADER = "VGI-Session-Close";
export const STICKY_ENABLED_HEADER = "VGI-Sticky-Enabled";
export const STICKY_DEFAULT_TTL_HEADER = "VGI-Sticky-Default-TTL";
export const STICKY_ECHO_HEADERS_HEADER = "VGI-Sticky-Echo-Headers";

/** Prefix the server uses to tell the client "echo this header on subsequent
 *  requests in this session". Clients capture and replay
 *  `VGI-Echo-<name>: <value>` as plain `<name>: <value>` for the session
 *  lifetime — used for client-driven routing (e.g. `fly-force-instance-id`). */
export const ECHO_HEADER_PREFIX = "VGI-Echo-";

/** Framework-managed sticky session teardown endpoint path component.
 *  `DELETE {prefix}/__session__` idempotently closes the session referenced
 *  by the request's `VGI-Session` header. */
export const SESSION_ENDPOINT = "__session__";

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
export async function readRequestFromBody(body: Uint8Array): Promise<{ schema: VgiSchema; batch: VgiBatch }> {
  const batch = deserializeBatch(body);
  // Reject only truly empty bodies. A zero-field, zero-row batch with batch
  // metadata is a legal exchange/cancel/continuation signal — the state
  // token rides on `batch.metadata` and downstream code (cancel detection,
  // schema conformance gating) is built to handle it.
  if (batch.schema.fields.length === 0 && batch.numRows === 0 && (batch.metadata?.size ?? 0) === 0) {
    throw new HttpRpcError("Empty IPC stream: no schema", 400);
  }
  return { schema: batch.schema, batch };
}
