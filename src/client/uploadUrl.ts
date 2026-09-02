// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Client-side request-body externalization.
 *
 * When a request body would exceed the server-advertised `maxRequestBytes`,
 * we (1) call `{prefix}/__upload_url__/init` to get a pre-signed upload URL,
 * (2) PUT the body to that URL, and (3) replace the inline body with a
 * zero-row "pointer" IPC stream that carries the original RPC dispatch
 * metadata plus `vgi_rpc.location: <download-url>`. The server then resolves
 * the pointer and dispatches normally.
 *
 * Mirrors Python's `_externalize_via_upload_url()` and `request_upload_urls()`.
 */

import { Field, Int64, RecordBatchReader, Schema } from "@query-farm/apache-arrow";
import { DEFAULT_ACCEPTED_MAX_RESPONSE_BYTES } from "#vgi-rpc-client-response-budget";
import { REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY } from "../constants.js";
import { RpcError } from "../errors.js";
import { makeExternalLocationBatch } from "../external.js";
import { ARROW_CONTENT_TYPE, serializeIpcStream } from "../http/common.js";
import { ACCEPT_MAX_RESPONSE_BYTES_HEADER, minPositive, optionalResponseBudget } from "../http/response-budget.js";
import { discoverHttpCapabilities, requireResponseBudgetSupport } from "./capabilities.js";
import { readResponseBodyBounded } from "./decode.js";
import { buildRequestIpc } from "./ipc.js";

const UPLOAD_URL_METHOD = "__upload_url__";
const UPLOAD_URL_PARAMS_SCHEMA = new Schema([new Field("count", new Int64(), false)]);

export interface UploadUrlPair {
  uploadUrl: string;
  downloadUrl: string;
  expiresAt: Date;
}

/**
 * POST `__upload_url__/init` and return the requested number of pre-signed
 * URL pairs. Server must have an `uploadUrlProvider` configured; otherwise
 * the route returns 404 and we surface that as `RpcError("NotSupported")`.
 */
export async function requestUploadUrls(
  baseUrl: string,
  prefix: string,
  count: number,
  authorization?: string,
  fetchFn: typeof globalThis.fetch = globalThis.fetch,
  acceptedMaxResponseBytes = DEFAULT_ACCEPTED_MAX_RESPONSE_BYTES,
  responseBudgetVerified = false,
): Promise<UploadUrlPair[]> {
  optionalResponseBudget(acceptedMaxResponseBytes, "acceptedMaxResponseBytes");
  let responseLimit = acceptedMaxResponseBytes;
  if (!responseBudgetVerified) {
    const capabilities = await discoverHttpCapabilities(
      baseUrl,
      prefix,
      authorization,
      acceptedMaxResponseBytes,
      fetchFn,
    );
    if (!capabilities.acceptMaxResponseBytesSupport) {
      throw new RpcError(
        "ProtocolError",
        "Server must advertise VGI-Accept-Max-Response-Bytes-Support: true before RPC dispatch",
        "",
      );
    }
    responseLimit =
      minPositive(acceptedMaxResponseBytes, capabilities.maxResponseBytes ?? undefined) ?? acceptedMaxResponseBytes;
  }
  const body = buildRequestIpc(UPLOAD_URL_PARAMS_SCHEMA, { count: BigInt(count) }, UPLOAD_URL_METHOD);
  const headers: Record<string, string> = { "Content-Type": ARROW_CONTENT_TYPE };
  if (authorization) headers.Authorization = authorization;
  headers[ACCEPT_MAX_RESPONSE_BYTES_HEADER] = String(acceptedMaxResponseBytes);

  const resp = await fetchFn(`${baseUrl}${prefix}/${UPLOAD_URL_METHOD}/init`, {
    method: "POST",
    headers,
    body: body as unknown as BodyInit,
  });
  if (resp.status === 404) {
    throw new RpcError("NotSupported", "Server does not support upload URLs", "");
  }
  if (resp.status === 401) {
    throw new RpcError("AuthenticationError", "Authentication required", "");
  }
  if (!resp.ok) {
    throw new RpcError("HttpError", `__upload_url__/init failed: HTTP ${resp.status}`, "");
  }
  const responseCapabilities = requireResponseBudgetSupport(resp.headers);
  responseLimit = minPositive(responseLimit, responseCapabilities.maxResponseBytes ?? undefined) ?? responseLimit;

  const respBody = await readResponseBodyBounded(resp, responseLimit);
  const reader = await RecordBatchReader.from(respBody);
  await reader.open();

  const pairs: UploadUrlPair[] = [];
  for (const batch of reader.readAll()) {
    if (batch.numRows === 0) continue;
    for (let r = 0; r < batch.numRows; r++) {
      const uploadUrl = batch.getChildAt(0)?.get(r) as string;
      const downloadUrl = batch.getChildAt(1)?.get(r) as string;
      const expiresRaw = batch.getChildAt(2)?.get(r);
      // Timestamp(us) → either a Date, a number (ms), or a bigint (us)
      let expiresAt: Date;
      if (expiresRaw instanceof Date) {
        expiresAt = expiresRaw;
      } else if (typeof expiresRaw === "bigint") {
        expiresAt = new Date(Number(expiresRaw / 1000n));
      } else if (typeof expiresRaw === "number") {
        expiresAt = new Date(expiresRaw);
      } else {
        expiresAt = new Date();
      }
      pairs.push({ uploadUrl, downloadUrl, expiresAt });
    }
  }

  if (pairs.length === 0) {
    throw new RpcError("ProtocolError", "Server returned no upload URLs", "");
  }
  return pairs;
}

/**
 * Build the externalized pointer body to send in place of *originalBody*.
 *
 * The pointer is a zero-row IPC stream whose first batch carries:
 *   - same schema as the original request batch
 *   - merged custom_metadata: original RPC dispatch keys + `vgi_rpc.location`
 *
 * The server's request reader honours the dispatch keys for routing, then
 * resolves the pointer to fetch the inline batch for parameter extraction.
 */
async function buildPointerRequestBody(originalBody: Uint8Array, downloadUrl: string): Promise<Uint8Array> {
  const reader = await RecordBatchReader.from(originalBody);
  await reader.open();
  const schema = reader.schema;
  if (!schema) {
    throw new RpcError("ProtocolError", "Original request body has no schema", "");
  }
  const batches = reader.readAll();
  if (batches.length === 0) {
    throw new RpcError("ProtocolError", "Original request body has no batches", "");
  }
  const original = batches[0];
  const originalMeta = original.metadata ?? new Map<string, string>();

  const pointer = makeExternalLocationBatch(schema, downloadUrl);
  const merged = new Map<string, string>(pointer.metadata ?? new Map());
  // Preserve the original RPC dispatch metadata so the server can route.
  const method = originalMeta.get(RPC_METHOD_KEY);
  const version = originalMeta.get(REQUEST_VERSION_KEY) ?? REQUEST_VERSION;
  if (method) merged.set(RPC_METHOD_KEY, method);
  merged.set(REQUEST_VERSION_KEY, version);
  // Carry over any other keys (request id, state token for exchange, etc).
  for (const [k, v] of originalMeta) {
    if (!merged.has(k)) merged.set(k, v);
  }

  // Re-emit the pointer batch with merged metadata.
  const { RecordBatch } = await import("@query-farm/apache-arrow");
  const pointerWithMeta = new RecordBatch(schema as any, (pointer as any).data, merged);
  return serializeIpcStream(schema, [pointerWithMeta]);
}

export interface ExternalizeOptions {
  baseUrl: string;
  prefix: string;
  authorization?: string;
  /** Optional per-URL validator; throw to reject. */
  urlValidator?: ((url: string) => void) | null;
  fetch?: typeof globalThis.fetch;
  acceptedMaxResponseBytes?: number;
  /** @internal Owning client already completed response-budget discovery. */
  responseBudgetVerified?: boolean;
}

/**
 * Upload *body* via a server-vended URL and return the pointer-batch body
 * that should be sent in place of the original. Throws if the server does
 * not advertise upload-URL support or the upload fails.
 */
export async function externalizeRequestBody(body: Uint8Array, opts: ExternalizeOptions): Promise<Uint8Array> {
  const fetchFn = opts.fetch ?? globalThis.fetch;
  const pairs = await requestUploadUrls(
    opts.baseUrl,
    opts.prefix,
    1,
    opts.authorization,
    fetchFn,
    opts.acceptedMaxResponseBytes,
    opts.responseBudgetVerified,
  );
  const pair = pairs[0];

  if (opts.urlValidator) {
    opts.urlValidator(pair.uploadUrl);
    opts.urlValidator(pair.downloadUrl);
  }

  const putResp = await fetchFn(pair.uploadUrl, {
    method: "PUT",
    headers: { "Content-Type": ARROW_CONTENT_TYPE },
    body: body as unknown as BodyInit,
  });
  if (!putResp.ok) {
    throw new RpcError("ExternalUploadFailed", `PUT to upload URL failed: HTTP ${putResp.status}`, "");
  }

  return buildPointerRequestBody(body, pair.downloadUrl);
}
