// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Client-side request-body externalization (upload-URL protocol v2).
 *
 * When a request body would exceed the server-advertised `maxRequestBytes`,
 * we (1) hash it and call `{prefix}/__upload_url__/init` with its exact size
 * and SHA-256 to get one pre-signed upload URL, (2) PUT exactly those bytes to
 * it with every header the server returned, and (3) replace the inline body
 * with a zero-row "pointer" IPC stream that carries the original RPC dispatch
 * metadata plus `vgi_rpc.location: <download-url>` and
 * `vgi_rpc.location.sha256: <hex>`. The server then resolves the pointer,
 * verifies the digest, and dispatches normally.
 *
 * Only a server advertising `VGI-Upload-URL-Support: 2` is uploaded to; a v1
 * (`true`) server is refused rather than sent the retired `count` form.
 *
 * Mirrors Python's `_externalize_via_upload_url()` and `request_upload_urls()`.
 * Hashing is Web Crypto and the PUT is a plain `fetch` with explicit headers,
 * so this runs unchanged in Bun, Node, browsers and Cloudflare Workers.
 */

import { type RecordBatch, RecordBatchReader, type Schema } from "@query-farm/apache-arrow";
import { withBatchMetadata } from "#vgi-rpc-arrow";
import { DEFAULT_ACCEPTED_MAX_RESPONSE_BYTES } from "#vgi-rpc-client-response-budget";
import { REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY } from "../constants.js";
import { RpcError } from "../errors.js";
import { makeExternalLocationBatch, sha256Hex } from "../external.js";
import {
  ARROW_CONTENT_TYPE,
  MAX_UPLOAD_URL_COUNT,
  reservedPath,
  serializeIpcStream,
  UPLOAD_URL_METHOD,
  UPLOAD_URL_PARAMS_SCHEMA,
  UPLOAD_URL_SUPPORT_VERSION,
} from "../http/common.js";
import { ACCEPT_MAX_RESPONSE_BYTES_HEADER, minPositive, optionalResponseBudget } from "../http/response-budget.js";
import { discoverHttpCapabilities, requireResponseBudgetSupport } from "./capabilities.js";
import { readResponseBodyBounded } from "./decode.js";
import { buildRequestIpc, dispatchLogOrError } from "./ipc.js";

// The method name and params schema come from the one place that defines the
// `__upload_url__` wire contract, rather than being restated here.

/** One upload a client intends to make: its exact byte length and lowercase
 *  hex SHA-256 (`__upload_url__` v2 request entry). */
export interface UploadUrlRequest {
  /** Exact byte length of the body that will be uploaded (positive integer). */
  size: number;
  /** SHA-256 of that body, 64 lowercase hex characters. */
  sha256: string;
}

/** One vended upload: the PUT/GET pair for a declared (size, sha256). */
export interface UploadUrlPair {
  /** Pre-signed URL the client PUTs the body to. */
  uploadUrl: string;
  /** Pre-signed URL the server reads the uploaded body from. */
  downloadUrl: string;
  /** When the pair stops being usable. */
  expiresAt: Date;
  /** Headers the PUT must carry verbatim (e.g. `x-amz-checksum-sha256`). */
  uploadHeaders: Record<string, string>;
  /** The storage id the PUT creates (protocol 3.4 servers; absent from older ones). */
  objectId?: string;
  /** A sealed argument reference to that object, for the same principal
   *  (protocol 3.4 servers; absent from older ones). */
  ref?: Uint8Array;
}

const SHA256_HEX = /^[0-9a-f]{64}$/;
const SUPPORTED_UPLOAD_URL_VERSION = Number(UPLOAD_URL_SUPPORT_VERSION);

/**
 * Refuse anything but an upload-URL v2 server. A v1 (`true`) server only takes
 * the retired `count` request and vends unsized URLs, and there is no fallback.
 */
export function requireUploadUrlV2(version: number | null): void {
  if (version === SUPPORTED_UPLOAD_URL_VERSION) return;
  if (version === null) {
    throw new RpcError("NotSupported", "Server does not support upload URLs", "");
  }
  if (version < SUPPORTED_UPLOAD_URL_VERSION) {
    throw new RpcError(
      "ProtocolError",
      `Server upload protocol is too old: it advertises VGI-Upload-URL-Support v${version} and this client needs ${UPLOAD_URL_SUPPORT_VERSION}`,
      "",
    );
  }
  throw new RpcError(
    "ProtocolError",
    `Server upload protocol is newer than this client: it advertises VGI-Upload-URL-Support: ${version} and this client speaks ${UPLOAD_URL_SUPPORT_VERSION}`,
    "",
  );
}

/** Coerce a decoded `upload_headers` map cell (arrow-js MapRow, JS Map, or a
 *  plain object) to a string record. */
function headersFromCell(cell: unknown): Record<string, string> {
  const out: Record<string, string> = {};
  if (cell == null) return out;
  let entries: Iterable<[unknown, unknown]>;
  if (cell instanceof Map) {
    entries = cell.entries();
  } else if (typeof (cell as { toJSON?: unknown }).toJSON === "function") {
    entries = Object.entries((cell as { toJSON(): Record<string, unknown> }).toJSON());
  } else if (typeof (cell as { [Symbol.iterator]?: unknown })[Symbol.iterator] === "function") {
    entries = cell as Iterable<[unknown, unknown]>;
  } else {
    entries = Object.entries(cell as Record<string, unknown>);
  }
  for (const [k, v] of entries) {
    if (typeof k !== "string" || typeof v !== "string") {
      throw new RpcError("ProtocolError", "Server returned malformed upload_headers", "");
    }
    out[k] = v;
  }
  return out;
}

/**
 * POST `__upload_url__/init` for one pre-signed URL pair per `uploads` entry
 * (each the exact size and SHA-256 of a body to be PUT). Server must have an
 * `uploadUrlProvider` configured; otherwise the route returns 404 and we
 * surface that as `RpcError("NotSupported")`. A server that does not
 * advertise `VGI-Upload-URL-Support: 2` is refused before any request.
 */
export async function requestUploadUrls(
  baseUrl: string,
  prefix: string,
  uploads: readonly UploadUrlRequest[],
  authorization?: string,
  fetchFn: typeof globalThis.fetch = globalThis.fetch,
  acceptedMaxResponseBytes = DEFAULT_ACCEPTED_MAX_RESPONSE_BYTES,
  responseBudgetVerified = false,
  uploadUrlVersion?: number | null,
): Promise<UploadUrlPair[]> {
  optionalResponseBudget(acceptedMaxResponseBytes, "acceptedMaxResponseBytes");
  // Checked here as the reference client does; the server re-checks (and adds
  // VGI-Max-Upload-Bytes), so a raw request is what exercises its refusals.
  if (!Array.isArray(uploads) || uploads.length < 1 || uploads.length > MAX_UPLOAD_URL_COUNT) {
    throw new RpcError(
      "ValueError",
      `uploads must have 1-${MAX_UPLOAD_URL_COUNT} entries, got ${Array.isArray(uploads) ? uploads.length : "none"}`,
      "",
    );
  }
  for (const u of uploads) {
    if (!Number.isSafeInteger(u.size) || u.size < 1) {
      throw new RpcError("ValueError", `upload size must be an integer >= 1, got ${u.size}`, "");
    }
    if (typeof u.sha256 !== "string" || !SHA256_HEX.test(u.sha256)) {
      throw new RpcError("ValueError", "upload sha256 must be 64 lowercase hex characters", "");
    }
  }
  let responseLimit = acceptedMaxResponseBytes;
  let version = uploadUrlVersion;
  if (!responseBudgetVerified || version === undefined) {
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
    version = capabilities.uploadUrlVersion;
  }
  requireUploadUrlV2(version ?? null);
  const body = buildRequestIpc(
    UPLOAD_URL_PARAMS_SCHEMA as unknown as Schema,
    { uploads: uploads.map((u) => ({ size: BigInt(u.size), sha256: u.sha256 })) },
    UPLOAD_URL_METHOD,
  );
  const headers: Record<string, string> = { "Content-Type": ARROW_CONTENT_TYPE };
  if (authorization) headers.Authorization = authorization;
  headers[ACCEPT_MAX_RESPONSE_BYTES_HEADER] = String(acceptedMaxResponseBytes);

  const resp = await fetchFn(baseUrl + reservedPath(`${UPLOAD_URL_METHOD}/init`, { prefix }), {
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
    await throwUploadInitError(resp, responseLimit);
  }
  const responseCapabilities = requireResponseBudgetSupport(resp.headers);
  responseLimit = minPositive(responseLimit, responseCapabilities.maxResponseBytes ?? undefined) ?? responseLimit;

  const respBody = await readResponseBodyBounded(resp, responseLimit);
  const reader = await RecordBatchReader.from(respBody);
  await reader.open();

  const pairs: UploadUrlPair[] = [];
  for (const batch of reader.readAll()) {
    if (batch.numRows === 0) continue;
    const uploadCol = batch.getChild("upload_url");
    const downloadCol = batch.getChild("download_url");
    const expiresCol = batch.getChild("expires_at");
    const headersCol = batch.getChild("upload_headers");
    // Read by name and optional: a pre-3.4 server sends neither.
    const objectIdCol = batch.getChild("object_id");
    const refCol = batch.getChild("ref");
    for (let r = 0; r < batch.numRows; r++) {
      const uploadUrl = uploadCol?.get(r) as string;
      const downloadUrl = downloadCol?.get(r) as string;
      const expiresRaw = expiresCol?.get(r);
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
      const pair: UploadUrlPair = {
        uploadUrl,
        downloadUrl,
        expiresAt,
        uploadHeaders: headersFromCell(headersCol?.get(r)),
      };
      const objectId = objectIdCol?.get(r);
      if (typeof objectId === "string" && objectId) pair.objectId = objectId;
      const ref = refCol?.get(r);
      if (ref instanceof Uint8Array && ref.byteLength > 0) pair.ref = new Uint8Array(ref);
      pairs.push(pair);
    }
  }

  if (pairs.length !== uploads.length) {
    throw new RpcError(
      "ProtocolError",
      `Server returned ${pairs.length} upload URLs for ${uploads.length} requested uploads`,
      "",
    );
  }
  return pairs;
}

/** A refused `__upload_url__/init` carries an RPC error batch (HTTP 400 for
 *  a malformed request): raise it as the server's typed error. Anything else
 *  becomes an `HttpError` with whatever text the body had. */
async function throwUploadInitError(resp: Response, limit: number): Promise<never> {
  let body: Uint8Array | null = null;
  try {
    body = await readResponseBodyBounded(resp, Math.min(limit, 1024 * 1024));
  } catch {
    // fall through: status alone
  }
  if (body && body.byteLength > 0) {
    let batches: Iterable<RecordBatch> | null = null;
    try {
      const reader = await RecordBatchReader.from(body);
      await reader.open();
      batches = reader.readAll();
    } catch {
      const text = new TextDecoder().decode(body).trim();
      if (text)
        throw new RpcError("HttpError", `__upload_url__/init failed: HTTP ${resp.status}: ${text.slice(0, 500)}`, "");
    }
    for (const batch of batches ?? []) dispatchLogOrError(batch);
  }
  throw new RpcError("HttpError", `__upload_url__/init failed: HTTP ${resp.status}`, "");
}

/**
 * Build the externalized pointer body to send in place of *originalBody*.
 *
 * The pointer is a zero-row IPC stream whose first batch carries:
 *   - same schema as the original request batch
 *   - merged custom_metadata: original RPC dispatch keys + `vgi_rpc.location`
 *     + `vgi_rpc.location.sha256` (mandatory on request pointers)
 *
 * The server's request reader honours the dispatch keys for routing, then
 * resolves the pointer to fetch the inline batch for parameter extraction.
 */
async function buildPointerRequestBody(
  originalBody: Uint8Array,
  downloadUrl: string,
  sha256: string,
): Promise<Uint8Array> {
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

  const pointer = makeExternalLocationBatch(schema, downloadUrl, sha256);
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

  // Re-emit the pointer batch with merged metadata. The pointer was built
  // through the facade, so it is the active backend's own batch.
  const pointerWithMeta = withBatchMetadata(pointer, merged);
  return serializeIpcStream(schema as any, [pointerWithMeta]);
}

export interface ExternalizeOptions {
  baseUrl: string;
  prefix: string;
  authorization?: string;
  /** Optional per-URL validator; throw (or reject) to refuse. */
  urlValidator?: ((url: string) => void | Promise<void>) | null;
  fetch?: typeof globalThis.fetch;
  acceptedMaxResponseBytes?: number;
  /** @internal Owning client already completed response-budget discovery. */
  responseBudgetVerified?: boolean;
  /** @internal The upload-URL version the owning client's capability snapshot
   *  holds; omitted, it is discovered. */
  uploadUrlVersion?: number | null;
}

/**
 * Upload *body* via a server-vended URL and return the pointer-batch body
 * that should be sent in place of the original. Throws if the server does
 * not advertise upload-URL v2 support or the upload fails.
 */
export async function externalizeRequestBody(body: Uint8Array, opts: ExternalizeOptions): Promise<Uint8Array> {
  const fetchFn = opts.fetch ?? globalThis.fetch;
  if (opts.uploadUrlVersion !== undefined) requireUploadUrlV2(opts.uploadUrlVersion);
  const sha256 = await sha256Hex(body);
  const pairs = await requestUploadUrls(
    opts.baseUrl,
    opts.prefix,
    [{ size: body.byteLength, sha256 }],
    opts.authorization,
    fetchFn,
    opts.acceptedMaxResponseBytes,
    opts.responseBudgetVerified,
    opts.uploadUrlVersion,
  );
  const pair = pairs[0]!;

  if (opts.urlValidator) {
    await opts.urlValidator(pair.uploadUrl);
    await opts.urlValidator(pair.downloadUrl);
  }

  // Every server-returned header goes on verbatim (the signature covers them).
  // Content-Length is never among them: fetch derives it from the fixed-size
  // body, which is exactly what was signed. A server that sends one anyway is
  // checked rather than forwarded (Fetch forbids setting it).
  const headers: Record<string, string> = {};
  let hasContentType = false;
  for (const [name, value] of Object.entries(pair.uploadHeaders)) {
    const lower = name.toLowerCase();
    if (lower === "content-length") {
      if (value.trim() !== String(body.byteLength)) {
        throw new RpcError(
          "ProtocolError",
          `Server upload_headers declare Content-Length ${value} for a ${body.byteLength}-byte body`,
          "",
        );
      }
      continue;
    }
    if (lower === "content-type") hasContentType = true;
    headers[name] = value;
  }
  // As the reference's upload_put_headers: octet-stream unless the server
  // listed (and so may have signed) a content type.
  if (!hasContentType) headers["Content-Type"] = "application/octet-stream";

  const putResp = await fetchFn(pair.uploadUrl, {
    method: "PUT",
    headers,
    body: body as unknown as BodyInit,
  });
  if (!putResp.ok) {
    const code = s3ErrorCode(await putResp.text().catch(() => ""));
    throw new RpcError(
      "ExternalUploadFailed",
      `PUT to upload URL failed: HTTP ${putResp.status}${code ? ` ${code}` : ""}`,
      "",
    );
  }
  await putResp.body?.cancel().catch(() => {});

  return buildPointerRequestBody(body, pair.downloadUrl, sha256);
}

/** The `<Code>` of an S3-style XML error body (`BadDigest`, `IncompleteBody`, ...). */
function s3ErrorCode(text: string): string | null {
  const m = /<Code>([^<]{1,64})<\/Code>/.exec(text);
  return m ? m[1]! : null;
}
