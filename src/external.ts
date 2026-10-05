// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * External storage support for large Arrow IPC batches.
 *
 * When a batch exceeds a configurable threshold, it is serialized to IPC,
 * optionally compressed with zstd, and uploaded to pluggable storage.
 * The batch is replaced with a zero-row "pointer batch" containing the
 * download URL and SHA-256 checksum in metadata.
 *
 * A result that changes rarely can instead be published once with
 * `publishExternal` and answered with the returned `ExternalRef` on every
 * later call: the dispatcher writes the pointer directly, with no
 * serialization or upload.
 */

import {
  deserializeBatches,
  serializeBatch,
  singleRowBatch,
  type VgiBatch,
  type VgiSchema,
  withBatchMetadata,
} from "./arrow/index.js";
import type { LogMessage } from "./client/types.js";
import {
  LOCATION_FETCH_MS_KEY,
  LOCATION_KEY,
  LOCATION_SHA256_KEY,
  LOCATION_SOURCE_KEY,
  LOG_LEVEL_KEY,
} from "./constants.js";
import { dispatchLogOrError } from "./log-batch.js";
import { zstdCompress, zstdDecompress } from "./util/zstd.js";
import { buildEmptyBatch, coerceInt64 } from "./wire/response.js";

// ---------------------------------------------------------------------------
// Interfaces and configuration
// ---------------------------------------------------------------------------

/** Pluggable storage backend for uploading large batches. */
export interface ExternalStorage {
  /** Upload IPC data and return a URL for retrieval. */
  upload(data: Uint8Array, contentEncoding: string): Promise<string>;
}

/** A pre-signed PUT/GET URL pair for client-side data upload. */
export interface UploadUrl {
  /** Pre-signed PUT URL the client uploads to. */
  uploadUrl: string;
  /** Pre-signed GET URL the server fetches from. */
  downloadUrl: string;
  /** Expiration time (UTC) for the URL pair. */
  expiresAt: Date;
}

/**
 * Generates pre-signed upload URL pairs for client-vended externalization.
 *
 * Implementations must be safe to call from multiple concurrent requests.
 * Object lifecycle is the operator's responsibility — uploaded objects are
 * not automatically deleted by vgi-rpc.
 */
export interface UploadUrlProvider {
  /** Allocate one upload/download URL pair. */
  generateUploadUrl(): Promise<UploadUrl> | UploadUrl;
}

/** Configuration for external storage of large batches. */
export interface ExternalLocationConfig {
  /** Storage backend for uploading. */
  storage: ExternalStorage;
  /** Minimum batch byte size to trigger externalization. Default: 1MB. */
  externalizeThresholdBytes?: number;
  /** Optional zstd compression for uploaded data. */
  compression?: {
    /** Compression algorithm; only `"zstd"` is currently supported. */
    algorithm: "zstd";
    /** zstd compression level. Default: 3. */
    level?: number;
  };
  /** URL validator called before fetching. Throw to reject. Default: HTTPS-only. */
  urlValidator?: ((url: string) => void) | null;
  /** Maximum compressed/on-wire bytes accepted from one fetch. Default: 256 MiB. */
  maxFetchBytes?: number;
  /** Maximum bytes accepted after decompression. Default: 16 * maxFetchBytes. */
  maxDecompressedBytes?: number;
  /** Maximum redirects followed while fetching. Each target is revalidated. Default: 5. */
  maxRedirects?: number;
  /** Request implementation for external downloads. Defaults to global `fetch`. */
  fetch?: typeof globalThis.fetch;
}

/** Monotonic milliseconds, falling back to `Date.now` where `performance` is
 *  absent (older embedders; every runtime this ships to has it). */
function performanceNow(): number {
  return typeof performance !== "undefined" ? performance.now() : Date.now();
}

const DEFAULT_THRESHOLD = 1_048_576; // 1 MB
const DEFAULT_MAX_FETCH_BYTES = 256 * 1024 * 1024;
const DEFAULT_MAX_REDIRECTS = 5;

// ---------------------------------------------------------------------------
// URL validation
// ---------------------------------------------------------------------------

/** Default validator that rejects non-HTTPS URLs. */
export function httpsOnlyValidator(url: string): void {
  const parsed = new URL(url);
  if (parsed.protocol !== "https:") {
    throw new Error(`External location URL must use HTTPS, got "${parsed.protocol}"`);
  }
}

/** Render a URL for diagnostics without bearer query strings or userinfo. */
export function redactExternalUrl(url: string): string {
  try {
    const parsed = new URL(url);
    parsed.username = "";
    parsed.password = "";
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString();
  } catch {
    return "<invalid-url>";
  }
}

function validateFetchConfig(config: ExternalLocationConfig): {
  maxFetchBytes: number;
  maxDecompressedBytes: number;
  maxRedirects: number;
} {
  const maxFetchBytes = config.maxFetchBytes ?? DEFAULT_MAX_FETCH_BYTES;
  const maxDecompressedBytes = config.maxDecompressedBytes ?? Math.min(Number.MAX_SAFE_INTEGER, maxFetchBytes * 16);
  const maxRedirects = config.maxRedirects ?? DEFAULT_MAX_REDIRECTS;
  if (!Number.isSafeInteger(maxFetchBytes) || maxFetchBytes < 0) {
    throw new Error("maxFetchBytes must be a non-negative safe integer");
  }
  if (!Number.isSafeInteger(maxDecompressedBytes) || maxDecompressedBytes < 0) {
    throw new Error("maxDecompressedBytes must be a non-negative safe integer");
  }
  if (!Number.isSafeInteger(maxRedirects) || maxRedirects < 0) {
    throw new Error("maxRedirects must be a non-negative safe integer");
  }
  return { maxFetchBytes, maxDecompressedBytes, maxRedirects };
}

async function readResponseBounded(
  response: Response,
  maxBytes: number,
  controller: AbortController,
): Promise<Uint8Array> {
  const declared = response.headers.get("Content-Length");
  if (declared != null) {
    const length = Number(declared);
    if (Number.isFinite(length) && length > maxBytes) {
      controller.abort();
      throw new Error(`External location fetch exceeds max_fetch_bytes (${length} > ${maxBytes})`);
    }
  }

  if (!response.body) return new Uint8Array();
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > maxBytes) {
        controller.abort();
        throw new Error(`External location fetch exceeded max_fetch_bytes (${maxBytes} bytes)`);
      }
      chunks.push(value);
    }
  } finally {
    try {
      await reader.cancel();
    } catch {
      // The stream may already be closed or aborted.
    }
    reader.releaseLock();
  }

  const data = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    data.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return data;
}

// ---------------------------------------------------------------------------
// SHA-256 helpers
// ---------------------------------------------------------------------------

async function sha256Hex(data: Uint8Array): Promise<string> {
  // Copy to a plain ArrayBuffer to satisfy Web Crypto API type requirements
  const buf = new ArrayBuffer(data.byteLength);
  new Uint8Array(buf).set(data);
  const hash = await crypto.subtle.digest("SHA-256", buf);
  return Array.from(new Uint8Array(hash))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

// ---------------------------------------------------------------------------
// Detection
// ---------------------------------------------------------------------------

/** Returns true if the batch is a zero-row pointer to external data. */
export function isExternalLocationBatch(batch: VgiBatch): boolean {
  if (batch.numRows !== 0) return false;
  const meta = batch.metadata;
  if (!meta) return false;
  return meta.has(LOCATION_KEY) && !meta.has(LOG_LEVEL_KEY);
}

// ---------------------------------------------------------------------------
// Pointer batch creation
// ---------------------------------------------------------------------------

/** Create a zero-row pointer batch with location URL and optional SHA-256. */
export function makeExternalLocationBatch(schema: VgiSchema, url: string, sha256?: string): VgiBatch {
  const metadata = new Map<string, string>();
  metadata.set(LOCATION_KEY, url);
  if (sha256) {
    metadata.set(LOCATION_SHA256_KEY, sha256);
  }
  return buildEmptyBatch(schema, metadata);
}

// ---------------------------------------------------------------------------
// IPC serialization helpers
// ---------------------------------------------------------------------------

function serializeBatchToIpc(batch: VgiBatch): Uint8Array {
  return serializeBatch(batch);
}

function batchByteSize(batch: VgiBatch): number {
  // Estimate from IPC serialization size for threshold check.
  return serializeBatch(batch).byteLength;
}

// ---------------------------------------------------------------------------
// Write path: externalization
// ---------------------------------------------------------------------------

/**
 * Maybe externalize a batch if it exceeds the threshold.
 * Returns the original batch unchanged if below threshold or no config.
 * @param onUpload Called with bytes actually uploaded. Keeping accounting at
 * this common path prevents new upload call sites from bypassing the total.
 * @param force Bypass only the configured threshold when a negotiated
 * response budget would otherwise reject a batch external storage can rescue.
 */
export async function maybeExternalizeBatch(
  batch: VgiBatch,
  config?: ExternalLocationConfig | null,
  onUpload?: (bytes: number) => void,
  force = false,
): Promise<VgiBatch> {
  if (!config?.storage) return batch;
  if (batch.numRows === 0) return batch;

  const threshold = config.externalizeThresholdBytes ?? DEFAULT_THRESHOLD;
  if (!force && batchByteSize(batch) < threshold) return batch;

  const { url, sha256 } = await uploadIpcBytes(
    serializeBatchToIpc(batch),
    config.storage,
    config.compression,
    onUpload,
  );
  return makeExternalLocationBatch(batch.schema, url, sha256);
}

// ---------------------------------------------------------------------------
// Shared hash / compress / upload
// ---------------------------------------------------------------------------

/**
 * Hash, optionally compress, and upload one serialized IPC stream.
 *
 * The single choke point shared by every server-side externalization path
 * (per-call {@link maybeExternalizeBatch} and {@link publishExternal}), so the
 * bytes a pointer names are always produced the same way: SHA-256 over the
 * raw (pre-compression) IPC bytes, zstd at the configured level (default 3)
 * with `contentEncoding` `"zstd"`, otherwise `""`.
 */
async function uploadIpcBytes(
  ipcData: Uint8Array,
  storage: ExternalStorage,
  compression: ExternalLocationConfig["compression"] | undefined,
  onUpload?: (bytes: number) => void,
): Promise<{ url: string; sha256: string }> {
  // SHA-256 of the raw IPC bytes (pre-compression) for end-to-end verification.
  const sha256 = await sha256Hex(ipcData);

  let body = ipcData;
  let contentEncoding = "";
  if (compression?.algorithm === "zstd") {
    body = (await zstdCompress(ipcData, compression.level ?? 3)) as Uint8Array;
    contentEncoding = "zstd";
  }

  onUpload?.(body.byteLength);
  const url = await storage.upload(body, contentEncoding);
  return { url, sha256 };
}

// ---------------------------------------------------------------------------
// Pre-published references
// ---------------------------------------------------------------------------

/** Brand that identifies an {@link ExternalRef} even across duplicated module
 *  copies (a bundled `dist/` beside `src/`), where `instanceof` would not. */
const EXTERNAL_REF_BRAND: unique symbol = Symbol.for("vgi_rpc.ExternalRef");

const SHA256_HEX = /^[0-9a-f]{64}$/;

/**
 * A reference to an already-published unary result.
 *
 * A unary handler may return an `ExternalRef` in place of its result values.
 * The server then answers with the external-location pointer batch for `url`
 * directly: the result is not built or validated, nothing is serialized,
 * compressed or uploaded during the call, and the ref is used whether or not
 * the server has external storage configured and regardless of
 * `externalizeThresholdBytes` (a ref is never inlined). It does not count
 * toward `maxExternalizedResponseBytes`. Clients resolve it like any other
 * pointer, so they need no change. Unary methods only.
 *
 * Build one with {@link publishExternal} (or by hand for an object published
 * out of band). The object at `url` must be an Arrow IPC stream (optionally
 * `Content-Encoding: zstd`) whose schema is the method's result schema and
 * which holds exactly one 1-row data batch.
 *
 * The caller owns caching the ref and the object's lifecycle: a long-lived ref
 * must not point at an object under the short-TTL lifecycle rule used for
 * per-call uploads, and a pre-signed URL expires -- re-sign or rebuild the ref
 * before then. Only return a ref to callers who are all entitled to the same
 * content.
 *
 * @example
 * ```ts
 * let cached: ExternalRef | undefined;
 * protocol.unary("catalog", {
 *   params: {},
 *   result: { result: str },
 *   handler: async () => {
 *     cached ??= await publishExternalResult(catalogSchema, { result: buildCatalog() }, storage);
 *     return cached;
 *   },
 * });
 * ```
 */
export class ExternalRef {
  /** Where the published IPC stream lives. */
  readonly url: string;
  /**
   * Lowercase hex SHA-256 of the raw (pre-compression) IPC stream bytes, sent
   * as `vgi_rpc.location.sha256`. `undefined` omits the key, so clients skip
   * the content check -- use this for an object rewritten in place or one too
   * large to hash.
   */
  readonly sha256: string | undefined;
  /** @internal */
  readonly [EXTERNAL_REF_BRAND] = true;

  /**
   * @param url - Where the published IPC stream lives; must be non-empty.
   * @param sha256 - Optional lowercase hex SHA-256 (64 characters) of the raw
   *   IPC stream bytes. `null`/`undefined` means no digest.
   * @throws Error if `url` is empty or `sha256` is not 64 lowercase hex characters.
   */
  constructor(url: string, sha256?: string | null) {
    if (typeof url !== "string" || url.length === 0) {
      throw new Error("ExternalRef.url must be non-empty");
    }
    if (sha256 != null && (typeof sha256 !== "string" || !SHA256_HEX.test(sha256))) {
      throw new Error("ExternalRef.sha256 must be 64 lowercase hex characters (or omitted)");
    }
    this.url = url;
    this.sha256 = sha256 ?? undefined;
    Object.freeze(this);
  }

  /** Build the zero-row pointer batch announcing this ref against `schema`
   *  (the method's result schema). */
  pointerBatch(schema: VgiSchema): VgiBatch {
    return makeExternalLocationBatch(schema, this.url, this.sha256);
  }
}

/** True when `value` is an {@link ExternalRef} (brand check, robust to
 *  duplicated module copies). */
export function isExternalRef(value: unknown): value is ExternalRef {
  return typeof value === "object" && value !== null && (value as any)[EXTERNAL_REF_BRAND] === true;
}

/** Options for {@link publishExternal}. */
export interface PublishExternalOptions {
  /** Optional compression applied before upload -- pass the server's
   *  `ExternalLocationConfig.compression` to match it. */
  compression?: ExternalLocationConfig["compression"];
  /** When `false` the ref carries no digest, so clients skip the content
   *  check. Default: `true`. */
  includeSha256?: boolean;
}

/**
 * Publish a unary result batch once and return a reusable {@link ExternalRef}.
 *
 * Serializes `batch` exactly as the per-call externalizer does (an IPC stream
 * of its schema plus this one batch), hashes the raw bytes, compresses when
 * `options.compression` is given, and calls `storage.upload` once. Cache the
 * returned ref and return it from the unary handler on later calls; the
 * server writes the pointer directly.
 *
 * @param batch - The 1-row result batch, built against the method's result
 *   schema (see {@link publishExternalResult} to build it from values).
 * @param storage - Storage backend to upload to.
 * @param options - Compression and digest options.
 * @throws Error if `batch` does not have exactly one row.
 */
export async function publishExternal(
  batch: VgiBatch,
  storage: ExternalStorage,
  options: PublishExternalOptions = {},
): Promise<ExternalRef> {
  if (batch.numRows !== 1) {
    throw new Error(`publishExternal expects a 1-row result batch, got ${batch.numRows} rows`);
  }
  const { url, sha256 } = await uploadIpcBytes(serializeBatchToIpc(batch), storage, options.compression);
  return new ExternalRef(url, options.includeSha256 === false ? undefined : sha256);
}

/**
 * Convenience over {@link publishExternal}: build the 1-row result batch for
 * `schema` (a method's `resultSchema`, e.g.
 * `protocol.getMethod("catalog")!.resultSchema`) from `values` -- the
 * same `{ result: value }` record a handler would return -- and publish it.
 *
 * @throws TypeError if a non-nullable result field is missing from `values`.
 */
export async function publishExternalResult(
  schema: VgiSchema,
  values: Record<string, any>,
  storage: ExternalStorage,
  options: PublishExternalOptions = {},
): Promise<ExternalRef> {
  for (const f of schema.fields) {
    if (values[f.name] === undefined && !f.nullable) {
      throw new TypeError(`Result missing required field '${f.name}'. Got keys: [${Object.keys(values).join(", ")}]`);
    }
  }
  return publishExternal(singleRowBatch(schema, coerceInt64(schema, values)), storage, options);
}

// ---------------------------------------------------------------------------
// Read path: resolution
// ---------------------------------------------------------------------------

/**
 * Resolve an external pointer batch by fetching the data from the URL.
 * Returns the original batch unchanged if not a pointer or no config.
 */
export async function resolveExternalLocation(
  batch: VgiBatch,
  config?: ExternalLocationConfig | null,
  onLog?: (message: LogMessage) => void,
): Promise<VgiBatch> {
  if (!config) return batch;
  if (!isExternalLocationBatch(batch)) return batch;

  const url = batch.metadata?.get(LOCATION_KEY);
  if (!url) return batch;

  const { maxFetchBytes, maxDecompressedBytes, maxRedirects } = validateFetchConfig(config);
  const validator = config.urlValidator === null ? undefined : (config.urlValidator ?? httpsOnlyValidator);
  const startedAt = performanceNow();
  let currentUrl = url;
  let response: Response | undefined;
  let controller: AbortController | undefined;
  for (let redirects = 0; ; redirects++) {
    if (validator) {
      try {
        validator(currentUrl);
      } catch (error) {
        const reason = validator === httpsOnlyValidator && error instanceof Error ? `: ${error.message}` : "";
        throw new Error(`External location URL rejected [url: ${redactExternalUrl(currentUrl)}]${reason}`);
      }
    }

    controller = new AbortController();
    try {
      // Bun otherwise transparently decodes Content-Encoding while retaining
      // the header, which would charge decoded bytes to the encoded-body cap
      // and then attempt to decode them a second time.
      response = await (config.fetch ?? globalThis.fetch)(currentUrl, {
        redirect: "manual",
        signal: controller.signal,
        // Bun-specific and intentionally harmless to standards-only fetch
        // implementations.
        decompress: false,
      } as RequestInit);
    } catch {
      throw new Error(`External location fetch failed [url: ${redactExternalUrl(currentUrl)}]`);
    }

    if (![301, 302, 303, 307, 308].includes(response.status)) break;
    if (redirects >= maxRedirects) {
      controller.abort();
      throw new Error(`External location redirect limit exceeded (${maxRedirects})`);
    }
    const location = response.headers.get("Location");
    if (!location) {
      controller.abort();
      throw new Error(`External location redirect had no Location header [url: ${redactExternalUrl(currentUrl)}]`);
    }
    try {
      currentUrl = new URL(location, currentUrl).toString();
    } catch {
      controller.abort();
      throw new Error(`External location redirect target is invalid [url: ${redactExternalUrl(currentUrl)}]`);
    }
    controller.abort();
  }

  if (!response.ok) {
    throw new Error(
      `External location fetch failed: ${response.status} ${response.statusText} [url: ${redactExternalUrl(currentUrl)}]`,
    );
  }
  let data = await readResponseBounded(response, maxFetchBytes, controller!);

  const contentEncoding = response.headers.get("Content-Encoding");
  if (contentEncoding === "zstd") {
    try {
      data = new Uint8Array(await zstdDecompress(data, maxDecompressedBytes));
    } catch (error) {
      if (error instanceof Error && /(?:decompressed size|\bcap\b)/i.test(error.message)) {
        throw new Error(`External location decompressed body exceeds max_decompressed_bytes (${maxDecompressedBytes})`);
      }
      throw new Error("External location zstd decompression failed");
    }
  }
  if (data.byteLength > maxDecompressedBytes) {
    throw new Error(
      `External location decompressed body exceeds max_decompressed_bytes (${data.byteLength} > ${maxDecompressedBytes})`,
    );
  }

  // Verify SHA-256 if present
  const expectedSha256 = batch.metadata?.get(LOCATION_SHA256_KEY);
  if (expectedSha256) {
    const actualSha256 = await sha256Hex(data);
    if (actualSha256 !== expectedSha256) {
      throw new Error(
        `SHA-256 checksum mismatch for ${redactExternalUrl(currentUrl)}: expected ${expectedSha256}, got ${actualSha256}`,
      );
    }
  }

  // Parse IPC stream.
  //
  // A whole stream, not a single batch: the server uploads the turn's log
  // batches alongside its data batch, and the refreshed stream cursor rides
  // on the data batch inside the payload rather than on the pointer. Taking
  // only the first batch dropped every log a server emitted on an
  // externalized turn, and — when a log came first — handed the caller a log
  // batch as its data.
  let resolved: VgiBatch | null = null;
  for (const candidate of deserializeBatches(data)) {
    if (candidate.numRows === 0 && dispatchLogOrError(candidate as never, onLog)) continue;
    resolved ??= candidate;
  }
  if (resolved === null || (resolved.numRows === 0 && resolved.schema.fields.length === 0)) {
    throw new Error(`No data batch found in external IPC stream from ${redactExternalUrl(currentUrl)}`);
  }

  // Provenance rides on the resolved batch's own metadata, merged — never
  // replacing it. The pointer is gone by now, so without these two keys a
  // caller has no way to say where the batch came from or what the fetch
  // cost. `source` is the *original* pointer URL rather than the last
  // redirect target, and is deliberately unredacted: §12 calls it application
  // metadata, unlike the diagnostic strings `redactExternalUrl` guards.
  const provenance = new Map<string, string>(resolved.metadata ?? []);
  provenance.set(LOCATION_FETCH_MS_KEY, (performanceNow() - startedAt).toFixed(1));
  provenance.set(LOCATION_SOURCE_KEY, url);
  return withBatchMetadata(resolved, provenance);
}
