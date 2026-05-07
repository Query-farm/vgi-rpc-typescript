// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * External storage support for large Arrow IPC batches.
 *
 * When a batch exceeds a configurable threshold, it is serialized to IPC,
 * optionally compressed with zstd, and uploaded to pluggable storage.
 * The batch is replaced with a zero-row "pointer batch" containing the
 * download URL and SHA-256 checksum in metadata.
 */

import {
  type VgiBatch,
  type VgiSchema,
  serializeBatch,
  deserializeBatch,
} from "./arrow/index.js";
import { LOCATION_KEY, LOCATION_SHA256_KEY, LOG_LEVEL_KEY } from "./constants.js";
import { zstdCompress, zstdDecompress } from "./util/zstd.js";
import { buildEmptyBatch } from "./wire/response.js";

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
  compression?: { algorithm: "zstd"; level?: number };
  /** URL validator called before fetching. Throw to reject. Default: HTTPS-only. */
  urlValidator?: ((url: string) => void) | null;
}

const DEFAULT_THRESHOLD = 1_048_576; // 1 MB

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
 */
export async function maybeExternalizeBatch(
  batch: VgiBatch,
  config?: ExternalLocationConfig | null,
): Promise<VgiBatch> {
  if (!config?.storage) return batch;
  if (batch.numRows === 0) return batch;

  const threshold = config.externalizeThresholdBytes ?? DEFAULT_THRESHOLD;
  if (batchByteSize(batch) < threshold) return batch;

  // Serialize to IPC
  let ipcData = serializeBatchToIpc(batch);

  // Compute SHA-256 of raw IPC bytes (pre-compression)
  const checksum = await sha256Hex(ipcData);

  // Optionally compress
  let contentEncoding = "";
  if (config.compression?.algorithm === "zstd") {
    ipcData = (await zstdCompress(ipcData, config.compression.level ?? 3)) as Uint8Array;
    contentEncoding = "zstd";
  }

  // Upload
  const url = await config.storage.upload(ipcData, contentEncoding);

  // Return pointer batch
  return makeExternalLocationBatch(batch.schema, url, checksum);
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
): Promise<VgiBatch> {
  if (!config) return batch;
  if (!isExternalLocationBatch(batch)) return batch;

  const url = batch.metadata?.get(LOCATION_KEY);
  if (!url) return batch;

  // Validate URL
  const validator = config.urlValidator === null ? undefined : (config.urlValidator ?? httpsOnlyValidator);
  if (validator) {
    validator(url);
  }

  // Fetch
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`External location fetch failed: ${response.status} ${response.statusText} [url: ${url}]`);
  }
  let data = new Uint8Array(await response.arrayBuffer());

  // Decompress if needed
  const contentEncoding = response.headers.get("Content-Encoding");
  if (contentEncoding === "zstd") {
    data = new Uint8Array(await zstdDecompress(data));
  }

  // Verify SHA-256 if present
  const expectedSha256 = batch.metadata?.get(LOCATION_SHA256_KEY);
  if (expectedSha256) {
    const actualSha256 = await sha256Hex(data);
    if (actualSha256 !== expectedSha256) {
      throw new Error(`SHA-256 checksum mismatch for ${url}: expected ${expectedSha256}, got ${actualSha256}`);
    }
  }

  // Parse IPC stream
  const resolved = deserializeBatch(data);
  if (resolved.numRows === 0 && resolved.schema.fields.length === 0) {
    throw new Error(`No data batch found in external IPC stream from ${url}`);
  }
  return resolved;
}
