// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Google Cloud Storage backend for external storage of large Arrow IPC batches.
 *
 * Requires `@google-cloud/storage` as a peer dependency.
 *
 * @example
 * ```typescript
 * import { createGCSStorage } from "@query-farm/vgi-rpc/gcs";
 *
 * const storage = createGCSStorage({
 *   bucket: "my-bucket",
 *   prefix: "vgi-rpc/",
 * });
 * const handler = createHttpHandler(protocol, {
 *   externalLocation: { storage, externalizeThresholdBytes: 1_048_576 },
 * });
 * ```
 */

import { DEFAULT_MAX_FETCH_BYTES, type ExternalStorage, ObjectMissingError, type ObjectStore } from "./external.js";

/** Configuration for the GCS storage backend. */
export interface GCSStorageConfig {
  /** GCS bucket name. */
  bucket: string;
  /** Key prefix for uploaded objects. Default: "vgi-rpc/". */
  prefix?: string;
  /** Lifetime of signed GET URLs in seconds. Default: 3600 (1 hour). */
  presignExpirySeconds?: number;
  /** GCS project ID. If omitted, uses Application Default Credentials. */
  projectId?: string;
}

/**
 * Create a GCS-backed ExternalStorage.
 *
 * Lazily imports `@google-cloud/storage` on first upload to avoid
 * loading the SDK unless needed.
 */
export function createGCSStorage(config: GCSStorageConfig): ExternalStorage & ObjectStore {
  const bucket = config.bucket;
  const prefix = config.prefix ?? "vgi-rpc/";
  const presignExpiry = config.presignExpirySeconds ?? 3600;

  let storageClient: any = null;

  async function ensureClient(): Promise<any> {
    if (storageClient) return storageClient;
    const { Storage } = await import("@google-cloud/storage");
    const clientConfig: Record<string, any> = {};
    if (config.projectId) clientConfig.projectId = config.projectId;
    storageClient = new Storage(clientConfig);
    return storageClient;
  }

  return {
    async upload(data: Uint8Array, contentEncoding: string): Promise<string> {
      const client = await ensureClient();
      const bucketRef = client.bucket(bucket);
      const blobName = `${prefix}${crypto.randomUUID()}${contentEncoding === "zstd" ? ".arrow.zst" : ".arrow"}`;
      const blob = bucketRef.file(blobName);

      const options: Record<string, any> = {
        contentType: "application/vnd.apache.arrow.stream",
        resumable: false,
      };
      if (contentEncoding) {
        options.metadata = { contentEncoding };
      }

      await blob.save(Buffer.from(data), options);

      // Generate signed GET URL
      const [url] = await blob.getSignedUrl({
        version: "v4" as const,
        action: "read" as const,
        expires: Date.now() + presignExpiry * 1000,
      });

      return url;
    },

    /** Store one object (no TTL: set a bucket lifecycle rule of at least one
     *  day on the prefix); the object name is the id. GCS verifies the body
     *  against the MD5/CRC32C the client library sends. */
    async putObject(data: Uint8Array): Promise<string> {
      const client = await ensureClient();
      const name = `${prefix}obj/${crypto.randomUUID()}`;
      await client
        .bucket(bucket)
        .file(name)
        .save(Buffer.from(data), { contentType: "application/octet-stream", resumable: false, validation: "crc32c" });
      return name;
    },

    async get(objectId: string, options: { maxBytes?: number } = {}): Promise<Uint8Array> {
      const limit = options.maxBytes ?? DEFAULT_MAX_FETCH_BYTES;
      const client = await ensureClient();
      const file = client.bucket(bucket).file(objectId);
      let metadata: any;
      try {
        [metadata] = await file.getMetadata();
      } catch (error: any) {
        if (error?.code === 404) throw new ObjectMissingError("GCS object is gone");
        throw error;
      }
      if (Number(metadata?.size) > limit) {
        throw new Error(`GCS object is ${metadata.size} bytes, over max_fetch_bytes (${limit})`);
      }
      let contents: Uint8Array;
      try {
        [contents] = await file.download();
      } catch (error: any) {
        if (error?.code === 404) throw new ObjectMissingError("GCS object is gone");
        throw error;
      }
      if (contents.byteLength > limit) throw new Error(`GCS object exceeds max_fetch_bytes (${limit})`);
      return new Uint8Array(contents);
    },
  };
}
