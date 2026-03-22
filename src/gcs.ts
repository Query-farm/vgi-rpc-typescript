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

import type { ExternalStorage } from "./external.js";

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
export function createGCSStorage(config: GCSStorageConfig): ExternalStorage {
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
  };
}
