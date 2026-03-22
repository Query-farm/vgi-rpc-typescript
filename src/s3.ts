// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * S3 storage backend for external storage of large Arrow IPC batches.
 *
 * Requires `@aws-sdk/client-s3` and `@aws-sdk/s3-request-presigner`
 * as peer dependencies.
 *
 * @example
 * ```typescript
 * import { createS3Storage } from "@query-farm/vgi-rpc/s3";
 *
 * const storage = createS3Storage({
 *   bucket: "my-bucket",
 *   prefix: "vgi-rpc/",
 * });
 * const handler = createHttpHandler(protocol, {
 *   externalLocation: { storage, externalizeThresholdBytes: 1_048_576 },
 * });
 * ```
 */

import type { ExternalStorage } from "./external.js";

/** Configuration for the S3 storage backend. */
export interface S3StorageConfig {
  /** S3 bucket name. */
  bucket: string;
  /** Key prefix for uploaded objects. Default: "vgi-rpc/". */
  prefix?: string;
  /** Lifetime of pre-signed GET URLs in seconds. Default: 3600 (1 hour). */
  presignExpirySeconds?: number;
  /** AWS region. If omitted, uses default SDK config. */
  region?: string;
  /** Custom S3 endpoint URL (for MinIO, LocalStack, etc.). */
  endpointUrl?: string;
  /** Force path-style addressing (required for some S3-compatible services). */
  forcePathStyle?: boolean;
}

/**
 * Create an S3-backed ExternalStorage.
 *
 * Lazily imports `@aws-sdk/client-s3` and `@aws-sdk/s3-request-presigner`
 * on first upload to avoid loading the AWS SDK unless needed.
 */
export function createS3Storage(config: S3StorageConfig): ExternalStorage {
  const bucket = config.bucket;
  const prefix = config.prefix ?? "vgi-rpc/";
  const presignExpiry = config.presignExpirySeconds ?? 3600;

  // Lazy-loaded AWS SDK clients
  let s3Client: any = null;

  async function ensureClient(): Promise<any> {
    if (s3Client) return s3Client;
    const { S3Client } = await import("@aws-sdk/client-s3");
    const clientConfig: Record<string, any> = {};
    if (config.region) clientConfig.region = config.region;
    if (config.endpointUrl) {
      clientConfig.endpoint = config.endpointUrl;
      clientConfig.forcePathStyle = config.forcePathStyle ?? true;
    }
    s3Client = new S3Client(clientConfig);
    return s3Client;
  }

  return {
    async upload(data: Uint8Array, contentEncoding: string): Promise<string> {
      const client = await ensureClient();
      const { PutObjectCommand } = await import("@aws-sdk/client-s3");
      const { getSignedUrl } = await import("@aws-sdk/s3-request-presigner");
      const { GetObjectCommand } = await import("@aws-sdk/client-s3");

      const key = `${prefix}${crypto.randomUUID()}${contentEncoding === "zstd" ? ".arrow.zst" : ".arrow"}`;

      const putCommand = new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: data,
        ContentType: "application/vnd.apache.arrow.stream",
        ...(contentEncoding ? { ContentEncoding: contentEncoding } : {}),
      });

      await client.send(putCommand);

      // Generate pre-signed GET URL
      const getCommand = new GetObjectCommand({ Bucket: bucket, Key: key });
      const url = await getSignedUrl(client, getCommand, { expiresIn: presignExpiry });
      return url;
    },
  };
}
