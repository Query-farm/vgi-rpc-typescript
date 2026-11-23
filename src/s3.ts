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

import { DEFAULT_MAX_FETCH_BYTES, type ExternalStorage, ObjectMissingError, type ObjectStore } from "./external.js";

async function sha256Base64(data: Uint8Array): Promise<string> {
  const buf = new ArrayBuffer(data.byteLength);
  new Uint8Array(buf).set(data);
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", buf));
  let s = "";
  for (const b of digest) s += String.fromCharCode(b);
  return btoa(s);
}

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
 * Create an S3-backed ExternalStorage (also an {@link ObjectStore}: stored
 * `StateRef` objects and argument references, keyed by object key).
 *
 * Lazily imports `@aws-sdk/client-s3` and `@aws-sdk/s3-request-presigner`
 * on first upload to avoid loading the AWS SDK unless needed.
 *
 * Objects carry no TTL: set a bucket lifecycle rule of at least one day on
 * the prefix.
 */
export function createS3Storage(config: S3StorageConfig): ExternalStorage & ObjectStore {
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

    async putObject(data: Uint8Array): Promise<string> {
      const client = await ensureClient();
      const { PutObjectCommand } = await import("@aws-sdk/client-s3");
      const key = `${prefix}obj/${crypto.randomUUID()}`;
      // The SHA-256 checksum makes S3 verify the body it stores.
      await client.send(
        new PutObjectCommand({
          Bucket: bucket,
          Key: key,
          Body: data,
          ContentLength: data.byteLength,
          ContentType: "application/octet-stream",
          ChecksumSHA256: await sha256Base64(data),
        }),
      );
      return key;
    },

    async get(objectId: string, options: { maxBytes?: number } = {}): Promise<Uint8Array> {
      const limit = options.maxBytes ?? DEFAULT_MAX_FETCH_BYTES;
      const client = await ensureClient();
      const { GetObjectCommand } = await import("@aws-sdk/client-s3");
      let response: any;
      try {
        response = await client.send(new GetObjectCommand({ Bucket: bucket, Key: objectId }));
      } catch (error: any) {
        if (error?.name === "NoSuchKey" || error?.$metadata?.httpStatusCode === 404) {
          throw new ObjectMissingError("S3 object is gone");
        }
        throw error;
      }
      if (typeof response.ContentLength === "number" && response.ContentLength > limit) {
        throw new Error(`S3 object is ${response.ContentLength} bytes, over max_fetch_bytes (${limit})`);
      }
      const bytes: Uint8Array = await response.Body.transformToByteArray();
      if (bytes.byteLength > limit) throw new Error(`S3 object exceeds max_fetch_bytes (${limit})`);
      return bytes;
    },
  };
}
