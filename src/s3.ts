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

import {
  DEFAULT_MAX_FETCH_BYTES,
  type ExternalStorage,
  ObjectMissingError,
  type ObjectStore,
  type UploadUrl,
  type UploadUrlProvider,
} from "./external.js";
import {
  checkStallTimeoutMs,
  type StorageResponse,
  StorageStallError,
  storageFetch,
  storageStallTimeoutMsFromEnv,
} from "./storage-http.js";

async function sha256Base64(data: Uint8Array): Promise<string> {
  const buf = new ArrayBuffer(data.byteLength);
  new Uint8Array(buf).set(data);
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", buf));
  let s = "";
  for (const b of digest) s += String.fromCharCode(b);
  return btoa(s);
}

/** Base64 of a 64-character lowercase hex SHA-256 (the `x-amz-checksum-sha256` form). */
function sha256Base64FromHex(sha256Hex: string): string {
  if (!/^[0-9a-f]{64}$/.test(sha256Hex)) throw new Error("sha256 must be 64 lowercase hex characters");
  let s = "";
  for (let i = 0; i < 64; i += 2) s += String.fromCharCode(Number.parseInt(sha256Hex.slice(i, i + 2), 16));
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
  /** Force path-style addressing (required for some S3-compatible services).
   *  Default: `true` when `endpointUrl` is set, else the SDK's default
   *  (virtual-hosted). */
  forcePathStyle?: boolean;
  /**
   * Stall timeout, ms: a request fails (and the SDK retries it) only when no
   * bytes move for this long (connecting counts as one window). There is no whole-request deadline,
   * so a large object on a slow link completes. Default:
   * `VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS`, else 30000.
   */
  stallTimeoutMs?: number;
}

/** RFC 3986 escaping, as the SDK's own query builder (the SigV4 canonical form). */
function escapeUri(value: string): string {
  return encodeURIComponent(value).replace(/[!'()*]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
}

function buildQueryString(query: Record<string, string | string[] | null | undefined>): string {
  const parts: string[] = [];
  for (const rawKey of Object.keys(query).sort()) {
    const value = query[rawKey];
    const key = escapeUri(rawKey);
    if (Array.isArray(value)) {
      for (const v of value) parts.push(`${key}=${escapeUri(v)}`);
    } else if (value || typeof value === "string") {
      parts.push(`${key}=${escapeUri(value)}`);
    } else {
      parts.push(key);
    }
  }
  return parts.join("&");
}

function timeoutError(message: string): Error {
  // The SDK's retry strategy treats `TimeoutError` as transient.
  return Object.assign(new Error(message), { name: "TimeoutError", code: "ETIMEDOUT" });
}

/** The response body, with every chunk restarting the stall window and the
 *  watchdog stopped at its end. A stalled read errors as a `TimeoutError`. */
function watchedBody(r: StorageResponse): ReadableStream<Uint8Array> | null {
  const source = r.response.body;
  if (!source) {
    r.watchdog.stop();
    return null;
  }
  const reader = source.getReader();
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const { done, value } = await reader.read();
        if (done) {
          r.watchdog.stop();
          controller.close();
          return;
        }
        r.watchdog.progress();
        controller.enqueue(value);
      } catch (error) {
        r.watchdog.stop();
        controller.error(r.watchdog.stalled ? timeoutError(new StorageStallError(r.watchdog.stallMs).message) : error);
      }
    },
    cancel(reason) {
      r.watchdog.stop();
      return reader.cancel(reason);
    },
  });
}

/**
 * An AWS SDK request handler on {@link storageFetch}: stall (idle) timeouts,
 * never a whole-request deadline, the same on Bun and Node.
 *
 * The SDK's default Node handler cannot do this reliably: under Bun its socket
 * idle timer is not refreshed while a body is being written (a slow PUT fails
 * although bytes keep moving), and on Node over TLS one large `end(buffer)`
 * refreshes it only once the whole buffer is out. Here a byte-array body goes
 * out as 64 KiB chunks, every chunk the transport takes and every response
 * chunk read restarts the window, and a stall surfaces as a `TimeoutError`,
 * which the SDK retries. Headers (Content-Length, checksums, the signature)
 * are the SDK's own.
 *
 * @param stallTimeoutMs Idle timeout, ms (connecting counts as one window).
 */
export function stallTimeoutRequestHandler(stallTimeoutMs: number): any {
  checkStallTimeoutMs(stallTimeoutMs);
  return {
    metadata: { handlerProtocol: "http/1.1" },
    async handle(request: any, options: { abortSignal?: AbortSignal } = {}) {
      let path: string = request.path;
      const query = buildQueryString(request.query ?? {});
      if (query) path += `?${query}`;
      if (request.fragment) path += `#${request.fragment}`;
      const port = request.port ? `:${request.port}` : "";
      const url = `${request.protocol}//${request.hostname}${port}${path}`;
      const headers: Record<string, string> = {};
      for (const [name, value] of Object.entries(request.headers ?? {})) {
        const lower = name.toLowerCase();
        // fetch derives Host from the URL (the value the SDK signed) and does
        // not take Expect or Transfer-Encoding from the caller.
        if (lower === "host" || lower === "expect" || lower === "transfer-encoding") continue;
        headers[name] = String(value);
      }
      let body: Uint8Array | null = null;
      const raw = request.method === "GET" || request.method === "HEAD" ? undefined : request.body;
      if (typeof raw === "string") body = new TextEncoder().encode(raw);
      else if (raw instanceof Uint8Array) body = raw;
      else if (raw != null) {
        // A stream body (createS3Storage never sends one): collect it first.
        const chunks: Uint8Array[] = [];
        for await (const chunk of raw as AsyncIterable<Uint8Array>) chunks.push(chunk);
        body = new Uint8Array(Buffer.concat(chunks));
      }
      let r: StorageResponse;
      try {
        r = await storageFetch(url, {
          method: request.method,
          headers,
          body,
          stallTimeoutMs,
          redirect: "manual",
          extra: { decompress: false },
          ...(options.abortSignal ? { signal: options.abortSignal } : {}),
        });
      } catch (error) {
        if (error instanceof StorageStallError) throw timeoutError(error.message);
        throw error;
      }
      const responseHeaders: Record<string, string> = {};
      r.response.headers.forEach((value, name) => {
        responseHeaders[name] = value;
      });
      const watched = watchedBody(r);
      const { Readable } = await import("node:stream");
      return {
        response: {
          statusCode: r.response.status,
          reason: r.response.statusText,
          headers: responseHeaders,
          body: watched ? Readable.fromWeb(watched as never) : Readable.from([]),
        },
      };
    },
    updateHttpClientConfig() {},
    httpHandlerConfigs() {
      return {};
    },
    destroy() {},
  };
}

/**
 * Create an S3-backed ExternalStorage (also an {@link ObjectStore}: stored
 * `StateRef` objects and argument references, keyed by object key, and an
 * {@link UploadUrlProvider}: pre-signed client upload URLs, so a server with
 * this storage accepts requests over its request cap and argument references).
 *
 * Lazily imports `@aws-sdk/client-s3` and `@aws-sdk/s3-request-presigner`
 * on first upload to avoid loading the AWS SDK unless needed.
 *
 * Objects carry no TTL: set a bucket lifecycle rule of at least one day on
 * the prefix.
 */
export function createS3Storage(config: S3StorageConfig): ExternalStorage & ObjectStore & UploadUrlProvider {
  const bucket = config.bucket;
  const prefix = config.prefix ?? "vgi-rpc/";
  const presignExpiry = config.presignExpirySeconds ?? 3600;
  const stallTimeoutMs =
    config.stallTimeoutMs !== undefined ? checkStallTimeoutMs(config.stallTimeoutMs) : storageStallTimeoutMsFromEnv();

  // Lazy-loaded AWS SDK clients
  let s3Client: any = null;

  async function ensureClient(): Promise<any> {
    if (s3Client) return s3Client;
    const { S3Client } = await import("@aws-sdk/client-s3");
    const clientConfig: Record<string, any> = { requestHandler: stallTimeoutRequestHandler(stallTimeoutMs) };
    if (config.region) clientConfig.region = config.region;
    if (config.endpointUrl) {
      clientConfig.endpoint = config.endpointUrl;
      clientConfig.forcePathStyle = config.forcePathStyle ?? true;
    } else if (config.forcePathStyle !== undefined) {
      clientConfig.forcePathStyle = config.forcePathStyle;
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

    /**
     * Pre-signed PUT and GET URLs for one sized client upload (upload URLs v2).
     *
     * The PUT URL signs `Content-Length` and `x-amz-checksum-sha256` (SigV4),
     * so S3 refuses any body that is not exactly `size` bytes hashing to
     * `sha256Hex` (`BadDigest`). The checksum stays a signed header (not a
     * query parameter): the client sends it from {@link UploadUrl.headers}.
     * The object is not deleted automatically: set a lifecycle rule on the prefix.
     */
    async generateUploadUrl(size: number, sha256Hex: string): Promise<UploadUrl> {
      if (!Number.isSafeInteger(size) || size < 1) throw new Error(`size must be a positive integer, got ${size}`);
      const checksum = sha256Base64FromHex(sha256Hex);
      const client = await ensureClient();
      const { PutObjectCommand, GetObjectCommand } = await import("@aws-sdk/client-s3");
      const { getSignedUrl } = await import("@aws-sdk/s3-request-presigner");
      const key = `${prefix}${crypto.randomUUID()}.arrow`;
      const expiresAt = new Date(Date.now() + presignExpiry * 1000);
      const uploadUrl = await getSignedUrl(
        client,
        new PutObjectCommand({ Bucket: bucket, Key: key, ContentLength: size, ChecksumSHA256: checksum }),
        {
          expiresIn: presignExpiry,
          signableHeaders: new Set(["content-length", "x-amz-checksum-sha256"]),
          unhoistableHeaders: new Set(["x-amz-checksum-sha256"]),
        },
      );
      const downloadUrl = await getSignedUrl(client, new GetObjectCommand({ Bucket: bucket, Key: key }), {
        expiresIn: presignExpiry,
      });
      return { uploadUrl, downloadUrl, expiresAt, headers: { "x-amz-checksum-sha256": checksum }, objectId: key };
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
