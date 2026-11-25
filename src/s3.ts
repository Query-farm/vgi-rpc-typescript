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
import { pinnedHostsValidator } from "./vendor.js";

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
  /**
   * Lifetime of pre-signed URLs in seconds. Default: 3600 (1 hour). Capped at
   * the signing credentials' expiry minus 60 s (a URL dies with the
   * credentials that signed it) and at SigV4's 7 days.
   */
  presignExpirySeconds?: number;
  /** AWS region. If omitted, uses default SDK config (`AWS_REGION`, the
   *  profile). S3-compatible stores take any name, e.g. `auto` for R2. */
  region?: string;
  /**
   * Credentials: an `AwsCredentialIdentity` (`{ accessKeyId, secretAccessKey,
   * sessionToken? }`) or a provider function. If omitted, the SDK's default
   * chain (environment, shared profile incl. `role_arn` / `credential_process`,
   * SSO, web identity, ECS/EKS container, EC2 instance role), which refreshes
   * temporary credentials before they expire.
   */
  credentials?: unknown;
  /** Custom S3 endpoint URL (MinIO, Ceph, R2's S3 API, Wasabi, B2,
   *  LocalStack), `https://` or `http://`. Unset: AWS. */
  endpointUrl?: string;
  /**
   * Endpoint used only for the pre-signed URLs handed to clients, when the
   * worker reaches storage at a different address than clients do (e.g.
   * Docker-internal `http://minio:9000` vs public `https://s3.example.com`).
   * Default: `endpointUrl`.
   */
  publicEndpointUrl?: string;
  /** Force path-style addressing (required for some S3-compatible services).
   *  Default: `true` when `endpointUrl` is set, else the SDK's default
   *  (virtual-hosted). */
  forcePathStyle?: boolean;
  /**
   * `sha256` (default): upload grants sign `x-amz-checksum-sha256`, so storage
   * refuses any other body, and stored objects carry it. `none`, for services
   * that do not enforce `x-amz-checksum-sha256`: no upload grants (the storage
   * is not an {@link UploadUrlProvider}; argument refs and large requests stay
   * inline) and objects are stored without it; results and objects are still
   * externalized, and readers verify their sha256 themselves.
   */
  checksum?: "sha256" | "none";
  /**
   * The AWS SDK's own request checksums (CRC32 by default since SDK 3.729):
   * `when_supported` sends them on every PutObject, `when_required` only where
   * an operation requires one. Some S3-compatible services reject the newer
   * checksum headers, so the default is `when_required` with `endpointUrl`,
   * `when_supported` for AWS. Explicit `x-amz-checksum-sha256` is unaffected.
   */
  sdkChecksum?: "when_supported" | "when_required";
  /**
   * Stall timeout, ms: a request fails (and the SDK retries it) only when no
   * bytes move for this long (connecting counts as one window). There is no
   * whole-request deadline, so a large object on a slow link completes.
   * Default: `VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS`, else 30000.
   */
  stallTimeoutMs?: number;
  /** HEAD the bucket before the first request or signature (default true). */
  verifyOnFirstUse?: boolean;
  /** Warnings (e.g. a URL signed for less than the grant minimum because the
   *  credentials expire sooner). Default `console.warn`. */
  log?: (message: string) => void;
}

/** The S3 storage: results, stored objects and (unless `checksum: "none"`)
 *  upload grants. */
export interface S3Storage extends ExternalStorage, ObjectStore {
  /** The checksum mode it was built with. */
  readonly checksum: "sha256" | "none";
  /**
   * HEAD the bucket. Runs once on first use anyway; call it at startup to fail
   * fast. Rejects with an actionable message: the bucket is missing, access is
   * denied, or the bucket is in another region (naming the region the service
   * reported).
   */
  verify(): Promise<void>;
  /**
   * A `urlValidator` for inbound pointers: accepts only URLs on this bucket's
   * pre-signed URL host (the public endpoint) and under its bucket path.
   */
  readonly urlValidator: (url: string) => Promise<void>;
  /** Upload grants; absent with `checksum: "none"`. */
  generateUploadUrl?: UploadUrlProvider["generateUploadUrl"];
  /** Delete one object, best effort: failures are ignored (the bucket
   *  lifecycle rule is the real clean-up). */
  deleteObject(objectId: string): Promise<void>;
}

/** The shortest URL lifetime an upload grant needs, seconds: below it the
 *  credentials are refreshed before signing. */
export const UPLOAD_GRANT_MIN_TTL_SECONDS = 900;
const SIGV4_MAX_EXPIRES_SECONDS = 604_800;

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
 * The optional AWS SDK packages, imported on first use.
 *
 * Every dynamic import of them lives here, awaited inside a `try`. Bun's
 * bundler (like esbuild) resolves a string-literal `import()` at build time
 * and fails the build when the package is not installed, unless the import is
 * awaited inside a `try` (or has a `.catch`). Guarded, `bun build --compile`
 * bundles the SDK when it is installed and otherwise leaves a runtime
 * `import()` that fails here with a clear message. One unguarded literal
 * import of an AWS package anywhere in this module breaks `bun build` for
 * every consumer without the optional peers, even one that never selects S3
 * (scripts/check-compile-optional-peers.sh guards this).
 */
let awsSdk: Promise<{ s3: any; presigner: any }> | null = null;
function loadAwsSdk(): Promise<{ s3: any; presigner: any }> {
  awsSdk ??= (async () => {
    try {
      const s3 = await import("@aws-sdk/client-s3");
      const presigner = await import("@aws-sdk/s3-request-presigner");
      return { s3, presigner };
    } catch {
      throw new Error(
        "S3 storage needs the optional packages @aws-sdk/client-s3 and @aws-sdk/s3-request-presigner: install them",
      );
    }
  })().catch((error) => {
    awsSdk = null;
    throw error;
  });
  return awsSdk;
}

/**
 * Create an S3-backed storage: an {@link ExternalStorage} (externalized
 * results: PUT + pre-signed GET), an {@link ObjectStore} (stored `StateRef`
 * objects and argument references, keyed by object key) and, unless
 * `checksum: "none"`, an {@link UploadUrlProvider} (pre-signed client upload
 * URLs bound to size and sha256, so a server with this storage accepts
 * requests over its request cap and argument references).
 *
 * Lazily imports `@aws-sdk/client-s3` and `@aws-sdk/s3-request-presigner`
 * (optional peers, see `loadAwsSdk`) on first use to avoid loading the AWS
 * SDK unless needed. The first use also HEADs the bucket ({@link S3Storage.verify}).
 *
 * Objects carry no TTL and the worker never deletes them: set a bucket
 * lifecycle rule expiring the prefix after 1-2 days.
 */
export function createS3Storage(config: S3StorageConfig & { checksum: "none" }): S3Storage;
export function createS3Storage(config: S3StorageConfig): S3Storage & UploadUrlProvider;
export function createS3Storage(config: S3StorageConfig): S3Storage {
  const bucket = config.bucket;
  if (!bucket) throw new Error("S3 storage needs a bucket");
  const prefix = config.prefix ?? "vgi-rpc/";
  const urlTtl = Math.min(config.presignExpirySeconds ?? 3600, SIGV4_MAX_EXPIRES_SECONDS);
  if (!Number.isFinite(urlTtl) || urlTtl < 1) throw new Error("presignExpirySeconds must be at least 1");
  const checksum = config.checksum ?? "sha256";
  if (checksum !== "sha256" && checksum !== "none") throw new Error(`checksum must be sha256 or none, got ${checksum}`);
  const sdkChecksum = config.sdkChecksum ?? (config.endpointUrl ? "when_required" : "when_supported");
  if (sdkChecksum !== "when_supported" && sdkChecksum !== "when_required") {
    throw new Error(`sdkChecksum must be when_supported or when_required, got ${sdkChecksum}`);
  }
  const stallTimeoutMs =
    config.stallTimeoutMs !== undefined ? checkStallTimeoutMs(config.stallTimeoutMs) : storageStallTimeoutMsFromEnv();
  const pathStyle = config.forcePathStyle ?? (config.endpointUrl ? true : undefined);

  // Lazy-loaded AWS SDK clients: one for requests, one (sharing its
  // credential provider) for the URLs handed out, on the public endpoint.
  let clients: Promise<{ client: any; presigner: any }> | null = null;
  let verified: Promise<void> | null = null;

  function clientConfig(endpoint: string | undefined): Record<string, any> {
    const c: Record<string, any> = {
      requestHandler: stallTimeoutRequestHandler(stallTimeoutMs),
      requestChecksumCalculation: sdkChecksum === "when_required" ? "WHEN_REQUIRED" : "WHEN_SUPPORTED",
      responseChecksumValidation: sdkChecksum === "when_required" ? "WHEN_REQUIRED" : "WHEN_SUPPORTED",
    };
    if (config.region) c.region = config.region;
    if (endpoint) c.endpoint = endpoint;
    if (pathStyle !== undefined) c.forcePathStyle = pathStyle;
    return c;
  }

  function ensureClients(): Promise<{ client: any; presigner: any }> {
    clients ??= (async () => {
      const { S3Client } = (await loadAwsSdk()).s3;
      const base = clientConfig(config.endpointUrl);
      if (config.credentials) base.credentials = config.credentials;
      const client = new S3Client(base);
      if (!config.publicEndpointUrl || config.publicEndpointUrl === config.endpointUrl) {
        return { client, presigner: client };
      }
      const pub = clientConfig(config.publicEndpointUrl);
      // The same (memoized, refreshing) credential provider signs both.
      pub.credentials = client.config.credentials;
      return { client, presigner: new S3Client(pub) };
    })().catch((error) => {
      clients = null;
      throw error;
    });
    return clients;
  }

  async function headBucket(client: any): Promise<void> {
    const { HeadBucketCommand } = (await loadAwsSdk()).s3;
    const where = config.endpointUrl ? ` at ${config.endpointUrl}` : "";
    try {
      await client.send(new HeadBucketCommand({ Bucket: bucket }));
    } catch (error: any) {
      const status = error?.$metadata?.httpStatusCode;
      const headers = error?.$response?.headers ?? {};
      const actual = headers["x-amz-bucket-region"] ?? headers["X-Amz-Bucket-Region"];
      const configured = (await client.config.region?.()) ?? config.region ?? "(SDK default)";
      if (actual && actual !== configured) {
        throw new Error(
          `S3 bucket '${bucket}'${where} is in region ${actual}, but the storage is configured for ${configured} (set VGI_STORAGE_S3_REGION=${actual})`,
        );
      }
      if (status === 404 || error?.name === "NotFound" || error?.name === "NoSuchBucket") {
        throw new Error(`S3 bucket '${bucket}' does not exist${where}`);
      }
      if (status === 403 || error?.name === "Forbidden" || error?.name === "AccessDenied") {
        throw new Error(
          `S3 access denied to bucket '${bucket}'${where}: check the credentials and the bucket policy (s3:ListBucket, s3:GetObject, s3:PutObject on the prefix)`,
        );
      }
      if (status === 301 || status === 400) {
        throw new Error(
          `S3 bucket '${bucket}'${where} refused the request (HTTP ${status}); the configured region ${configured} is likely wrong`,
        );
      }
      throw new Error(
        `S3 bucket '${bucket}'${where} check failed (${error?.name ?? "Error"}${status ? ` HTTP ${status}` : ""})`,
      );
    }
  }

  async function ready(): Promise<{ client: any; presigner: any }> {
    const c = await ensureClients();
    if (config.verifyOnFirstUse === false) return c;
    verified ??= headBucket(c.client).catch((error) => {
      verified = null; // check again on the next use
      throw error;
    });
    await verified;
    return c;
  }

  /** A URL lifetime no longer than the signing credentials last (minus 60 s);
   *  refresh them first when that leaves less than `minSeconds`. */
  async function signingTtl(presigner: any, minSeconds: number): Promise<number> {
    const provider = presigner.config.credentials;
    let identity = await provider();
    const remaining = (id: any): number =>
      id?.expiration instanceof Date ? (id.expiration.getTime() - Date.now()) / 1000 - 60 : Number.POSITIVE_INFINITY;
    if (remaining(identity) < minSeconds) {
      // Refresh once; a session shorter than the minimum (e.g. a 900 s STS
      // session) then signs for what it has left, with a warning.
      identity = await provider({ forceRefresh: true });
      if (remaining(identity) < minSeconds) {
        (config.log ?? console.warn)(
          `S3 credentials expire in ${Math.max(0, Math.floor(remaining(identity)))} s (after the 60 s margin), less than the ${minSeconds} s this URL should live; signing for the remaining lifetime`,
        );
      }
    }
    const ttl = Math.floor(Math.min(urlTtl, remaining(identity)));
    if (ttl < 1) throw new Error("S3 credentials expire too soon to sign a URL");
    return ttl;
  }

  async function presignGet(presigner: any, key: string, minSeconds = 0): Promise<{ url: string; ttl: number }> {
    const sdk = await loadAwsSdk();
    const { GetObjectCommand } = sdk.s3;
    const { getSignedUrl } = sdk.presigner;
    const ttl = await signingTtl(presigner, minSeconds);
    const url = await getSignedUrl(presigner, new GetObjectCommand({ Bucket: bucket, Key: key }), { expiresIn: ttl });
    return { url, ttl };
  }

  let validator: Promise<(url: string) => void> | null = null;
  async function urlValidator(url: string): Promise<void> {
    validator ??= (async () => {
      // The host and bucket path of a URL this storage signs (path-style or
      // virtual-hosted), from a probe signature: no request is made.
      const { presigner } = await ensureClients();
      const { url: probe } = await presignGet(presigner, `${prefix}__probe__`);
      const parsed = new URL(probe);
      const base = `${parsed.protocol}//${parsed.host}${parsed.pathname.slice(0, parsed.pathname.length - `${prefix}__probe__`.length)}`;
      return pinnedHostsValidator([parsed.host], { allowHttp: parsed.protocol === "http:", prefixes: [base] });
    })().catch((error) => {
      validator = null;
      throw error;
    });
    (await validator)(url);
  }

  const storage: S3Storage = {
    checksum,
    verify: async () => {
      const c = await ensureClients();
      verified ??= headBucket(c.client).catch((error) => {
        verified = null;
        throw error;
      });
      await verified;
    },
    urlValidator,

    async upload(data: Uint8Array, contentEncoding: string): Promise<string> {
      const { client, presigner } = await ready();
      const { PutObjectCommand } = (await loadAwsSdk()).s3;
      const key = `${prefix}${crypto.randomUUID()}${contentEncoding === "zstd" ? ".arrow.zst" : ".arrow"}`;
      await client.send(
        new PutObjectCommand({
          Bucket: bucket,
          Key: key,
          Body: data,
          ContentLength: data.byteLength,
          ContentType: "application/vnd.apache.arrow.stream",
          ...(contentEncoding ? { ContentEncoding: contentEncoding } : {}),
          ...(checksum === "sha256" ? { ChecksumSHA256: await sha256Base64(data) } : {}),
        }),
      );
      return (await presignGet(presigner, key)).url;
    },

    async putObject(data: Uint8Array): Promise<string> {
      const { client } = await ready();
      const { PutObjectCommand } = (await loadAwsSdk()).s3;
      const key = `${prefix}obj/${crypto.randomUUID()}`;
      // With `sha256` storage verifies the body it stores; with `none` the
      // reader verifies size and digest on fetch.
      await client.send(
        new PutObjectCommand({
          Bucket: bucket,
          Key: key,
          Body: data,
          ContentLength: data.byteLength,
          ContentType: "application/octet-stream",
          ...(checksum === "sha256" ? { ChecksumSHA256: await sha256Base64(data) } : {}),
        }),
      );
      return key;
    },

    async deleteObject(objectId: string): Promise<void> {
      try {
        const { client } = await ready();
        const { DeleteObjectCommand } = (await loadAwsSdk()).s3;
        await client.send(new DeleteObjectCommand({ Bucket: bucket, Key: objectId }));
      } catch {
        // Best effort: the lifecycle rule removes it anyway.
      }
    },

    async get(objectId: string, options: { maxBytes?: number } = {}): Promise<Uint8Array> {
      const limit = options.maxBytes ?? DEFAULT_MAX_FETCH_BYTES;
      const { client } = await ready();
      const { GetObjectCommand } = (await loadAwsSdk()).s3;
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
        await response.Body?.destroy?.();
        throw new Error(`S3 object is ${response.ContentLength} bytes, over max_fetch_bytes (${limit})`);
      }
      const bytes: Uint8Array = await response.Body.transformToByteArray();
      if (bytes.byteLength > limit) throw new Error(`S3 object exceeds max_fetch_bytes (${limit})`);
      return bytes;
    },
  };

  if (checksum === "sha256") {
    /**
     * Pre-signed PUT and GET URLs for one sized client upload (upload URLs v2).
     *
     * The PUT URL signs `Content-Length` and `x-amz-checksum-sha256` (SigV4),
     * so S3 refuses any body that is not exactly `size` bytes hashing to
     * `sha256Hex` (`BadDigest`). The checksum stays a signed header (not a
     * query parameter): the client sends it from {@link UploadUrl.headers}.
     * Both URLs live at least 15 minutes (credentials are refreshed first if
     * they would expire sooner) and at most the configured lifetime.
     */
    storage.generateUploadUrl = async (size: number, sha256Hex: string): Promise<UploadUrl> => {
      if (!Number.isSafeInteger(size) || size < 1) throw new Error(`size must be a positive integer, got ${size}`);
      const sha = sha256Base64FromHex(sha256Hex);
      const { presigner } = await ready();
      const sdk = await loadAwsSdk();
      const { PutObjectCommand } = sdk.s3;
      const { getSignedUrl } = sdk.presigner;
      const key = `${prefix}${crypto.randomUUID()}.arrow`;
      const ttl = await signingTtl(presigner, Math.min(UPLOAD_GRANT_MIN_TTL_SECONDS, urlTtl));
      const uploadUrl = await getSignedUrl(
        presigner,
        new PutObjectCommand({ Bucket: bucket, Key: key, ContentLength: size, ChecksumSHA256: sha }),
        {
          expiresIn: ttl,
          signableHeaders: new Set(["content-length", "x-amz-checksum-sha256"]),
          unhoistableHeaders: new Set(["x-amz-checksum-sha256"]),
        },
      );
      const { url: downloadUrl } = await presignGet(presigner, key, Math.min(UPLOAD_GRANT_MIN_TTL_SECONDS, urlTtl));
      const expiresAt = new Date(Date.now() + ttl * 1000);
      return { uploadUrl, downloadUrl, expiresAt, headers: { "x-amz-checksum-sha256": sha }, objectId: key };
    };
  }
  return storage;
}

/** An environment mapping (`process.env`, or a Cloudflare Worker's `env`). */
type EnvLike = Readonly<Record<string, string | undefined>>;

/** Environment variables of the direct S3 backend (contract v1, same names in every SDK). */
export const S3_ENV = {
  BUCKET: "VGI_STORAGE_S3_BUCKET",
  PREFIX: "VGI_STORAGE_S3_PREFIX",
  ENDPOINT: "VGI_STORAGE_S3_ENDPOINT",
  PUBLIC_ENDPOINT: "VGI_STORAGE_S3_PUBLIC_ENDPOINT",
  REGION: "VGI_STORAGE_S3_REGION",
  FORCE_PATH_STYLE: "VGI_STORAGE_S3_FORCE_PATH_STYLE",
  CHECKSUM: "VGI_STORAGE_S3_CHECKSUM",
  SDK_CHECKSUM: "VGI_STORAGE_S3_SDK_CHECKSUM",
  ACCESS_KEY_ID: "VGI_STORAGE_S3_ACCESS_KEY_ID",
  SECRET_ACCESS_KEY: "VGI_STORAGE_S3_SECRET_ACCESS_KEY",
  SESSION_TOKEN: "VGI_STORAGE_S3_SESSION_TOKEN",
  SESSION_EXPIRES: "VGI_STORAGE_S3_SESSION_EXPIRES",
  VERIFY: "VGI_STORAGE_S3_VERIFY",
  URL_TTL_SECONDS: "VGI_STORAGE_URL_TTL_SECONDS",
  ALLOW_PLAIN_HTTP: "VGI_STORAGE_ALLOW_PLAIN_HTTP",
} as const;

/** Which half of the bucket layout a store writes: `rpc` (results, upload
 *  grants, StateRef and argument objects; lifecycle 1 day) or `fs`
 *  (function storage; lifecycle 2 days). */
export type StorageRole = "rpc" | "fs";

/** Parse `VGI_STORAGE_S3_SESSION_EXPIRES`: RFC 3339 or epoch seconds. */
export function parseSessionExpires(raw: string, name: string = S3_ENV.SESSION_EXPIRES): Date {
  const text = raw.trim();
  const date = /^\d+(\.\d+)?$/.test(text) ? new Date(Number(text) * 1000) : new Date(text);
  if (Number.isNaN(date.getTime())) {
    throw new Error(`${name} must be an RFC 3339 timestamp or epoch seconds, got ${JSON.stringify(raw)}`);
  }
  return date;
}

/** Plain `http://` storage is refused unless `VGI_STORAGE_ALLOW_PLAIN_HTTP=1`. */
export function checkPlainHttp(env: EnvLike, name: string, value: string | undefined): void {
  if (value === undefined || !value.toLowerCase().startsWith("http:")) return;
  const allow = (env[S3_ENV.ALLOW_PLAIN_HTTP] ?? "").trim().toLowerCase();
  if (!["1", "true", "yes", "on"].includes(allow)) {
    throw new Error(
      `${name} is plain http (${value}); set ${S3_ENV.ALLOW_PLAIN_HTTP}=1 to allow it (test and private networks only)`,
    );
  }
}

function envValue(env: EnvLike, name: string): string | undefined {
  const v = (env[name] ?? "").trim();
  return v === "" ? undefined : v;
}

function envBool(env: EnvLike, name: string): boolean | undefined {
  const v = envValue(env, name)?.toLowerCase();
  if (v === undefined) return undefined;
  if (["1", "true", "yes", "on"].includes(v)) return true;
  if (["0", "false", "no", "off"].includes(v)) return false;
  throw new Error(`${name} must be true or false, got ${JSON.stringify(v)}`);
}

function checkEndpoint(name: string, value: string | undefined): string | undefined {
  if (value === undefined) return undefined;
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error(`${name} must be an http:// or https:// URL, got ${JSON.stringify(value)}`);
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw new Error(`${name} must be an http:// or https:// URL, got ${JSON.stringify(value)}`);
  }
  return value.replace(/\/+$/, "");
}

/**
 * {@link S3StorageConfig} from the `VGI_STORAGE_S3_*` environment (direct
 * storage contract v1). Credentials: `VGI_STORAGE_S3_ACCESS_KEY_ID` /
 * `_SECRET_ACCESS_KEY` (+ optional `_SESSION_TOKEN`) when set, else the AWS
 * SDK default chain. Region: `VGI_STORAGE_S3_REGION`, `AWS_REGION`,
 * `AWS_DEFAULT_REGION`, else `us-east-1`. Prefix: `VGI_STORAGE_S3_PREFIX`
 * (default empty). Never includes a secret in an error message.
 *
 * @throws Error if the bucket is unset or a variable will not parse.
 */
export function s3StorageConfigFromEnv(
  env?: EnvLike,
  options: { role?: StorageRole; log?: (message: string) => void } = {},
): S3StorageConfig {
  const e = env ?? (globalThis as { process?: { env?: EnvLike } }).process?.env ?? {};
  const log = options.log ?? console.warn;
  const bucket = envValue(e, S3_ENV.BUCKET);
  if (!bucket) throw new Error(`${S3_ENV.BUCKET} is not set`);
  const config: S3StorageConfig = {
    bucket,
    prefix: `${envValue(e, S3_ENV.PREFIX) ?? ""}${options.role ?? "rpc"}/`,
    log,
    region: envValue(e, S3_ENV.REGION) ?? envValue(e, "AWS_REGION") ?? envValue(e, "AWS_DEFAULT_REGION") ?? "us-east-1",
    stallTimeoutMs: storageStallTimeoutMsFromEnv(e),
  };
  const endpoint = checkEndpoint(S3_ENV.ENDPOINT, envValue(e, S3_ENV.ENDPOINT));
  checkPlainHttp(e, S3_ENV.ENDPOINT, endpoint);
  if (endpoint) config.endpointUrl = endpoint;
  const pub = checkEndpoint(S3_ENV.PUBLIC_ENDPOINT, envValue(e, S3_ENV.PUBLIC_ENDPOINT));
  checkPlainHttp(e, S3_ENV.PUBLIC_ENDPOINT, pub);
  if (pub) config.publicEndpointUrl = pub;
  const pathStyle = envBool(e, S3_ENV.FORCE_PATH_STYLE);
  if (pathStyle !== undefined) config.forcePathStyle = pathStyle;
  const checksum = envValue(e, S3_ENV.CHECKSUM)?.toLowerCase();
  if (checksum !== undefined) {
    if (checksum !== "sha256" && checksum !== "none") {
      throw new Error(`${S3_ENV.CHECKSUM} must be sha256 or none, got ${JSON.stringify(checksum)}`);
    }
    config.checksum = checksum;
  }
  const sdk = envValue(e, S3_ENV.SDK_CHECKSUM)?.toLowerCase();
  if (sdk !== undefined) {
    if (sdk !== "when_supported" && sdk !== "when_required") {
      throw new Error(`${S3_ENV.SDK_CHECKSUM} must be when_supported or when_required, got ${JSON.stringify(sdk)}`);
    }
    config.sdkChecksum = sdk;
  }
  const ttl = envValue(e, S3_ENV.URL_TTL_SECONDS);
  if (ttl !== undefined) {
    if (!/^\d+$/.test(ttl) || Number(ttl) < 1) {
      throw new Error(`${S3_ENV.URL_TTL_SECONDS} must be a positive integer, got ${JSON.stringify(ttl)}`);
    }
    config.presignExpirySeconds = Number(ttl);
  }
  const keyId = envValue(e, S3_ENV.ACCESS_KEY_ID);
  const secret = envValue(e, S3_ENV.SECRET_ACCESS_KEY);
  const token = envValue(e, S3_ENV.SESSION_TOKEN);
  if ((keyId === undefined) !== (secret === undefined)) {
    throw new Error(`${S3_ENV.ACCESS_KEY_ID} and ${S3_ENV.SECRET_ACCESS_KEY} must be set together`);
  }
  if (token !== undefined && keyId === undefined) {
    throw new Error(`${S3_ENV.SESSION_TOKEN} needs ${S3_ENV.ACCESS_KEY_ID} and ${S3_ENV.SECRET_ACCESS_KEY}`);
  }
  const expiresRaw = envValue(e, S3_ENV.SESSION_EXPIRES);
  if (expiresRaw !== undefined && token === undefined) {
    throw new Error(`${S3_ENV.SESSION_EXPIRES} applies to ${S3_ENV.SESSION_TOKEN}, which is not set`);
  }
  if (keyId !== undefined) {
    const identity: Record<string, unknown> = {
      accessKeyId: keyId,
      secretAccessKey: secret,
      ...(token ? { sessionToken: token } : {}),
    };
    if (expiresRaw !== undefined) identity.expiration = parseSessionExpires(expiresRaw);
    else if (token !== undefined) {
      log(
        `${S3_ENV.SESSION_TOKEN} is set without ${S3_ENV.SESSION_EXPIRES}: URL lifetimes cannot be capped at the ` +
          "session's expiry, so URLs may stop working early; set it, or use credential_process / the standard AWS chain",
      );
    }
    // A provider, so the expiry reaches the URL-lifetime cap.
    config.credentials = async () => identity;
  }
  return config;
}
