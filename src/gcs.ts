// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Google Cloud Storage backend for external storage of large Arrow IPC batches.
 *
 * Requires `@google-cloud/storage` as a peer dependency, used only to sign V4
 * URLs (with the service-account key, or IAM `signBlob` under Application
 * Default Credentials). Every transfer is a plain PUT or GET on a signed URL
 * through vgi-rpc's storage HTTP layer, so it has the same stall timeout
 * (`VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS`, no whole-transfer deadline) and
 * retry policy as every other storage transfer.
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
 *   uploadUrlProvider: storage,
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
  discardStorageBody,
  readStorageBody,
  StorageBodyTooLargeError,
  type StorageResponse,
  StorageStallError,
  storageBodyText,
  storageFetch,
  storageStallTimeoutMsFromEnv,
} from "./storage-http.js";
import { pinnedHostsValidator } from "./vendor.js";

/** Configuration for the GCS storage backend. */
export interface GCSStorageConfig {
  /** GCS bucket name. */
  bucket: string;
  /** Key prefix for uploaded objects. Default: "vgi-rpc/". */
  prefix?: string;
  /** Lifetime of signed URLs in seconds. Default: 3600 (1 hour); V4 allows at most 7 days. */
  presignExpirySeconds?: number;
  /** GCS project ID. If omitted, uses Application Default Credentials. */
  projectId?: string;
  /**
   * API endpoint, e.g. an emulator (`http://localhost:4443`) or a private
   * endpoint. Signed URLs and transfers use it. Default: `https://storage.googleapis.com`.
   */
  endpointUrl?: string;
  /** A service-account key file. Default: Application Default Credentials
   *  (`GOOGLE_APPLICATION_CREDENTIALS`, workload identity, metadata server). */
  keyFilename?: string;
  /** Stall timeout, ms (default `VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS`, else 30000). */
  stallTimeoutMs?: number;
  /** Attempts per transfer: transport errors (connect, reset, stall), 429 and 5xx retry (default 3). */
  maxAttempts?: number;
}

/** The GCS storage: results, stored objects and upload grants. */
export interface GCSStorage extends ExternalStorage, ObjectStore, UploadUrlProvider {
  /** A `urlValidator` for inbound pointers: only URLs on this bucket's signed-URL host and path. */
  readonly urlValidator: (url: string) => Promise<void>;
}

const V4_MAX_EXPIRES_SECONDS = 604_800;
const MAX_BACKOFF_MS = 5_000;

function md5Base64(data: Uint8Array): Promise<string> {
  // Node and Bun: node:crypto has MD5 (Web Crypto does not).
  return import("node:crypto").then(({ createHash }) => createHash("md5").update(data).digest("base64"));
}

/**
 * Create a GCS-backed storage: {@link ExternalStorage} (results: signed PUT,
 * then a signed GET URL), {@link ObjectStore} (`StateRef` objects and argument
 * references by object name) and {@link UploadUrlProvider} (a signed PUT whose
 * `x-goog-content-length-range` pins the size; GCS cannot enforce a sha256,
 * so the server verifies the digest when it resolves the upload).
 *
 * Objects carry no TTL and the worker never deletes them: set a bucket
 * lifecycle rule expiring the prefix after 1-2 days.
 */
export function createGCSStorage(config: GCSStorageConfig): GCSStorage {
  const bucketName = config.bucket;
  if (!bucketName) throw new Error("GCS storage needs a bucket");
  const prefix = config.prefix ?? "vgi-rpc/";
  const ttl = Math.min(config.presignExpirySeconds ?? 3600, V4_MAX_EXPIRES_SECONDS);
  if (!Number.isFinite(ttl) || ttl < 1) throw new Error("presignExpirySeconds must be at least 1");
  const stallTimeoutMs =
    config.stallTimeoutMs !== undefined ? checkStallTimeoutMs(config.stallTimeoutMs) : storageStallTimeoutMsFromEnv();
  const maxAttempts = config.maxAttempts ?? 3;
  if (!Number.isInteger(maxAttempts) || maxAttempts < 1) throw new Error("maxAttempts must be >= 1");
  const endpoint = config.endpointUrl?.replace(/\/+$/, "");

  let bucketRef: any = null;
  async function bucket(): Promise<any> {
    if (bucketRef) return bucketRef;
    let Storage: any;
    try {
      ({ Storage } = await import("@google-cloud/storage"));
    } catch {
      throw new Error("GCS storage needs the optional package @google-cloud/storage: install it");
    }
    const options: Record<string, any> = {};
    if (config.projectId) options.projectId = config.projectId;
    if (config.keyFilename) options.keyFilename = config.keyFilename;
    if (endpoint) options.apiEndpoint = endpoint;
    bucketRef = new Storage(options).bucket(bucketName);
    return bucketRef;
  }

  async function sign(
    name: string,
    action: "read" | "write",
    extra: { contentType?: string; contentMd5?: string; extensionHeaders?: Record<string, string> } = {},
  ): Promise<string> {
    const file = (await bucket()).file(name);
    const [url] = await file.getSignedUrl({
      version: "v4",
      action,
      expires: Date.now() + ttl * 1000,
      ...(endpoint ? { host: endpoint } : {}),
      ...extra,
    });
    return url;
  }

  /** One transfer, retrying transport errors, 429 and 5xx with backoff. */
  async function transfer(
    method: "GET" | "PUT",
    url: string,
    headers: Record<string, string>,
    body: Uint8Array | null,
  ): Promise<StorageResponse> {
    let last = "";
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      if (attempt > 0) {
        const delay = Math.min(250 * 2 ** (attempt - 1), MAX_BACKOFF_MS);
        await new Promise((r) => setTimeout(r, delay * (0.5 + Math.random() / 2)));
      }
      let r: StorageResponse;
      try {
        r = await storageFetch(url, { method, headers, body, stallTimeoutMs, extra: { decompress: false } });
      } catch (error) {
        last = error instanceof StorageStallError ? "stalled" : ((error as Error)?.name ?? "Error");
        continue;
      }
      if (r.response.status === 429 || r.response.status >= 500) {
        last = `HTTP ${r.response.status}`;
        await discardStorageBody(r);
        continue;
      }
      return r;
    }
    throw new Error(`GCS ${method} failed after ${maxAttempts} attempts (${last})`);
  }

  async function put(name: string, data: Uint8Array, contentType: string, contentEncoding?: string): Promise<void> {
    const contentMd5 = await md5Base64(data);
    const extensionHeaders: Record<string, string> = {};
    if (contentEncoding) extensionHeaders["content-encoding"] = contentEncoding;
    const url = await sign(name, "write", { contentType, contentMd5, extensionHeaders });
    // GCS verifies the body against Content-MD5.
    const headers: Record<string, string> = {
      "Content-Type": contentType,
      "Content-MD5": contentMd5,
      ...extensionHeaders,
    };
    const r = await transfer("PUT", url, headers, data);
    if (r.response.status < 200 || r.response.status >= 300) {
      const text = await storageBodyText(r);
      const code = /<Code>([^<]{1,64})<\/Code>/.exec(text)?.[1];
      throw new Error(`GCS upload failed: HTTP ${r.response.status}${code ? ` ${code}` : ""}`);
    }
    await discardStorageBody(r);
  }

  let validator: Promise<(url: string) => void> | null = null;

  return {
    async upload(data: Uint8Array, contentEncoding: string): Promise<string> {
      const name = `${prefix}${crypto.randomUUID()}${contentEncoding === "zstd" ? ".arrow.zst" : ".arrow"}`;
      await put(name, data, "application/vnd.apache.arrow.stream", contentEncoding || undefined);
      return sign(name, "read");
    },

    async putObject(data: Uint8Array): Promise<string> {
      const name = `${prefix}obj/${crypto.randomUUID()}`;
      await put(name, data, "application/octet-stream");
      return name;
    },

    async get(objectId: string, options: { maxBytes?: number } = {}): Promise<Uint8Array> {
      const limit = options.maxBytes ?? DEFAULT_MAX_FETCH_BYTES;
      const r = await transfer("GET", await sign(objectId, "read"), {}, null);
      if (r.response.status === 404) {
        await discardStorageBody(r);
        throw new ObjectMissingError("GCS object is gone");
      }
      if (r.response.status < 200 || r.response.status >= 300) {
        await discardStorageBody(r);
        throw new Error(`GCS download failed: HTTP ${r.response.status}`);
      }
      try {
        return await readStorageBody(r, limit);
      } catch (error) {
        if (error instanceof StorageBodyTooLargeError) {
          throw new Error(`GCS object exceeds max_fetch_bytes (${limit})`);
        }
        throw error;
      }
    },

    async generateUploadUrl(size: number, sha256Hex: string): Promise<UploadUrl> {
      if (!Number.isSafeInteger(size) || size < 1) throw new Error(`size must be a positive integer, got ${size}`);
      if (!/^[0-9a-f]{64}$/.test(sha256Hex)) throw new Error("sha256 must be 64 lowercase hex characters");
      const name = `${prefix}${crypto.randomUUID()}.arrow`;
      // GCS refuses a body outside the signed range; the digest is checked by
      // the server when it resolves the upload (GCS has no sha256 to enforce).
      const extensionHeaders = { "x-goog-content-length-range": `${size},${size}` };
      const uploadUrl = await sign(name, "write", { extensionHeaders });
      const downloadUrl = await sign(name, "read");
      return {
        uploadUrl,
        downloadUrl,
        expiresAt: new Date(Date.now() + ttl * 1000),
        headers: extensionHeaders,
        objectId: name,
      };
    },

    async urlValidator(url: string): Promise<void> {
      validator ??= (async () => {
        const probeName = `${prefix}__probe__`;
        const parsed = new URL(await sign(probeName, "read"));
        const base = `${parsed.protocol}//${parsed.host}${parsed.pathname.slice(0, parsed.pathname.length - probeName.length)}`;
        return pinnedHostsValidator([parsed.host], { allowHttp: parsed.protocol === "http:", prefixes: [base] });
      })().catch((error) => {
        validator = null;
        throw error;
      });
      (await validator)(url);
    },
  };
}

/** An environment mapping (`process.env`, or a Cloudflare Worker's `env`). */
type EnvLike = Readonly<Record<string, string | undefined>>;

/** Environment variables of the direct GCS backend (contract v1). */
export const GCS_ENV = {
  BUCKET: "VGI_STORAGE_GCS_BUCKET",
  PREFIX: "VGI_STORAGE_GCS_PREFIX",
  ENDPOINT: "VGI_STORAGE_GCS_ENDPOINT",
  CREDENTIALS_FILE: "VGI_STORAGE_GCS_CREDENTIALS_FILE",
  URL_TTL_SECONDS: "VGI_STORAGE_URL_TTL_SECONDS",
} as const;

/**
 * {@link GCSStorageConfig} from the `VGI_STORAGE_GCS_*` environment (direct
 * storage contract v1): bucket, prefix (default empty), endpoint, credentials
 * file (default Application Default Credentials), URL lifetime.
 *
 * @throws Error if the bucket is unset or a variable will not parse.
 */
export function gcsStorageConfigFromEnv(env?: EnvLike): GCSStorageConfig {
  const e = env ?? (globalThis as { process?: { env?: EnvLike } }).process?.env ?? {};
  const get = (name: string): string | undefined => {
    const v = (e[name] ?? "").trim();
    return v === "" ? undefined : v;
  };
  const bucket = get(GCS_ENV.BUCKET);
  if (!bucket) throw new Error(`${GCS_ENV.BUCKET} is not set`);
  const config: GCSStorageConfig = {
    bucket,
    prefix: get(GCS_ENV.PREFIX) ?? "",
    stallTimeoutMs: storageStallTimeoutMsFromEnv(e),
  };
  const endpoint = get(GCS_ENV.ENDPOINT);
  if (endpoint) {
    let parsed: URL | null = null;
    try {
      parsed = new URL(endpoint);
    } catch {}
    if (!parsed || (parsed.protocol !== "http:" && parsed.protocol !== "https:")) {
      throw new Error(`${GCS_ENV.ENDPOINT} must be an http:// or https:// URL, got ${JSON.stringify(endpoint)}`);
    }
    config.endpointUrl = endpoint;
  }
  const file = get(GCS_ENV.CREDENTIALS_FILE);
  if (file) config.keyFilename = file;
  const ttl = get(GCS_ENV.URL_TTL_SECONDS);
  if (ttl !== undefined) {
    if (!/^\d+$/.test(ttl) || Number(ttl) < 1) {
      throw new Error(`${GCS_ENV.URL_TTL_SECONDS} must be a positive integer, got ${JSON.stringify(ttl)}`);
    }
    config.presignExpirySeconds = Number(ttl);
  }
  return config;
}
