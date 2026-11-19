// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Blob-vendor client: region-pinned, single-object pre-signed URLs.
 *
 * The *blob vendor* is a small HTTP service that holds the object-storage
 * credentials so workers never do. A worker asks it for a pre-signed URL for
 * one object of a declared size and SHA-256, uploads straight to storage with
 * that URL, and hands a download URL (or an object id) to whoever needs the
 * bytes. The storage enforces the signed `Content-Length` and
 * `x-amz-checksum-sha256`, so whatever sits at the object's key is exactly the
 * declared bytes.
 *
 * Port of vgi-rpc-python's `vgi_rpc/vendor.py`; the wire format (the vendor's
 * JSON shapes) and the environment variables are the same.
 *
 * - {@link VendorStorage} implements both {@link ExternalStorage} (server-side
 *   externalization of large results, purpose `rpc`) and
 *   {@link UploadUrlProvider} (vending upload URLs to clients for large
 *   requests).
 * - {@link VendorBlobClient} puts, gets and deletes raw byte blobs (purpose
 *   `fs`), for callers that keep their own references, e.g. a key-value store
 *   that externalizes large values. Reads are verified against the caller's
 *   size and SHA-256.
 * - {@link pinnedHostsValidator} and {@link vendorUrlValidator} accept only
 *   URLs on the vendor's storage hosts.
 * - {@link externalConfigFromEnv} / {@link clientExternalConfigFromEnv} build
 *   environment-driven configs for hosted deployments.
 *
 * Runtime-agnostic: only `fetch`, `URL` and Web Crypto, so it runs on Bun,
 * Node, Deno and Cloudflare Workers (pass the Worker's `env` bindings as the
 * `env` argument of the `*FromEnv` helpers).
 *
 * Every vendor request carries `Authorization: Bearer <token>`. Requests to
 * storage hosts (the signed URLs) are made with the global `fetch` and exactly
 * the headers the grant requires: never the token, never a caller-supplied
 * fetch, no redirects. Neither the token nor a signed query string ever
 * appears in an error message: URLs are reported through
 * {@link redactExternalUrl}, which drops the query, fragment and userinfo.
 *
 * Environment variables:
 *
 * | Variable | Meaning |
 * |---|---|
 * | `VGI_BLOB_VENDOR_URL` | Vendor base URL. Unset: the `*FromEnv` helpers return `null`. |
 * | `VGI_BLOB_VENDOR_TOKEN` | Bearer token; falls back to `VGI_CF_DO_TOKEN`. |
 * | `VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES` | Result-batch size from which the server externalizes (default 1 MiB). |
 * | `VGI_RPC_EXTERNAL_COMPRESSION` | `none` (default) or `zstd`, optionally `zstd:<level>`. |
 * | `VGI_RPC_MAX_UPLOAD_BYTES` | Optional size advertised to clients for vendor upload URLs. |
 *
 * @module
 */

import type { ExternalLocationConfig, ExternalStorage, UploadUrl, UploadUrlProvider } from "./external.js";
import { redactExternalUrl } from "./external.js";

/** Environment variable naming the vendor's base URL. */
export const ENV_URL = "VGI_BLOB_VENDOR_URL";
/** Environment variable holding the vendor's bearer token. */
export const ENV_TOKEN = "VGI_BLOB_VENDOR_TOKEN";
/** Token fallback: the storage service's per-deployment key. */
export const ENV_TOKEN_FALLBACK = "VGI_CF_DO_TOKEN";
/** Environment variable: result-batch size from which the server externalizes. */
export const ENV_THRESHOLD = "VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES";
/** Environment variable: compression for externalized results (`none` / `zstd[:level]`). */
export const ENV_COMPRESSION = "VGI_RPC_EXTERNAL_COMPRESSION";
/** Environment variable: the client-upload size the server advertises. */
export const ENV_MAX_UPLOAD_BYTES = "VGI_RPC_MAX_UPLOAD_BYTES";

const DEFAULT_THRESHOLD = 1_048_576;
const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_INFO_TTL_MS = 600_000;
const MAX_BACKOFF_MS = 5_000;
/** Default cap on a blob {@link VendorBlobClient.getBytes} will download. */
const DEFAULT_MAX_FETCH_BYTES = 256 * 1024 * 1024;

/** Object purpose: `rpc` objects get a download URL at `put` and a short
 *  lifecycle; `fs` objects (function storage) are fetched by id. */
export type VendorPurpose = "rpc" | "fs";

/** An environment mapping: `process.env`, or a Cloudflare Worker's `env`. */
export type EnvLike = Readonly<Record<string, string | undefined>>;

/**
 * A blob-vendor or signed-URL request failed.
 *
 * The message never contains the bearer token or a signed query string.
 */
export class VendorError extends Error {
  /** HTTP status of the failing response; `null` for a transport failure or a
   *  malformed response. */
  readonly status: number | null;
  /**
   * Machine-readable code: the vendor's `error` (`not_found`, `not_uploaded`,
   * `quota_exceeded`, `idempotency_conflict`, ...), the storage's XML `<Code>`
   * (`BadDigest`, `NoSuchKey`, ...), or, for checks made by
   * {@link VendorBlobClient.getBytes} itself, `size_mismatch` /
   * `sha256_mismatch`. `null` when there was none.
   */
  readonly code: string | null;

  constructor(message: string, status: number | null = null, code: string | null = null) {
    super(message);
    this.name = "VendorError";
    this.status = status;
    this.code = code;
  }
}

// ---------------------------------------------------------------------------
// Value types
// ---------------------------------------------------------------------------

/** The execution a blob belongs to, for scoped clean-up. The vendor accepts
 *  1-128 characters of `[A-Za-z0-9._:-]` in each field, so encode binary ids
 *  as hex. */
export interface BlobScope {
  /** The storage shard key. */
  readonly shard: string;
  /** The execution identifier (e.g. hex of the raw id bytes). */
  readonly executionId: string;
}

/** A reference to one vendored blob, small enough to store inline. */
export interface BlobRef {
  /** The vendor's object id. */
  readonly objectId: string;
  /** Exact byte length of the blob. */
  readonly size: number;
  /** Lowercase hex SHA-256 of the blob bytes. */
  readonly sha256: string;
}

/** The vendor's answer to `put`: where and how to upload one object. */
export interface PutGrant {
  /** The server-chosen object id. */
  readonly objectId: string;
  /** Pre-signed PUT URL. */
  readonly uploadUrl: string;
  /** Headers the PUT must carry exactly as given (the signature covers them). */
  readonly requiredHeaders: Readonly<Record<string, string>>;
  /** Pre-signed GET URL, when the vendor returns one (only for `rpc`). */
  readonly downloadUrl: string | null;
  /** When `uploadUrl` stops working. */
  readonly expiresAt: Date;
  /** The region the object lives in. */
  readonly region: string | null;
  /** When `downloadUrl` stops working, if given. */
  readonly downloadExpiresAt: Date | null;
}

/** The vendor's answer to `get`: where one object can be downloaded. */
export interface BlobLocation {
  /** Pre-signed GET URL. */
  readonly downloadUrl: string;
  /** Declared byte length. */
  readonly size: number;
  /** Declared SHA-256, standard base64. */
  readonly sha256B64: string;
  /** `Content-Encoding` stored with the object. */
  readonly contentEncoding: string | null;
  /** When `downloadUrl` stops working. */
  readonly expiresAt: Date;
}

/** The vendor's `/v1/info`: storage hosts for this tenant's region. */
export interface VendorInfo {
  /** The tenant's region name. */
  readonly region: string;
  /** Hosts upload URLs may point at. */
  readonly uploadHosts: readonly string[];
  /** Hosts download URLs may point at. */
  readonly downloadHosts: readonly string[];
  /** The tenant the token resolves to. */
  readonly tenant: string;
  /** The R2 jurisdiction (`eu`, `us`), or `null`. */
  readonly jurisdiction: string | null;
  /** URL prefixes this tenant's objects live under. */
  readonly urlPrefixes: readonly string[];
  /** Numeric limits (`max_object_bytes`, `quota_bytes`, ...). */
  readonly limits: Readonly<Record<string, number>>;
  /** URL lifetimes in seconds (`put`, `rpc_download`, `get`). */
  readonly ttlSeconds: Readonly<Record<string, number>>;
}

/** A `urlValidator`: throws (or rejects) for a disallowed URL. */
export type UrlValidator = (url: string) => void | Promise<void>;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function toArrayBuffer(data: Uint8Array): ArrayBuffer {
  if (data.byteOffset === 0 && data.byteLength === data.buffer.byteLength && data.buffer instanceof ArrayBuffer) {
    return data.buffer;
  }
  const buf = new ArrayBuffer(data.byteLength);
  new Uint8Array(buf).set(data);
  return buf;
}

/** SHA-256 digest of `data` (Web Crypto). */
export async function sha256Bytes(data: Uint8Array): Promise<Uint8Array> {
  return new Uint8Array(await crypto.subtle.digest("SHA-256", toArrayBuffer(data)));
}

function toHex(bytes: Uint8Array): string {
  let out = "";
  for (let i = 0; i < bytes.length; i++) out += bytes[i]!.toString(16).padStart(2, "0");
  return out;
}

function toBase64(bytes: Uint8Array): string {
  let binary = "";
  for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]!);
  return btoa(binary);
}

function parseExpires(value: unknown): Date {
  if (typeof value === "number" && Number.isFinite(value)) {
    return new Date(value > 1e11 ? value : value * 1000);
  }
  if (typeof value === "string") {
    const ms = Date.parse(value);
    if (!Number.isNaN(ms)) return new Date(ms);
  }
  throw new VendorError("Blob vendor returned a malformed expires_at");
}

type Json = Record<string, unknown>;

function field(obj: Json, key: string, op: string): unknown {
  if (!(key in obj)) throw new VendorError(`Blob vendor ${op} response is missing '${key}'`);
  return obj[key];
}

function strField(obj: Json, key: string, op: string): string {
  const value = field(obj, key, op);
  if (typeof value !== "string" || value === "") {
    throw new VendorError(`Blob vendor ${op} response has a malformed '${key}'`);
  }
  return value;
}

function optStrField(obj: Json, key: string, op: string): string | null {
  const value = obj[key];
  if (value === undefined || value === null || value === "") return null;
  if (typeof value !== "string") throw new VendorError(`Blob vendor ${op} response has a malformed '${key}'`);
  return value;
}

function intField(obj: Json, key: string, op: string): number {
  const value = field(obj, key, op);
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new VendorError(`Blob vendor ${op} response has a malformed '${key}'`);
  }
  return value;
}

function intMap(obj: Json, key: string): Record<string, number> {
  const value = obj[key];
  const out: Record<string, number> = {};
  if (value === null || typeof value !== "object" || Array.isArray(value)) return out;
  for (const [k, v] of Object.entries(value as Json)) {
    if (typeof v === "number" && Number.isSafeInteger(v)) out[k] = v;
  }
  return out;
}

function hostsField(obj: Json, key: string): string[] {
  const value = obj[key] ?? [];
  if (!Array.isArray(value) || !value.every((h) => typeof h === "string" && h !== "")) {
    throw new VendorError(`Blob vendor info response has a malformed '${key}'`);
  }
  return value as string[];
}

function isLoopbackHost(hostname: string): boolean {
  const host = hostname.toLowerCase().replace(/^\[|\]$/g, "");
  if (host === "localhost" || host === "::1") return true;
  return /^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(host);
}

function defaultPort(protocol: string): number {
  return protocol === "https:" ? 443 : 80;
}

function hostKey(hostname: string, port: string | number, protocol: string): string {
  const host = hostname.toLowerCase().replace(/^\[|\]$/g, "");
  const p = port === "" ? defaultPort(protocol) : Number(port);
  return `${host}|${p}`;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Read a response body, refusing more than `maxBytes`. */
async function readBounded(response: Response, maxBytes: number, what: string): Promise<Uint8Array> {
  const declared = response.headers.get("Content-Length");
  if (declared != null) {
    const length = Number(declared);
    if (Number.isFinite(length) && length > maxBytes) {
      await response.body?.cancel().catch(() => {});
      throw new VendorError(`${what} is ${length} bytes, over the ${maxBytes}-byte cap`);
    }
  }
  if (!response.body) return new Uint8Array();
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > maxBytes) {
        throw new VendorError(`${what} exceeded the ${maxBytes}-byte cap`);
      }
      chunks.push(value);
    }
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
  if (chunks.length === 1) return chunks[0]!;
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}

/** The `<Code>` of an S3/R2 XML error body, if any. */
function s3ErrorCode(body: string): string | null {
  const match = /<Code>([A-Za-z0-9]{1,64})<\/Code>/.exec(body);
  return match ? match[1]! : null;
}

// ---------------------------------------------------------------------------
// URL pinning
// ---------------------------------------------------------------------------

/** Options for {@link pinnedHostsValidator}. */
export interface PinnedHostsOptions {
  /** Also accept `http` URLs. Only for loopback test vendors; never enable it
   *  for a real deployment. */
  allowHttp?: boolean;
  /** URL prefixes the URL (without its query) must start with, e.g. a
   *  region's bucket path. */
  prefixes?: readonly string[];
}

/**
 * Build a `urlValidator` that accepts only URLs on `hosts`.
 *
 * A URL passes when its scheme is `https` (or `http` with `allowHttp`), it
 * carries no userinfo, and its host and port are exactly one of `hosts`. An
 * entry is `host` or `host:port` (`[v6]:port` for IPv6); an entry without a
 * port matches the scheme's default port only. Subdomains do not match. With
 * `prefixes`, the URL (without its query) must also start with one of them.
 *
 * @throws Error if `hosts` is empty or an entry will not parse.
 */
export function pinnedHostsValidator(hosts: Iterable<string>, options: PinnedHostsOptions = {}): (url: string) => void {
  const allowedProtocols = options.allowHttp ? ["https:", "http:"] : ["https:"];
  const allowed = new Set<string>();
  for (const raw of hosts) {
    const entry = raw.trim();
    let parsed: URL;
    try {
      parsed = new URL(`https://${entry}`);
    } catch {
      throw new Error(`Invalid pinned host entry: ${JSON.stringify(raw)}`);
    }
    if (
      !parsed.hostname ||
      parsed.username ||
      parsed.password ||
      parsed.search ||
      parsed.hash ||
      (parsed.pathname !== "/" && parsed.pathname !== "") ||
      entry.includes("/") ||
      entry.includes("@")
    ) {
      throw new Error(`Invalid pinned host entry: ${JSON.stringify(raw)}`);
    }
    // `new URL` drops an explicit default port (`:443`), so read the port the
    // entry spelled rather than the normalized one.
    const portMatch = /:(\d+)$/.exec(entry);
    if (portMatch) {
      allowed.add(hostKey(parsed.hostname, portMatch[1]!, "https:"));
    } else {
      for (const protocol of allowedProtocols) allowed.add(hostKey(parsed.hostname, "", protocol));
    }
  }
  if (allowed.size === 0) throw new Error("pinnedHostsValidator needs at least one host");
  const prefixes = (options.prefixes ?? []).filter((p) => p);

  return (url: string) => {
    let parsed: URL;
    try {
      parsed = new URL(url);
    } catch {
      throw new Error("URL is malformed");
    }
    if (!allowedProtocols.includes(parsed.protocol)) {
      const names = allowedProtocols.map((p) => p.slice(0, -1)).join(" or ");
      throw new Error(`URL scheme '${parsed.protocol.slice(0, -1)}' not allowed (only ${names})`);
    }
    if (parsed.username || parsed.password) throw new Error("URL userinfo is not allowed");
    if (!parsed.hostname || !allowed.has(hostKey(parsed.hostname, parsed.port, parsed.protocol))) {
      throw new Error(`URL host is not a pinned storage host: ${redactExternalUrl(url)}`);
    }
    if (prefixes.length > 0) {
      // WHATWG URL parsing has already resolved `..` segments (encoded or
      // not), so a traversal out of the prefix shows up as a mismatch here.
      const bare = `${parsed.protocol}//${parsed.host}${parsed.pathname}`;
      if (!prefixes.some((p) => bare.startsWith(p))) {
        throw new Error(`URL is outside the pinned storage prefixes: ${redactExternalUrl(url)}`);
      }
    }
  };
}

/** `urlValidator` backed by the vendor's `/v1/info`, cached. */
class VendorHostsValidator {
  private validator: ((url: string) => void) | null = null;
  private fetchedAt = 0;
  private pending: Promise<(url: string) => void> | null = null;

  constructor(
    private readonly api: VendorApi,
    private readonly ttlMs: number,
  ) {}

  private async current(): Promise<(url: string) => void> {
    const now = Date.now();
    if (this.validator && now - this.fetchedAt < this.ttlMs) return this.validator;
    this.pending ??= this.refresh().finally(() => {
      this.pending = null;
    });
    return this.pending;
  }

  private async refresh(): Promise<(url: string) => void> {
    try {
      const info = await this.api.info();
      const validator = pinnedHostsValidator([...info.uploadHosts, ...info.downloadHosts], {
        allowHttp: this.api.loopbackHttp,
        prefixes: info.urlPrefixes,
      });
      this.validator = validator;
      this.fetchedAt = Date.now();
      return validator;
    } catch (error) {
      if (this.validator) {
        // Keep the last known-good pin through a vendor blip.
        this.fetchedAt = Date.now();
        return this.validator;
      }
      throw new Error(`Cannot pin storage hosts: ${(error as Error).message}`);
    }
  }

  /** Throws (rejects) for a URL off the vendor's storage hosts. */
  readonly validate: UrlValidator = async (url: string) => {
    (await this.current())(url);
  };
}

// ---------------------------------------------------------------------------
// HTTP + wire mapping (the one place that knows the vendor's JSON shapes)
// ---------------------------------------------------------------------------

/** Options shared by {@link VendorStorage} and {@link VendorBlobClient}. */
export interface VendorClientOptions {
  /** Per-request timeout, ms (default 30000). */
  timeoutMs?: number;
  /**
   * `fetch` for vendor API requests (e.g. a Cloudflare service binding). Its
   * transport only: requests carry exactly the vendor headers. Storage-host
   * (signed URL) requests always use the global `fetch`.
   */
  fetch?: typeof globalThis.fetch;
  /** Attempts per request; transport errors, 429 and 5xx are retried with
   *  exponential backoff (default 3). */
  maxAttempts?: number;
  /** Base backoff between attempts, ms (default 250). */
  backoffMs?: number;
  /** How long the `/v1/info` host pin is cached, ms (default 600000). */
  infoTtlMs?: number;
}

class VendorApi {
  static readonly PUT_PATH = "/v1/blobs/put";
  static readonly GET_PATH = "/v1/blobs/get";
  static readonly DELETE_PATH = "/v1/blobs/delete";
  static readonly DELETE_SCOPE_PATH = "/v1/blobs/delete_scope";
  static readonly UPLOAD_URLS_PATH = "/v1/upload_urls";
  static readonly INFO_PATH = "/v1/info";
  static readonly MAX_UPLOAD_URLS = 100;
  static readonly MAX_DELETE_IDS = 1000;

  readonly baseUrl: string;
  readonly loopbackHttp: boolean;
  readonly timeoutMs: number;
  private readonly token: string;
  private readonly apiFetch: typeof globalThis.fetch | undefined;
  private readonly maxAttempts: number;
  private readonly backoffMs: number;

  constructor(url: string, token: string, options: VendorClientOptions) {
    let parsed: URL;
    try {
      parsed = new URL(url);
    } catch {
      throw new Error(`Blob vendor URL must be an absolute https URL: ${redactExternalUrl(url)}`);
    }
    if ((parsed.protocol !== "https:" && parsed.protocol !== "http:") || !parsed.hostname) {
      throw new Error(`Blob vendor URL must be an absolute https URL: ${redactExternalUrl(url)}`);
    }
    this.loopbackHttp = parsed.protocol === "http:";
    if (this.loopbackHttp && !isLoopbackHost(parsed.hostname)) {
      // The bearer token would cross the network in plain text.
      throw new Error(`Blob vendor URL must use https (http is allowed only for loopback): ${redactExternalUrl(url)}`);
    }
    if (!token) throw new Error("Blob vendor token is empty");
    const maxAttempts = options.maxAttempts ?? 3;
    if (!Number.isInteger(maxAttempts) || maxAttempts < 1) throw new Error("maxAttempts must be >= 1");
    this.baseUrl = url.replace(/\/+$/, "");
    this.token = token;
    this.timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    this.apiFetch = options.fetch;
    this.maxAttempts = maxAttempts;
    this.backoffMs = options.backoffMs ?? 250;
  }

  // -- transport ----------------------------------------------------------

  /**
   * Send one request, retrying transport errors, 429 and 5xx with backoff.
   * The request carries exactly `headers`, never follows a redirect, and on a
   * storage host always goes through the global `fetch`.
   */
  async send(
    method: string,
    url: string,
    op: string,
    headers: Record<string, string>,
    body: Uint8Array | string | null,
    storage: boolean,
  ): Promise<Response> {
    const fetchFn = storage ? globalThis.fetch : (this.apiFetch ?? globalThis.fetch);
    let lastError = "";
    let status: number | null = null;
    for (let attempt = 0; attempt < this.maxAttempts; attempt++) {
      if (attempt > 0) {
        const delay = Math.min(this.backoffMs * 2 ** (attempt - 1), MAX_BACKOFF_MS);
        await sleep(delay * (0.5 + Math.random() / 2));
      }
      const controller = new AbortController();
      const timer = this.timeoutMs > 0 ? setTimeout(() => controller.abort(), this.timeoutMs) : null;
      let response: Response;
      try {
        const init: RequestInit = {
          method,
          headers,
          redirect: "manual",
          signal: controller.signal,
        };
        if (body !== null) init.body = body as BodyInit;
        // Bun-only: hand back the stored bytes, not a transparently decoded body.
        (init as Record<string, unknown>).decompress = false;
        response = await fetchFn(url, init);
      } catch (error) {
        // Never the error's message: fetch errors can embed the full URL.
        status = null;
        lastError = controller.signal.aborted ? "timeout" : ((error as Error)?.name ?? "Error");
        continue;
      } finally {
        if (timer) clearTimeout(timer);
      }
      if (response.status === 429 || response.status >= 500) {
        status = response.status;
        lastError = `HTTP ${response.status}`;
        await response.body?.cancel().catch(() => {});
        continue;
      }
      return response;
    }
    throw new VendorError(
      `Blob vendor ${op} failed after ${this.maxAttempts} attempts (${lastError}): ${redactExternalUrl(url)}`,
      status,
    );
  }

  private async call(method: string, path: string, op: string, body: Json | null): Promise<Json> {
    const url = `${this.baseUrl}${path}`;
    const headers: Record<string, string> = {
      Authorization: `Bearer ${this.token}`,
      Accept: "application/json",
    };
    let content: string | null = null;
    if (body !== null) {
      headers["Content-Type"] = "application/json";
      content = JSON.stringify(body);
    }
    const response = await this.send(method, url, op, headers, content, false);
    const text = await response.text();
    if (response.status < 200 || response.status >= 300) {
      const [code, detail] = errorDetail(text);
      throw new VendorError(`Blob vendor ${op} failed: HTTP ${response.status}${detail}`, response.status, code);
    }
    if (!text) return {};
    let parsed: unknown;
    try {
      parsed = JSON.parse(text);
    } catch {
      throw new VendorError(`Blob vendor ${op} returned non-JSON`, response.status);
    }
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new VendorError(`Blob vendor ${op} returned a non-object JSON body`, response.status);
    }
    return parsed as Json;
  }

  // -- wire mapping -------------------------------------------------------

  async put(req: {
    size: number;
    sha256B64: string;
    purpose: VendorPurpose;
    scope?: BlobScope | null;
    contentEncoding?: string | null;
    idempotencyKey?: string | null;
  }): Promise<PutGrant> {
    const body: Json = { size: req.size, sha256_b64: req.sha256B64, purpose: req.purpose };
    if (req.scope) body.scope = { shard: req.scope.shard, execution_id: req.scope.executionId };
    if (req.contentEncoding) body.content_encoding = req.contentEncoding;
    if (req.idempotencyKey) body.idempotency_key = req.idempotencyKey;
    const resp = await this.call("POST", VendorApi.PUT_PATH, "put", body);
    const method = resp.upload_method ?? "PUT";
    if (method !== "PUT") {
      throw new VendorError(`Blob vendor put asked for unsupported upload_method ${JSON.stringify(method)}`);
    }
    const rawHeaders = field(resp, "required_headers", "put");
    if (
      rawHeaders === null ||
      typeof rawHeaders !== "object" ||
      Array.isArray(rawHeaders) ||
      !Object.values(rawHeaders as Json).every((v) => typeof v === "string")
    ) {
      throw new VendorError("Blob vendor put response has malformed 'required_headers'");
    }
    return {
      objectId: strField(resp, "object_id", "put"),
      uploadUrl: strField(resp, "upload_url", "put"),
      requiredHeaders: { ...(rawHeaders as Record<string, string>) },
      downloadUrl: optStrField(resp, "download_url", "put"),
      expiresAt: parseExpires(field(resp, "expires_at", "put")),
      region: optStrField(resp, "region", "put"),
      downloadExpiresAt:
        resp.download_expires_at !== undefined && resp.download_expires_at !== null
          ? parseExpires(resp.download_expires_at)
          : null,
    };
  }

  async get(objectId: string): Promise<BlobLocation> {
    const resp = await this.call("POST", VendorApi.GET_PATH, "get", { object_id: objectId });
    return {
      downloadUrl: strField(resp, "download_url", "get"),
      size: intField(resp, "size", "get"),
      sha256B64: strField(resp, "sha256_b64", "get"),
      contentEncoding: optStrField(resp, "content_encoding", "get"),
      expiresAt: parseExpires(field(resp, "expires_at", "get")),
    };
  }

  async delete(objectIds: string[]): Promise<number> {
    const resp = await this.call("POST", VendorApi.DELETE_PATH, "delete", { object_ids: objectIds });
    return typeof resp.deleted === "number" ? resp.deleted : 0;
  }

  async deleteScope(scope: BlobScope): Promise<number> {
    const resp = await this.call("POST", VendorApi.DELETE_SCOPE_PATH, "delete_scope", {
      shard: scope.shard,
      execution_id: scope.executionId,
    });
    return typeof resp.deleted === "number" ? resp.deleted : 0;
  }

  async uploadUrls(count: number): Promise<UploadUrl[]> {
    if (!Number.isInteger(count) || count < 1 || count > VendorApi.MAX_UPLOAD_URLS) {
      throw new Error(`count must be between 1 and ${VendorApi.MAX_UPLOAD_URLS}`);
    }
    const resp = await this.call("POST", VendorApi.UPLOAD_URLS_PATH, "upload_urls", { count });
    const urls = field(resp, "urls", "upload_urls");
    if (!Array.isArray(urls) || urls.length === 0) {
      throw new VendorError("Blob vendor upload_urls response has malformed 'urls'");
    }
    return urls.map((item) => {
      if (item === null || typeof item !== "object" || Array.isArray(item)) {
        throw new VendorError("Blob vendor upload_urls response has a malformed entry");
      }
      const entry = item as Json;
      return {
        uploadUrl: strField(entry, "upload_url", "upload_urls"),
        downloadUrl: strField(entry, "download_url", "upload_urls"),
        expiresAt: parseExpires(field(entry, "expires_at", "upload_urls")),
      };
    });
  }

  async info(): Promise<VendorInfo> {
    const resp = await this.call("GET", VendorApi.INFO_PATH, "info", null);
    return {
      region: typeof resp.region === "string" ? resp.region : "",
      uploadHosts: hostsField(resp, "upload_hosts"),
      downloadHosts: hostsField(resp, "download_hosts"),
      tenant: typeof resp.tenant === "string" ? resp.tenant : "",
      jurisdiction: optStrField(resp, "jurisdiction", "info"),
      urlPrefixes: hostsField(resp, "url_prefixes"),
      limits: intMap(resp, "limits"),
      ttlSeconds: intMap(resp, "ttl_seconds"),
    };
  }

  // -- signed-URL upload --------------------------------------------------

  /** PUT `data` to a granted URL with exactly its required headers. */
  async putObject(grant: PutGrant, data: Uint8Array, contentEncoding?: string | null): Promise<void> {
    const headers: Record<string, string> = {};
    let declared: string | undefined;
    let hasEncoding = false;
    for (const [name, value] of Object.entries(grant.requiredHeaders)) {
      const lower = name.toLowerCase();
      if (lower === "content-length") {
        // The runtime derives Content-Length from the body (Fetch forbids
        // setting it), so check it agrees with the grant instead of sending it.
        declared = value;
        continue;
      }
      if (lower === "content-encoding") hasEncoding = true;
      headers[name] = value;
    }
    if (declared !== undefined && declared !== String(data.byteLength)) {
      throw new VendorError(`Blob vendor granted Content-Length ${declared} for a ${data.byteLength}-byte object`);
    }
    if (contentEncoding && !hasEncoding) headers["Content-Encoding"] = contentEncoding;
    const response = await this.send("PUT", grant.uploadUrl, "upload", headers, data, true);
    if (response.status < 200 || response.status >= 300) {
      const code = s3ErrorCode(await response.text().catch(() => ""));
      throw new VendorError(
        `Signed upload failed: HTTP ${response.status}${code ? ` ${code}` : ""}: ${redactExternalUrl(grant.uploadUrl)}`,
        response.status,
        code,
      );
    }
    await response.body?.cancel().catch(() => {});
  }
}

/** Pull `[code, ": code: message"]` out of a vendor `{error, message}` body. */
function errorDetail(text: string): [string | null, string] {
  try {
    const parsed = JSON.parse(text);
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      const code = typeof parsed.error === "string" && parsed.error ? parsed.error.slice(0, 64) : null;
      const message = typeof parsed.message === "string" ? parsed.message.slice(0, 200) : null;
      const parts = [code, message].filter((p): p is string => !!p);
      return [code, parts.length ? `: ${parts.join(": ")}` : ""];
    }
  } catch {
    // Not JSON: no detail.
  }
  return [null, ""];
}

function resolveEnv(env: EnvLike | undefined): EnvLike {
  if (env) return env;
  const proc = (globalThis as { process?: { env?: EnvLike } }).process;
  return proc?.env ?? {};
}

function envUrlAndToken(env: EnvLike): [string, string] {
  const url = (env[ENV_URL] ?? "").trim();
  if (!url) throw new Error(`${ENV_URL} is not set`);
  const token = (env[ENV_TOKEN] ?? "").trim() || (env[ENV_TOKEN_FALLBACK] ?? "").trim();
  if (!token) throw new Error(`${ENV_URL} is set but neither ${ENV_TOKEN} nor ${ENV_TOKEN_FALLBACK} is`);
  return [url, token];
}

// ---------------------------------------------------------------------------
// Shared base
// ---------------------------------------------------------------------------

abstract class VendorClientBase {
  /** @internal */
  protected readonly api: VendorApi;
  private readonly hosts: VendorHostsValidator;

  constructor(url: string, token: string, options: VendorClientOptions = {}) {
    this.api = new VendorApi(url, token, options);
    this.hosts = new VendorHostsValidator(this.api, options.infoTtlMs ?? DEFAULT_INFO_TTL_MS);
  }

  /** A `urlValidator` pinned to the vendor's storage hosts (`/v1/info`, cached). */
  get urlValidator(): UrlValidator {
    return this.hosts.validate;
  }

  /** The vendor's base URL (no credentials in it). */
  get baseUrl(): string {
    return this.api.baseUrl;
  }

  /** Fetch the vendor's region and storage hosts (uncached). */
  info(): Promise<VendorInfo> {
    return this.api.info();
  }

  /** @internal Reject a vended URL off the pinned hosts. */
  protected async check(url: string): Promise<void> {
    try {
      await this.hosts.validate(url);
    } catch (error) {
      throw new VendorError(`Blob vendor returned a URL off its pinned hosts: ${(error as Error).message}`);
    }
  }
}

// ---------------------------------------------------------------------------
// VendorStorage
// ---------------------------------------------------------------------------

/** Options for {@link VendorStorage}. */
export interface VendorStorageOptions extends VendorClientOptions {
  /** Object purpose; `rpc` (default) gets a download URL and the short lifecycle. */
  purpose?: VendorPurpose;
  /** Check vended upload/download URLs against the vendor's hosts before
   *  using them (default `true`). */
  pinHosts?: boolean;
}

/**
 * {@link ExternalStorage} and {@link UploadUrlProvider} backed by the blob vendor.
 *
 * `upload()` (server-side externalization) asks the vendor for a sized,
 * checksummed grant, PUTs the bytes with exactly the grant's required headers,
 * and returns the grant's download URL. `generateUploadUrl()` (client
 * uploads) asks `/v1/upload_urls` for one unsized PUT/GET pair. Objects are
 * cleaned up by the vendor (registry alarm plus storage lifecycle rules);
 * vgi-rpc never deletes them.
 */
export class VendorStorage extends VendorClientBase implements ExternalStorage, UploadUrlProvider {
  private readonly purpose: VendorPurpose;
  private readonly pin: boolean;

  constructor(url: string, token: string, options: VendorStorageOptions = {}) {
    super(url, token, options);
    this.purpose = options.purpose ?? "rpc";
    this.pin = options.pinHosts ?? true;
  }

  /** Build from `VGI_BLOB_VENDOR_URL` and the vendor token variables.
   *  @throws Error if the URL or token is missing. */
  static fromEnv(options: VendorStorageOptions & { env?: EnvLike } = {}): VendorStorage {
    const [url, token] = envUrlAndToken(resolveEnv(options.env));
    return new VendorStorage(url, token, options);
  }

  /** Upload `data` via a vendor grant and return its pre-signed download URL.
   *  @param contentEncoding Encoding already applied to `data` (`""` for none). */
  async upload(data: Uint8Array, contentEncoding: string): Promise<string> {
    const encoding = contentEncoding || null;
    const grant = await this.api.put({
      size: data.byteLength,
      sha256B64: toBase64(await sha256Bytes(data)),
      purpose: this.purpose,
      contentEncoding: encoding,
    });
    if (this.pin) await this.check(grant.uploadUrl);
    await this.api.putObject(grant, data, encoding);
    const downloadUrl = grant.downloadUrl ?? (await this.api.get(grant.objectId)).downloadUrl;
    if (this.pin) await this.check(downloadUrl);
    return downloadUrl;
  }

  /** Ask the vendor for one unsized PUT/GET pair. */
  async generateUploadUrl(): Promise<UploadUrl> {
    const [upload] = await this.api.uploadUrls(1);
    if (this.pin) {
      await this.check(upload!.uploadUrl);
      await this.check(upload!.downloadUrl);
    }
    return upload!;
  }
}

// ---------------------------------------------------------------------------
// VendorBlobClient
// ---------------------------------------------------------------------------

/** Options for {@link VendorBlobClient}. */
export interface VendorBlobClientOptions extends VendorClientOptions {
  /** Object purpose (default `fs`). */
  purpose?: VendorPurpose;
  /** Largest blob {@link VendorBlobClient.getBytes} will fetch (default 256 MiB). */
  maxFetchBytes?: number;
}

/**
 * Raw byte blobs through the vendor, referenced by {@link BlobRef}.
 *
 * For callers that store references themselves (e.g. a key-value store that
 * externalizes large values). Objects use purpose `fs` by default: no download
 * URL at `put`; {@link getBytes} asks the vendor for a fresh one.
 */
export class VendorBlobClient extends VendorClientBase {
  private readonly purpose: VendorPurpose;
  /** Largest blob {@link getBytes} will fetch. */
  readonly maxFetchBytes: number;

  constructor(url: string, token: string, options: VendorBlobClientOptions = {}) {
    super(url, token, options);
    this.purpose = options.purpose ?? "fs";
    this.maxFetchBytes = options.maxFetchBytes ?? DEFAULT_MAX_FETCH_BYTES;
  }

  /** Build from `VGI_BLOB_VENDOR_URL` and the vendor token variables.
   *  @throws Error if the URL or token is missing. */
  static fromEnv(options: VendorBlobClientOptions & { env?: EnvLike } = {}): VendorBlobClient {
    const [url, token] = envUrlAndToken(resolveEnv(options.env));
    return new VendorBlobClient(url, token, options);
  }

  /**
   * Store `data` and return its reference.
   *
   * @param options.scope The execution the blob belongs to, for {@link deleteScope}.
   * @param options.idempotencyKey Makes a retried logical write reuse the same
   *   object: the same key with the same bytes returns the same object id;
   *   with other bytes the vendor refuses (`code === "idempotency_conflict"`).
   * @throws VendorError if the vendor or the signed upload fails.
   */
  async putBytes(
    data: Uint8Array,
    options: { scope?: BlobScope | null; idempotencyKey?: string | null } = {},
  ): Promise<BlobRef> {
    const digest = await sha256Bytes(data);
    const grant = await this.api.put({
      size: data.byteLength,
      sha256B64: toBase64(digest),
      purpose: this.purpose,
      scope: options.scope ?? null,
      idempotencyKey: options.idempotencyKey ?? null,
    });
    await this.check(grant.uploadUrl);
    await this.api.putObject(grant, data);
    return { objectId: grant.objectId, size: data.byteLength, sha256: toHex(digest) };
  }

  /**
   * Fetch and verify the blob `ref` names.
   *
   * The download is pinned to the vendor's storage hosts, capped at
   * {@link maxFetchBytes}, and checked against the size and SHA-256 in `ref`
   * (not the vendor's own report).
   *
   * @throws VendorError if the vendor fails, the object is missing (status
   *   404), the download fails, or the bytes do not match `ref` (`code`
   *   `size_mismatch` / `sha256_mismatch`).
   */
  async getBytes(ref: BlobRef): Promise<Uint8Array> {
    if (ref.size > this.maxFetchBytes) {
      throw new VendorError(`Blob ${ref.objectId} is ${ref.size} bytes, over maxFetchBytes (${this.maxFetchBytes})`);
    }
    const location = await this.api.get(ref.objectId);
    if (location.contentEncoding !== null && location.contentEncoding !== "identity") {
      throw new VendorError(
        `Blob ${ref.objectId} has unsupported content_encoding ${JSON.stringify(location.contentEncoding)}`,
      );
    }
    if (location.size !== ref.size) {
      throw new VendorError(
        `Blob ${ref.objectId} size mismatch: vendor reports ${location.size}, ref has ${ref.size}`,
        null,
        "size_mismatch",
      );
    }
    try {
      await this.urlValidator(location.downloadUrl);
    } catch (error) {
      throw new VendorError(`Blob ${ref.objectId} download rejected: ${(error as Error).message}`);
    }
    const response = await this.api.send("GET", location.downloadUrl, "download", {}, null, true);
    if (response.status < 200 || response.status >= 300) {
      const code = s3ErrorCode(await response.text().catch(() => ""));
      throw new VendorError(
        `Blob ${ref.objectId} download failed: HTTP ${response.status}${code ? ` ${code}` : ""}: ` +
          redactExternalUrl(location.downloadUrl),
        response.status,
        code,
      );
    }
    // One byte over the expected size is enough to report a size mismatch.
    const data = await readBounded(response, ref.size + 1, `Blob ${ref.objectId}`).catch((error) => {
      if (error instanceof VendorError && /cap/.test(error.message)) {
        throw new VendorError(
          `Blob ${ref.objectId} size mismatch: expected ${ref.size}, got more`,
          null,
          "size_mismatch",
        );
      }
      throw new VendorError(`Blob ${ref.objectId} download failed (${(error as Error)?.name ?? "Error"})`);
    });
    if (data.byteLength !== ref.size) {
      throw new VendorError(
        `Blob ${ref.objectId} size mismatch: expected ${ref.size}, got ${data.byteLength}`,
        null,
        "size_mismatch",
      );
    }
    const actual = toHex(await sha256Bytes(data));
    if (actual !== ref.sha256.toLowerCase()) {
      throw new VendorError(
        `Blob ${ref.objectId} SHA-256 mismatch: expected ${ref.sha256}, got ${actual}`,
        null,
        "sha256_mismatch",
      );
    }
    return data;
  }

  /** Delete objects by reference or object id (missing objects are ignored).
   *  @returns How many objects the vendor deleted. */
  async delete(refs: Iterable<BlobRef | string>): Promise<number> {
    const ids = [...refs].map((r) => (typeof r === "string" ? r : r.objectId));
    let total = 0;
    for (let start = 0; start < ids.length; start += VendorApi.MAX_DELETE_IDS) {
      total += await this.api.delete(ids.slice(start, start + VendorApi.MAX_DELETE_IDS));
    }
    return total;
  }

  /** Delete every object put under `(shard, executionId)`.
   *  @returns How many objects the vendor deleted. */
  deleteScope(shard: string, executionId: string): Promise<number> {
    return this.api.deleteScope({ shard, executionId });
  }
}

// ---------------------------------------------------------------------------
// Env-driven configs
// ---------------------------------------------------------------------------

/**
 * Build a `urlValidator` pinned to the vendor's storage hosts.
 *
 * The hosts come from the vendor's `GET /v1/info`, fetched on first use and
 * cached for `ttlMs`; a failed refresh keeps the last good list. Only `https`
 * URLs pass (`http` only when the vendor itself is a loopback `http` URL, i.e.
 * a local test vendor). The returned validator is async.
 */
export function vendorUrlValidator(
  url: string,
  token: string,
  options: { ttlMs?: number; fetch?: typeof globalThis.fetch } = {},
): UrlValidator {
  const apiOptions: VendorClientOptions = {};
  if (options.fetch) apiOptions.fetch = options.fetch;
  return new VendorHostsValidator(new VendorApi(url, token, apiOptions), options.ttlMs ?? DEFAULT_INFO_TTL_MS).validate;
}

function parsePositiveInt(env: EnvLike, name: string): number | null {
  const raw = (env[name] ?? "").trim();
  if (!raw) return null;
  if (!/^[+-]?\d+$/.test(raw)) throw new Error(`${name} must be an integer, got ${JSON.stringify(raw)}`);
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${name} must be positive, got ${value}`);
  return value;
}

function parseCompression(raw: string): ExternalLocationConfig["compression"] | undefined {
  const text = raw.trim().toLowerCase();
  if (text === "" || text === "none" || text === "off" || text === "identity") return undefined;
  const [name, levelText] = text.split(":", 2) as [string, string | undefined];
  if (name !== "zstd") {
    // The TypeScript server compresses with zstd only.
    throw new Error(`${ENV_COMPRESSION} must be none or zstd (optionally ':<level>'), got ${JSON.stringify(raw)}`);
  }
  if (levelText === undefined || levelText === "") return { algorithm: "zstd" };
  if (!/^[+-]?\d+$/.test(levelText)) {
    throw new Error(`${ENV_COMPRESSION} level must be an integer, got ${JSON.stringify(raw)}`);
  }
  return { algorithm: "zstd", level: Number(levelText) };
}

/** {@link ExternalLocationConfig} whose `storage` is the {@link VendorStorage}
 *  (also usable as the HTTP handler's `uploadUrlProvider`). */
export interface VendorExternalLocationConfig extends ExternalLocationConfig {
  /** The vendor-backed storage (results and upload URLs). */
  storage: VendorStorage;
}

/**
 * Server external-location config from the blob-vendor environment.
 *
 * Uses {@link VendorStorage} for uploads, pins inbound pointer URLs to the
 * vendor's hosts, and reads `VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES` (default
 * 1 MiB) and `VGI_RPC_EXTERNAL_COMPRESSION` (default none). Pass
 * `config.storage` as `uploadUrlProvider` to `createHttpHandler` to also vend
 * client upload URLs.
 *
 * @param env Environment mapping (default `process.env`; on Cloudflare Workers
 *   pass the Worker's `env`).
 * @returns The config, or `null` when `VGI_BLOB_VENDOR_URL` is unset.
 * @throws Error if the vendor URL is set but the token is missing, or a
 *   variable will not parse.
 */
export function externalConfigFromEnv(env?: EnvLike): VendorExternalLocationConfig | null {
  const environ = resolveEnv(env);
  if (!(environ[ENV_URL] ?? "").trim()) return null;
  const storage = VendorStorage.fromEnv({ env: environ });
  const config: VendorExternalLocationConfig = {
    storage,
    externalizeThresholdBytes: parsePositiveInt(environ, ENV_THRESHOLD) ?? DEFAULT_THRESHOLD,
    urlValidator: storage.urlValidator,
  };
  const compression = parseCompression(environ[ENV_COMPRESSION] ?? "");
  if (compression) config.compression = compression;
  return config;
}

/** Client-side external-location settings from {@link clientExternalConfigFromEnv}. */
export interface VendorClientExternalConfig {
  /** Accepts pointer and upload URLs only on the vendor's storage hosts. */
  urlValidator: UrlValidator;
}

/**
 * Client external-location config pinned to the blob vendor's hosts, for
 * clients that hold a vendor token (e.g. a worker calling another worker):
 * pointer and upload URLs are accepted only on the vendor's storage hosts.
 *
 * @returns `{ urlValidator }`, or `null` when `VGI_BLOB_VENDOR_URL` is unset.
 */
export function clientExternalConfigFromEnv(env?: EnvLike): VendorClientExternalConfig | null {
  const environ = resolveEnv(env);
  if (!(environ[ENV_URL] ?? "").trim()) return null;
  const [url, token] = envUrlAndToken(environ);
  return { urlValidator: vendorUrlValidator(url, token) };
}

/** Read `VGI_RPC_MAX_UPLOAD_BYTES` (the advertised client-upload size), or
 *  `null` when unset. */
export function maxUploadBytesFromEnv(env?: EnvLike): number | null {
  return parsePositiveInt(resolveEnv(env), ENV_MAX_UPLOAD_BYTES);
}
