// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { DEFAULT_ACCEPTED_MAX_RESPONSE_BYTES } from "#vgi-rpc-client-response-budget";
import { RpcError } from "../errors.js";
import {
  ACCEPT_MAX_RESPONSE_BYTES_HEADER,
  optionalResponseBudget,
  parsePositiveSafeDecimal,
  parseResponseBudgetDecimal,
} from "../http/response-budget.js";

/**
 * HTTP server capability discovery.
 *
 * Mirrors Python's `http_capabilities()`: probes `OPTIONS {prefix}/health`
 * and reads three response headers:
 *   - `VGI-Max-Request-Bytes`  — server-enforced inline request cap
 *   - `VGI-Upload-URL-Support` — "true" when the server vends upload URLs
 *   - `VGI-Max-Upload-Bytes`   — cap on out-of-band upload size
 *   - `VGI-Max-Response-Bytes` — server-side response cap
 *   - `VGI-Accept-Max-Response-Bytes-Support` — negotiated client cap support
 *
 * Honours `Cache-Control: max-age=N` for refresh scheduling.
 */

export interface HttpServerCapabilities {
  /** Server's advertised max inline request body size (bytes). */
  maxRequestBytes: number | null;
  /** Whether the server vends upload URLs via `__upload_url__/init`. */
  uploadUrlSupport: boolean;
  /** Cap on the size of an externalized upload (bytes). */
  maxUploadBytes: number | null;
  /** Server/hosting maximum response bytes, when advertised. */
  maxResponseBytes: number | null;
  /** Whether the server honors VGI-Accept-Max-Response-Bytes. */
  acceptMaxResponseBytesSupport: boolean;
  /** Cap on the externalized bytes of one response, when advertised. */
  maxExternalizedResponseBytes: number | null;
  /** Whether the server has a storage backend wired up, and can therefore
   *  rescue an oversize response by externalizing it. */
  externalizationEnabled: boolean;
  /** Content encodings the server can decode on requests and produce on
   *  responses, as the lowercase wire tokens (`zstd`, `gzip`, `identity`).
   *
   *  Present-but-empty is a real answer — "this server compresses nothing" —
   *  and distinct from an absent header, which means a server predating the
   *  advertisement and is read as zstd-only. */
  supportedEncodings: string[];
  /** Whether the server honours `VGI-Session` sticky sessions. */
  stickyEnabled: boolean;
  /** Seconds a session lives when opened without an explicit TTL. */
  stickyDefaultTtl: number | null;
  /** Header *names* the server tells clients to echo for the life of a
   *  session. The values arrive per session as `VGI-Echo-<name>` response
   *  headers; this is the introspectable list. */
  stickyEchoHeaders: string[];
  /** Monotonic-time-ish epoch (ms) at which this snapshot should be re-probed. */
  cacheExpiresAt: number | null;
}

const MAX_REQUEST_BYTES_HEADER = "VGI-Max-Request-Bytes";
const UPLOAD_URL_HEADER = "VGI-Upload-URL-Support";
const MAX_UPLOAD_BYTES_HEADER = "VGI-Max-Upload-Bytes";
const MAX_RESPONSE_BYTES_HEADER = "VGI-Max-Response-Bytes";
const ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER = "VGI-Accept-Max-Response-Bytes-Support";
const MAX_EXTERNALIZED_RESPONSE_BYTES_HEADER = "VGI-Max-Externalized-Response-Bytes";
const EXTERNALIZATION_ENABLED_HEADER = "VGI-Externalization-Enabled";
const SUPPORTED_ENCODINGS_HEADER = "VGI-Supported-Encodings";
const STICKY_ENABLED_HEADER = "VGI-Sticky-Enabled";
const STICKY_DEFAULT_TTL_HEADER = "VGI-Sticky-Default-TTL";
const STICKY_ECHO_HEADERS_HEADER = "VGI-Sticky-Echo-Headers";

/** Read a header case-insensitively, returning `null` when it is absent. */
function headerValue(headers: Headers, name: string): string | null {
  return headers.get(name) ?? headers.get(name.toLowerCase());
}

/** Split a comma-separated header into its non-empty, trimmed tokens. */
function headerList(headers: Headers, name: string): string[] {
  const raw = headerValue(headers, name);
  if (raw == null) return [];
  return raw
    .split(",")
    .map((token) => token.trim())
    .filter((token) => token.length > 0);
}

function parseHeaderInt(headers: Headers, name: string, responseBudget = false): number | null {
  const raw = headers.get(name) ?? headers.get(name.toLowerCase());
  if (raw == null) return null;
  return responseBudget ? parseResponseBudgetDecimal(raw) : parsePositiveSafeDecimal(raw);
}

/** Parse one HTTP response's VGI capability headers into a validated snapshot. */
export function parseCapabilitiesFromHeaders(headers: Headers): HttpServerCapabilities {
  const uploadRaw = headers.get(UPLOAD_URL_HEADER) ?? headers.get(UPLOAD_URL_HEADER.toLowerCase());
  const uploadUrlSupport = uploadRaw === "true";

  let cacheExpiresAt: number | null = null;
  const cc = headers.get("Cache-Control") ?? headers.get("cache-control");
  if (cc) {
    for (const token of cc.split(",")) {
      const t = token.trim().toLowerCase();
      if (t.startsWith("max-age=")) {
        const seconds = Number.parseFloat(t.slice("max-age=".length));
        if (Number.isFinite(seconds)) {
          cacheExpiresAt = Date.now() + seconds * 1000;
        }
        break;
      }
    }
  }

  // Absent means a server predating the advertisement, which only ever spoke
  // zstd; present-but-empty means a server that positively speaks none.
  const encodingsRaw = headerValue(headers, SUPPORTED_ENCODINGS_HEADER);
  const supportedEncodings =
    encodingsRaw == null
      ? ["zstd"]
      : headerList(headers, SUPPORTED_ENCODINGS_HEADER).map((token) => token.toLowerCase());

  return {
    maxRequestBytes: parseHeaderInt(headers, MAX_REQUEST_BYTES_HEADER),
    uploadUrlSupport,
    maxUploadBytes: parseHeaderInt(headers, MAX_UPLOAD_BYTES_HEADER),
    maxResponseBytes: parseHeaderInt(headers, MAX_RESPONSE_BYTES_HEADER, true),
    acceptMaxResponseBytesSupport:
      (headers.get(ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER) ??
        headers.get(ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER.toLowerCase())) === "true",
    maxExternalizedResponseBytes: parseHeaderInt(headers, MAX_EXTERNALIZED_RESPONSE_BYTES_HEADER, true),
    externalizationEnabled: headerValue(headers, EXTERNALIZATION_ENABLED_HEADER) === "true",
    supportedEncodings,
    stickyEnabled: headerValue(headers, STICKY_ENABLED_HEADER) === "true",
    stickyDefaultTtl: parseHeaderInt(headers, STICKY_DEFAULT_TTL_HEADER),
    stickyEchoHeaders: headerList(headers, STICKY_ECHO_HEADERS_HEADER),
    cacheExpiresAt,
  };
}

/** Every VGI HTTP response, not only discovery, must repeat exact support. */
export function requireResponseBudgetSupport(headers: Headers): HttpServerCapabilities {
  const capabilities = parseCapabilitiesFromHeaders(headers);
  if (!capabilities.acceptMaxResponseBytesSupport) {
    throw new RpcError(
      "ProtocolError",
      "Server must advertise VGI-Accept-Max-Response-Bytes-Support: true on every RPC response",
      "",
    );
  }
  return capabilities;
}

/**
 * Probe the server's auth-exempt `/health` endpoint for HTTP capabilities.
 *
 * The probe is `HEAD`, not `OPTIONS`, because `OPTIONS` cannot be used from a
 * browser. It carries `VGI-Accept-Max-Response-Bytes`, which makes it a
 * non-simple request, so the browser preflights it and asks
 * `Access-Control-Request-Method: OPTIONS` — and a server answers a preflight
 * with the methods its `/health` route actually implements, which is `GET` and
 * `HEAD`. The probe is blocked before it is sent, and the error names a method
 * nobody wrote. Every browser consumer hit this; each had to shim `fetch` to
 * get a connection at all.
 *
 * `HEAD` is equivalent for this purpose and is what the spec asks for:
 * `{prefix}/health` answers `GET, HEAD, OPTIONS` and carries the capability
 * headers on all three (WIRE_PROTOCOL.md §10). The C++ client already probes
 * with `HEAD`, so this also removes a divergence between the ports rather than
 * adding one.
 */
export async function discoverHttpCapabilities(
  baseUrl: string,
  prefix: string,
  authorization?: string,
  acceptedMaxResponseBytes?: number,
  fetchFn: typeof globalThis.fetch = globalThis.fetch,
): Promise<HttpServerCapabilities> {
  const headers: Record<string, string> = {};
  if (authorization) headers.Authorization = authorization;
  const accepted = acceptedMaxResponseBytes ?? DEFAULT_ACCEPTED_MAX_RESPONSE_BYTES;
  optionalResponseBudget(accepted, "acceptedMaxResponseBytes");
  headers[ACCEPT_MAX_RESPONSE_BYTES_HEADER] = String(accepted);
  const resp = await fetchFn(`${baseUrl}${prefix}/health`, {
    method: "HEAD",
    headers,
  });
  if (!resp.ok) {
    throw new RpcError("TransportError", `Capability discovery failed: HTTP ${resp.status}`, "");
  }
  // HEAD answers 200 with no body; any successful 2xx status is valid.
  return parseCapabilitiesFromHeaders(resp.headers);
}

/** Return whether a cached capability snapshot remains usable without another probe. */
export function isCapabilitySnapshotFresh(snapshot: HttpServerCapabilities | null): boolean {
  if (!snapshot) return false;
  if (snapshot.cacheExpiresAt == null) return true;
  return Date.now() < snapshot.cacheExpiresAt;
}
