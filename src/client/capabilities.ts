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
  /** Monotonic-time-ish epoch (ms) at which this snapshot should be re-probed. */
  cacheExpiresAt: number | null;
}

const MAX_REQUEST_BYTES_HEADER = "VGI-Max-Request-Bytes";
const UPLOAD_URL_HEADER = "VGI-Upload-URL-Support";
const MAX_UPLOAD_BYTES_HEADER = "VGI-Max-Upload-Bytes";
const MAX_RESPONSE_BYTES_HEADER = "VGI-Max-Response-Bytes";
const ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER = "VGI-Accept-Max-Response-Bytes-Support";

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

  return {
    maxRequestBytes: parseHeaderInt(headers, MAX_REQUEST_BYTES_HEADER),
    uploadUrlSupport,
    maxUploadBytes: parseHeaderInt(headers, MAX_UPLOAD_BYTES_HEADER),
    maxResponseBytes: parseHeaderInt(headers, MAX_RESPONSE_BYTES_HEADER, true),
    acceptMaxResponseBytesSupport:
      (headers.get(ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER) ??
        headers.get(ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER.toLowerCase())) === "true",
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

/** Probe the server's auth-exempt OPTIONS endpoint for HTTP transport capabilities. */
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
    method: "OPTIONS",
    headers,
  });
  if (!resp.ok) {
    throw new RpcError("TransportError", `Capability discovery failed: HTTP ${resp.status}`, "");
  }
  // OPTIONS commonly answers 204; any successful 2xx status is valid.
  return parseCapabilitiesFromHeaders(resp.headers);
}

/** Return whether a cached capability snapshot remains usable without another probe. */
export function isCapabilitySnapshotFresh(snapshot: HttpServerCapabilities | null): boolean {
  if (!snapshot) return false;
  if (snapshot.cacheExpiresAt == null) return true;
  return Date.now() < snapshot.cacheExpiresAt;
}
