// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * HTTP server capability discovery.
 *
 * Mirrors Python's `http_capabilities()`: probes `OPTIONS {prefix}/health`
 * and reads three response headers:
 *   - `VGI-Max-Request-Bytes`  — server-enforced inline request cap
 *   - `VGI-Upload-URL-Support` — "true" when the server vends upload URLs
 *   - `VGI-Max-Upload-Bytes`   — cap on out-of-band upload size
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
  /** Monotonic-time-ish epoch (ms) at which this snapshot should be re-probed. */
  cacheExpiresAt: number | null;
}

const MAX_REQUEST_BYTES_HEADER = "VGI-Max-Request-Bytes";
const UPLOAD_URL_HEADER = "VGI-Upload-URL-Support";
const MAX_UPLOAD_BYTES_HEADER = "VGI-Max-Upload-Bytes";

function parseHeaderInt(headers: Headers, name: string): number | null {
  const raw = headers.get(name) ?? headers.get(name.toLowerCase());
  if (raw == null) return null;
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

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
    cacheExpiresAt,
  };
}

export async function discoverHttpCapabilities(
  baseUrl: string,
  prefix: string,
  authorization?: string,
): Promise<HttpServerCapabilities> {
  const headers: Record<string, string> = {};
  if (authorization) headers.Authorization = authorization;
  const resp = await fetch(`${baseUrl}${prefix}/health`, {
    method: "OPTIONS",
    headers,
  });
  // Capability headers are advertised on every response; we don't require 200.
  return parseCapabilitiesFromHeaders(resp.headers);
}

export function isCapabilitySnapshotFresh(snapshot: HttpServerCapabilities | null): boolean {
  if (!snapshot) return false;
  if (snapshot.cacheExpiresAt == null) return true;
  return Date.now() < snapshot.cacheExpiresAt;
}
