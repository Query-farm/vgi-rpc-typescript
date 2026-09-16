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
/** Parse one HTTP response's VGI capability headers into a validated snapshot. */
export declare function parseCapabilitiesFromHeaders(headers: Headers): HttpServerCapabilities;
/** Every VGI HTTP response, not only discovery, must repeat exact support. */
export declare function requireResponseBudgetSupport(headers: Headers): HttpServerCapabilities;
/** Probe the server's auth-exempt OPTIONS endpoint for HTTP transport capabilities. */
export declare function discoverHttpCapabilities(baseUrl: string, prefix: string, authorization?: string, acceptedMaxResponseBytes?: number, fetchFn?: typeof globalThis.fetch): Promise<HttpServerCapabilities>;
/** Return whether a cached capability snapshot remains usable without another probe. */
export declare function isCapabilitySnapshotFresh(snapshot: HttpServerCapabilities | null): boolean;
//# sourceMappingURL=capabilities.d.ts.map