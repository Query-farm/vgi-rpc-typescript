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
export declare function parseCapabilitiesFromHeaders(headers: Headers): HttpServerCapabilities;
/** Every VGI HTTP response, not only discovery, must repeat exact support. */
export declare function requireResponseBudgetSupport(headers: Headers): HttpServerCapabilities;
export declare function discoverHttpCapabilities(baseUrl: string, prefix: string, authorization?: string, acceptedMaxResponseBytes?: number, fetchFn?: typeof globalThis.fetch): Promise<HttpServerCapabilities>;
export declare function isCapabilitySnapshotFresh(snapshot: HttpServerCapabilities | null): boolean;
//# sourceMappingURL=capabilities.d.ts.map