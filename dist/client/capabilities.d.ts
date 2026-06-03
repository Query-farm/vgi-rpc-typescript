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
export declare function parseCapabilitiesFromHeaders(headers: Headers): HttpServerCapabilities;
export declare function discoverHttpCapabilities(baseUrl: string, prefix: string, authorization?: string): Promise<HttpServerCapabilities>;
export declare function isCapabilitySnapshotFresh(snapshot: HttpServerCapabilities | null): boolean;
//# sourceMappingURL=capabilities.d.ts.map