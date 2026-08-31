export interface UploadUrlPair {
    uploadUrl: string;
    downloadUrl: string;
    expiresAt: Date;
}
/**
 * POST `__upload_url__/init` and return the requested number of pre-signed
 * URL pairs. Server must have an `uploadUrlProvider` configured; otherwise
 * the route returns 404 and we surface that as `RpcError("NotSupported")`.
 */
export declare function requestUploadUrls(baseUrl: string, prefix: string, count: number, authorization?: string, fetchFn?: typeof globalThis.fetch): Promise<UploadUrlPair[]>;
export interface ExternalizeOptions {
    baseUrl: string;
    prefix: string;
    authorization?: string;
    /** Optional per-URL validator; throw to reject. */
    urlValidator?: ((url: string) => void) | null;
    fetch?: typeof globalThis.fetch;
}
/**
 * Upload *body* via a server-vended URL and return the pointer-batch body
 * that should be sent in place of the original. Throws if the server does
 * not advertise upload-URL support or the upload fails.
 */
export declare function externalizeRequestBody(body: Uint8Array, opts: ExternalizeOptions): Promise<Uint8Array>;
//# sourceMappingURL=uploadUrl.d.ts.map