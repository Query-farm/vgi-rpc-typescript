/**
 * External storage support for large Arrow IPC batches.
 *
 * When a batch exceeds a configurable threshold, it is serialized to IPC,
 * optionally compressed with zstd, and uploaded to pluggable storage.
 * The batch is replaced with a zero-row "pointer batch" containing the
 * download URL and SHA-256 checksum in metadata.
 */
import { type VgiBatch, type VgiSchema } from "./arrow/index.js";
/** Pluggable storage backend for uploading large batches. */
export interface ExternalStorage {
    /** Upload IPC data and return a URL for retrieval. */
    upload(data: Uint8Array, contentEncoding: string): Promise<string>;
}
/** A pre-signed PUT/GET URL pair for client-side data upload. */
export interface UploadUrl {
    /** Pre-signed PUT URL the client uploads to. */
    uploadUrl: string;
    /** Pre-signed GET URL the server fetches from. */
    downloadUrl: string;
    /** Expiration time (UTC) for the URL pair. */
    expiresAt: Date;
}
/**
 * Generates pre-signed upload URL pairs for client-vended externalization.
 *
 * Implementations must be safe to call from multiple concurrent requests.
 * Object lifecycle is the operator's responsibility — uploaded objects are
 * not automatically deleted by vgi-rpc.
 */
export interface UploadUrlProvider {
    /** Allocate one upload/download URL pair. */
    generateUploadUrl(): Promise<UploadUrl> | UploadUrl;
}
/** Configuration for external storage of large batches. */
export interface ExternalLocationConfig {
    /** Storage backend for uploading. */
    storage: ExternalStorage;
    /** Minimum batch byte size to trigger externalization. Default: 1MB. */
    externalizeThresholdBytes?: number;
    /** Optional zstd compression for uploaded data. */
    compression?: {
        /** Compression algorithm; only `"zstd"` is currently supported. */
        algorithm: "zstd";
        /** zstd compression level. Default: 3. */
        level?: number;
    };
    /** URL validator called before fetching. Throw to reject. Default: HTTPS-only. */
    urlValidator?: ((url: string) => void) | null;
}
/** Default validator that rejects non-HTTPS URLs. */
export declare function httpsOnlyValidator(url: string): void;
/** Returns true if the batch is a zero-row pointer to external data. */
export declare function isExternalLocationBatch(batch: VgiBatch): boolean;
/** Create a zero-row pointer batch with location URL and optional SHA-256. */
export declare function makeExternalLocationBatch(schema: VgiSchema, url: string, sha256?: string): VgiBatch;
/**
 * Maybe externalize a batch if it exceeds the threshold.
 * Returns the original batch unchanged if below threshold or no config.
 */
export declare function maybeExternalizeBatch(batch: VgiBatch, config?: ExternalLocationConfig | null): Promise<VgiBatch>;
/**
 * Resolve an external pointer batch by fetching the data from the URL.
 * Returns the original batch unchanged if not a pointer or no config.
 */
export declare function resolveExternalLocation(batch: VgiBatch, config?: ExternalLocationConfig | null): Promise<VgiBatch>;
//# sourceMappingURL=external.d.ts.map