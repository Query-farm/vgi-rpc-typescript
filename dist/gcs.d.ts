/**
 * Google Cloud Storage backend for external storage of large Arrow IPC batches.
 *
 * Requires `@google-cloud/storage` as a peer dependency.
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
 * });
 * ```
 */
import type { ExternalStorage } from "./external.js";
/** Configuration for the GCS storage backend. */
export interface GCSStorageConfig {
    /** GCS bucket name. */
    bucket: string;
    /** Key prefix for uploaded objects. Default: "vgi-rpc/". */
    prefix?: string;
    /** Lifetime of signed GET URLs in seconds. Default: 3600 (1 hour). */
    presignExpirySeconds?: number;
    /** GCS project ID. If omitted, uses Application Default Credentials. */
    projectId?: string;
}
/**
 * Create a GCS-backed ExternalStorage.
 *
 * Lazily imports `@google-cloud/storage` on first upload to avoid
 * loading the SDK unless needed.
 */
export declare function createGCSStorage(config: GCSStorageConfig): ExternalStorage;
//# sourceMappingURL=gcs.d.ts.map