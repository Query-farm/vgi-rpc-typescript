/**
 * S3 storage backend for external storage of large Arrow IPC batches.
 *
 * Requires `@aws-sdk/client-s3` and `@aws-sdk/s3-request-presigner`
 * as peer dependencies.
 *
 * @example
 * ```typescript
 * import { createS3Storage } from "@query-farm/vgi-rpc/s3";
 *
 * const storage = createS3Storage({
 *   bucket: "my-bucket",
 *   prefix: "vgi-rpc/",
 * });
 * const handler = createHttpHandler(protocol, {
 *   externalLocation: { storage, externalizeThresholdBytes: 1_048_576 },
 * });
 * ```
 */
import type { ExternalStorage } from "./external.js";
/** Configuration for the S3 storage backend. */
export interface S3StorageConfig {
    /** S3 bucket name. */
    bucket: string;
    /** Key prefix for uploaded objects. Default: "vgi-rpc/". */
    prefix?: string;
    /** Lifetime of pre-signed GET URLs in seconds. Default: 3600 (1 hour). */
    presignExpirySeconds?: number;
    /** AWS region. If omitted, uses default SDK config. */
    region?: string;
    /** Custom S3 endpoint URL (for MinIO, LocalStack, etc.). */
    endpointUrl?: string;
    /** Force path-style addressing (required for some S3-compatible services). */
    forcePathStyle?: boolean;
}
/**
 * Create an S3-backed ExternalStorage.
 *
 * Lazily imports `@aws-sdk/client-s3` and `@aws-sdk/s3-request-presigner`
 * on first upload to avoid loading the AWS SDK unless needed.
 */
export declare function createS3Storage(config: S3StorageConfig): ExternalStorage;
//# sourceMappingURL=s3.d.ts.map