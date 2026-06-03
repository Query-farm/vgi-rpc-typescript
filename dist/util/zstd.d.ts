/** Return true when the current runtime can produce zstd-compressed output.
 *
 *  Bun has `Bun.zstdCompressSync`; Node ≥22.15 / Deno ≥2.6.9 expose it via
 *  `node:zlib`. Other runtimes (workerd, older Node) have no encoder. The
 *  fzstd fallback is decompress-only so it doesn't count.
 */
export declare function isZstdCompressAvailable(): boolean;
/** Compress data with zstd at the given level (1-22). */
export declare function zstdCompress(data: Uint8Array, level: number): Promise<Uint8Array<ArrayBuffer>>;
/**
 * Decompress zstd-compressed data, optionally bounding the output size.
 *
 * Zstd frames carry the decompressed size in the header and decompressors
 * trust it eagerly: a ~3 KB compressed body claiming 100 MB output would
 * allocate 100 MB. When `maxOutputSize` is supplied, this helper:
 *
 * 1. Reads `Frame_Content_Size` from the frame header. If declared and
 *    above the cap, refuses *before* allocation with a clear error.
 * 2. Decompresses, then asserts the actual output size is also under the
 *    cap (covers frames whose size is not in the header — a streaming
 *    cap would be tighter, but neither Bun.zstdDecompressSync nor
 *    node:zlib's sync API exposes one, so we use the post-check).
 *
 * Mirrors the Python server-side fix in `_decompress_body` and the
 * client-side fix in `external_fetch.fetch_url`.
 */
export declare function zstdDecompress(data: Uint8Array, maxOutputSize?: number): Promise<Uint8Array<ArrayBuffer>>;
//# sourceMappingURL=zstd.d.ts.map