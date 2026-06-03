/**
 * Decompress gzip-encoded data, optionally bounded by `maxOutputSize`.
 *
 * The gzip footer's ISIZE field is mod 2^32 so it can't be trusted for a
 * pre-check — we bound output incrementally during streaming decode.
 */
export declare function gzipDecompress(data: Uint8Array, maxOutputSize?: number): Promise<Uint8Array<ArrayBuffer>>;
/** Compress data with gzip. `level` is accepted for API parity with zstd but ignored — the Web API doesn't expose a level. */
export declare function gzipCompress(data: Uint8Array, _level?: number): Promise<Uint8Array<ArrayBuffer>>;
//# sourceMappingURL=gzip.d.ts.map