/** Cryptographically-strong random bytes. */
export declare function randomBytes(length: number): Uint8Array;
/**
 * Constant-time byte-array comparison. Returns false fast on length mismatch
 * (the length itself is not a secret), and otherwise XOR-accumulates without
 * an early return so the comparison takes the same wall time regardless of
 * which byte differs.
 */
export declare function constantTimeEqual(a: Uint8Array, b: Uint8Array): boolean;
/** HMAC-SHA256 over `data` with `key`. Returns a 32-byte tag. */
export declare function hmacSha256(key: Uint8Array, data: Uint8Array): Promise<Uint8Array>;
/**
 * Verify an HMAC-SHA256 tag in constant time. Equivalent to
 * `constantTimeEqual(await hmacSha256(key, data), tag)`, but routes through
 * `crypto.subtle.verify` which is also constant-time on conforming runtimes.
 */
export declare function hmacSha256Verify(key: Uint8Array, data: Uint8Array, tag: Uint8Array): Promise<boolean>;
/** SHA-256 of `data` as raw bytes. */
export declare function sha256(data: Uint8Array): Promise<Uint8Array>;
/** SHA-256 of `data` as lower-case hex. */
export declare function sha256Hex(data: Uint8Array): Promise<string>;
//# sourceMappingURL=web-crypto.d.ts.map