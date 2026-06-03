/** Thrown by {@link openBytes} for any envelope it cannot open — malformed,
 *  tampered, wrong key, wrong AAD, wrong version, truncated, all surface the
 *  same way so callers cannot distinguish failure modes via error content. */
export declare class SealError extends Error {
    constructor(message: string);
}
/** Normalise a key to 32 bytes by SHA-256 hashing when it isn't already 32B.
 *  Mirrors Python's `normalize_key` so any callers can pass operator-provided
 *  keys of arbitrary length without a separate stretching step. */
export declare function normalizeKey(key: Uint8Array): Promise<Uint8Array>;
export interface SealOptions {
    /** Associated data bound at the crypto layer — typically a principal or
     *  request-scoped identifier. Must match between seal and open. */
    aad: Uint8Array;
    /** Envelope version byte. Defaults to 1; carry through to {@link openBytes}. */
    version?: number;
}
/** Seal `plaintext` under `key` with AEAD, returning the wire envelope. */
export declare function sealBytes(plaintext: Uint8Array, key: Uint8Array, opts: SealOptions): Uint8Array;
/** Open and verify an envelope produced by {@link sealBytes}. */
export declare function openBytes(envelope: Uint8Array, key: Uint8Array, opts: SealOptions): Uint8Array;
//# sourceMappingURL=crypto.d.ts.map