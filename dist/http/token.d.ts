/**
 * Build the AEAD associated data that binds a state token to its issuing
 * principal. Anonymous and authenticated tokens produce distinct AAD
 * strings, so an anonymous token cannot be opened by a named identity
 * (and vice versa).
 */
export declare function computeAad(principal: string | null | undefined): Uint8Array;
export declare function bytesToBase64(bytes: Uint8Array): string;
export declare function base64ToBytes(b64: string): Uint8Array;
/**
 * Seal a state token with XChaCha20-Poly1305 AEAD (v4 wire format).
 *
 * Layout (base64-encoded):
 *
 * ```
 *   [1B  version=4]
 *   [24B XChaCha20-Poly1305 nonce (random)]
 *   [..  ciphertext + 16B Poly1305 tag]
 *        plaintext:
 *          [8B  created_at uint64 LE]
 *          [4B  state_len uint32 LE]   [state_len bytes]
 *          [4B  schema_len uint32 LE]  [schema_len bytes]
 *          [4B  input_schema_len LE]   [input_schema_len bytes]
 * ```
 *
 * `created_at` lives inside the ciphertext so TTL enforcement runs after
 * authenticity. The version byte is informational (a self-describing
 * format marker); a tampered version byte still fails decryption because
 * we use the matching algorithm for that version. `principal` is bound
 * via AEAD associated data so a token minted for one identity fails
 * decryption when presented by another.
 */
export declare function packStateToken(stateBytes: Uint8Array, schemaBytes: Uint8Array, inputSchemaBytes: Uint8Array, tokenKey: Uint8Array, principal: string | null | undefined, createdAt?: number): string;
/** Decrypted payload of a state token, as returned by {@link unpackStateToken}. */
export interface UnpackedToken {
    /** Serialized stream-state bytes carried by the token. */
    stateBytes: Uint8Array;
    /** Serialized output-schema IPC bytes. */
    schemaBytes: Uint8Array;
    /** Serialized input-schema IPC bytes (exchange streams). */
    inputSchemaBytes: Uint8Array;
    /** Unix epoch seconds at which the token was minted (used for TTL checks). */
    createdAt: number;
}
/**
 * Open and verify a state token. Decryption (which checks the Poly1305
 * tag) authenticates the payload; any tampering, wrong key, or AAD
 * mismatch (e.g. cross-principal replay) surfaces as a uniform
 * "signature verification failed" error so callers cannot distinguish
 * failure modes via timing or message content.
 *
 * Throws on tampered, expired, malformed, or unknown-version tokens.
 */
export declare function unpackStateToken(tokenBase64: string, tokenKey: Uint8Array, tokenTtl: number, principal: string | null | undefined): UnpackedToken;
//# sourceMappingURL=token.d.ts.map