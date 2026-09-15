/** Length of the random per-stream id minted at `/init`. */
export declare const CALL_ID_LEN = 16;
/** Scope for a token that belongs to the server rather than to any one hosted
 *  protocol -- sticky-session tokens, today.
 *
 *  Not a protocol name: the grammar forbids a leading NUL, so no hosted
 *  protocol can ever collide with it. */
export declare const SERVER_SCOPE = "\0server";
/**
 * Everything the AEAD associated data of a state or call token is bound to.
 *
 * Passed as one object rather than four positional strings because every field
 * is a string and a transposition would still compile -- and a token bound to
 * the wrong scope fails open in the only direction that matters: it opens.
 */
export interface TokenScope {
    /**
     * Wire name of the protocol that owns the stream, or {@link SERVER_SCOPE}.
     *
     * Bound into the AAD rather than carried in the plaintext so a
     * cross-protocol continuation fails the AEAD tag check -- rejected exactly
     * as an invalid token, with no comparison code to get wrong and nothing to
     * forget on the call-state cache-hit path, where the call token is never
     * opened at all. The cursor is always opened first, so binding it here
     * covers both paths.
     */
    protocol: string;
    /** The issuing principal; `null` for an anonymous caller. An authenticator
     *  that deliberately uses an empty principal is still authenticated, and the
     *  empty string and `null` produce different AAD. */
    principal: string | null | undefined;
    /** Digest of the resolved transport-peer evidence, when there is any.
     *  Its presence selects the bound AAD prefix. */
    evidenceBinding?: string;
    /** Authentication domain of the issuing principal. */
    domain?: string | null;
}
/**
 * Build the AEAD associated data that binds a state token to its issuing
 * principal and to the protocol that owns its stream. Anonymous and
 * authenticated tokens produce distinct AAD strings, so an anonymous token
 * cannot be opened by a named identity (and vice versa).
 */
export declare function computeAad(scope: TokenScope): Uint8Array;
/**
 * {@link computeAad}'s counterpart for call tokens. The prefix differs
 * deliberately, so a call token and a cursor token are not interchangeable
 * even for the same principal: presenting one where the other is expected
 * fails the AEAD tag check rather than decoding into a payload the reader
 * would misinterpret.
 */
export declare function computeCallAad(scope: TokenScope): Uint8Array;
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
 * we use the matching algorithm for that version. The {@link TokenScope} --
 * principal, auth domain, peer-evidence digest, and the owning protocol -- is
 * bound via AEAD associated data, so a token minted for one identity or one
 * protocol fails decryption when presented under another.
 */
export declare function packStateToken(stateBytes: Uint8Array, callId: Uint8Array, tokenKey: Uint8Array, scope: TokenScope, createdAt?: number): string;
/**
 * Seal the half of a stream's state that is fixed for the life of the call —
 * the resolved schemas — plus the `callId` binding it to its cursors. Minted
 * once, by `/init`; never re-issued.
 */
export declare function packCallToken(callId: Uint8Array, schemaBytes: Uint8Array, inputSchemaBytes: Uint8Array, tokenKey: Uint8Array, scope: TokenScope, createdAt?: number, responseBudget?: {
    responseLimitBytes?: number;
    preferredResponseBytes?: number;
}): string;
/** Decrypted payload of a state token, as returned by {@link unpackStateToken}. */
export interface UnpackedToken {
    /** Serialized stream-state bytes carried by the token. */
    stateBytes: Uint8Array;
    /**
     * The call token this cursor belongs to. Recovered from inside the cursor's
     * ciphertext, so it is authenticated before it is trusted.
     */
    callId: Uint8Array;
    /** Unix epoch seconds at which the token was minted (used for TTL checks). */
    createdAt: number;
}
/**
 * The half of a stream's state fixed for the life of the call — what a
 * cursor's `callId` resolves to, from cache or from the client's echoed call
 * token.
 */
export interface ResolvedCall {
    /** Serialized output-schema IPC bytes. */
    schemaBytes: Uint8Array;
    /** Serialized input-schema IPC bytes (exchange streams). */
    inputSchemaBytes: Uint8Array;
    /** Initial authenticated hard response cap; zero/undefined on no cap. */
    responseLimitBytes?: number;
    /** Initial advisory batching target, sealed with the hard cap. */
    preferredResponseBytes?: number;
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
export declare function unpackStateToken(tokenBase64: string, tokenKey: Uint8Array, tokenTtl: number, scope: TokenScope): UnpackedToken;
/**
 * Open and verify a call token, returning it paired with its embedded
 * `callId` so the caller can check it against the cursor that named it.
 */
export declare function unpackCallToken(token: string, tokenKey: Uint8Array, scope: TokenScope, tokenTtl?: number): {
    callId: Uint8Array;
    call: ResolvedCall;
};
//# sourceMappingURL=token.d.ts.map