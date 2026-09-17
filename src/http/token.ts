// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { openBytes, SealError, sealBytes } from "../crypto.js";

const _UTF8 = new TextEncoder();

// On-wire version bytes. The cursor and call tokens carry independent
// version lines because they change for independent reasons. Cursor v5 drops
// the schemas, which moved into the call token; a v4 reader would mis-frame
// it, so the bump turns a rolling deploy's stale token into a clean failure.
const TOKEN_VERSION = 5;
const CALL_TOKEN_VERSION = 2;

/** Length of the random per-stream id minted at `/init`. */
export const CALL_ID_LEN = 16;

/** Scope for a token that belongs to the server rather than to any one hosted
 *  protocol -- sticky-session tokens, today.
 *
 *  Not a protocol name: the grammar forbids a leading NUL, so no hosted
 *  protocol can ever collide with it. */
export const SERVER_SCOPE = "\u0000server";

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

// AAD prefixes. The state and call lines are versioned independently because
// they change for independent reasons; the prefix is fixed-length and
// therefore prefix-unambiguous with respect to the variable-length identity
// tail that follows. The version numbering currently matches the Python
// reference, so the two ports happen to construct byte-identical associated
// data -- but that is not a contract and must not be relied on. Associated
// data never crosses the wire and a sealed token is only ever opened by the
// implementation that minted it, so this construction is internal to this
// worker framework and MAY diverge (WIRE_PROTOCOL.md §5c). Several ports
// already stamp different prefixes.
const AAD_PREFIX = _UTF8.encode("vgi_rpc.state.v6\0");
const BOUND_AAD_PREFIX = _UTF8.encode("vgi_rpc.state.v7\0");
const CALL_AAD_PREFIX = _UTF8.encode("vgi_rpc.call.v3\0");
const BOUND_CALL_AAD_PREFIX = _UTF8.encode("vgi_rpc.call.v4\0");

/**
 * Build the AEAD associated data that binds a state token to its issuing
 * principal and to the protocol that owns its stream. Anonymous and
 * authenticated tokens produce distinct AAD strings, so an anonymous token
 * cannot be opened by a named identity (and vice versa).
 */
export function computeAad(scope: TokenScope): Uint8Array {
  return scope.evidenceBinding
    ? boundAadWith(BOUND_AAD_PREFIX, scope, scope.evidenceBinding)
    : aadWith(AAD_PREFIX, scope);
}

/**
 * {@link computeAad}'s counterpart for call tokens. The prefix differs
 * deliberately, so a call token and a cursor token are not interchangeable
 * even for the same principal: presenting one where the other is expected
 * fails the AEAD tag check rather than decoding into a payload the reader
 * would misinterpret.
 */
export function computeCallAad(scope: TokenScope): Uint8Array {
  return scope.evidenceBinding
    ? boundAadWith(BOUND_CALL_AAD_PREFIX, scope, scope.evidenceBinding)
    : aadWith(CALL_AAD_PREFIX, scope);
}

/** The protocol tail, appended last so it is unambiguous against the
 *  variable-length identity that precedes it. */
function scopeTail(protocol: string): Uint8Array {
  return _UTF8.encode(`\0${protocol}`);
}

function boundAadWith(prefix: Uint8Array, scope: TokenScope, evidenceBinding: string): Uint8Array {
  const binding = _UTF8.encode(evidenceBinding);
  const tail = scopeTail(scope.protocol);
  if (scope.principal === null || scope.principal === undefined) {
    return concatBytes(prefix, _UTF8.encode("\0anonymous\0"), binding, tail);
  }
  return concatBytes(
    prefix,
    new Uint8Array([1]),
    _UTF8.encode(scope.domain ?? ""),
    new Uint8Array([0]),
    _UTF8.encode(scope.principal),
    new Uint8Array([0]),
    binding,
    tail,
  );
}

function aadWith(prefix: Uint8Array, scope: TokenScope): Uint8Array {
  const tail = scopeTail(scope.protocol);
  if (!scope.principal) {
    return concatBytes(prefix, _UTF8.encode("\0anonymous"), tail);
  }
  const pBytes = _UTF8.encode(scope.principal);
  const identity = new Uint8Array(1 + pBytes.length);
  identity[0] = 0x01;
  identity.set(pBytes, 1);
  return concatBytes(prefix, identity, tail);
}

// Base64 helpers — `btoa`/`atob` exist on Node 16+, Bun, and workerd; we work
// in chunks to stay below the per-call argument limit (Latin-1 only, so we
// move byte-by-byte through `String.fromCharCode`).

export function bytesToBase64(bytes: Uint8Array): string {
  let s = "";
  for (let i = 0; i < bytes.length; i += 0x8000) {
    s += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  }
  return btoa(s);
}

export function base64ToBytes(b64: string): Uint8Array {
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

// Little-endian writers/readers — Buffer was the previous abstraction; we now
// touch DataView for portability across Node, Bun, and workerd.

function writeU32LE(view: DataView, offset: number, value: number): void {
  view.setUint32(offset, value, /* littleEndian */ true);
}

function writeU64LE(view: DataView, offset: number, value: bigint): void {
  view.setBigUint64(offset, value, /* littleEndian */ true);
}

function readU32LE(view: DataView, offset: number): number {
  return view.getUint32(offset, /* littleEndian */ true);
}

function readU64LE(view: DataView, offset: number): bigint {
  return view.getBigUint64(offset, /* littleEndian */ true);
}

function concatBytes(...parts: Uint8Array[]): Uint8Array {
  let total = 0;
  for (const p of parts) total += p.length;
  const out = new Uint8Array(total);
  let offset = 0;
  for (const p of parts) {
    out.set(p, offset);
    offset += p.length;
  }
  return out;
}

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
export function packStateToken(
  stateBytes: Uint8Array,
  callId: Uint8Array,
  tokenKey: Uint8Array,
  scope: TokenScope,
  createdAt?: number,
): string {
  if (tokenKey.length !== 32) {
    throw new Error("XChaCha20-Poly1305 token key must be 32 bytes");
  }
  const now = createdAt ?? Math.floor(Date.now() / 1000);

  const plaintext = new Uint8Array(8 + CALL_ID_LEN + 4 + stateBytes.length);
  const view = new DataView(plaintext.buffer);
  let offset = 0;

  writeU64LE(view, offset, BigInt(now));
  offset += 8;
  plaintext.set(callId, offset);
  offset += CALL_ID_LEN;

  writeU32LE(view, offset, stateBytes.length);
  offset += 4;
  plaintext.set(stateBytes, offset);

  const wire = sealBytes(plaintext, tokenKey, {
    aad: computeAad(scope),
    version: TOKEN_VERSION,
  });
  return bytesToBase64(wire);
}

/**
 * Seal the half of a stream's state that is fixed for the life of the call —
 * the resolved schemas — plus the `callId` binding it to its cursors. Minted
 * once, by `/init`; never re-issued.
 */
export function packCallToken(
  callId: Uint8Array,
  schemaBytes: Uint8Array,
  inputSchemaBytes: Uint8Array,
  tokenKey: Uint8Array,
  scope: TokenScope,
  createdAt?: number,
  responseBudget?: { responseLimitBytes?: number; preferredResponseBytes?: number },
): string {
  if (tokenKey.length !== 32) {
    throw new Error("XChaCha20-Poly1305 token key must be 32 bytes");
  }
  const now = createdAt ?? Math.floor(Date.now() / 1000);

  const plaintext = new Uint8Array(8 + CALL_ID_LEN + 4 + schemaBytes.length + 4 + inputSchemaBytes.length + 16);
  const view = new DataView(plaintext.buffer);
  let offset = 0;

  writeU64LE(view, offset, BigInt(now));
  offset += 8;
  plaintext.set(callId, offset);
  offset += CALL_ID_LEN;

  writeU32LE(view, offset, schemaBytes.length);
  offset += 4;
  plaintext.set(schemaBytes, offset);
  offset += schemaBytes.length;

  writeU32LE(view, offset, inputSchemaBytes.length);
  offset += 4;
  plaintext.set(inputSchemaBytes, offset);
  offset += inputSchemaBytes.length;

  writeU64LE(view, offset, BigInt(responseBudget?.responseLimitBytes ?? 0));
  offset += 8;
  writeU64LE(view, offset, BigInt(responseBudget?.preferredResponseBytes ?? 0));

  const wire = sealBytes(plaintext, tokenKey, {
    aad: computeCallAad(scope),
    version: CALL_TOKEN_VERSION,
  });
  return bytesToBase64(wire);
}

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
export function unpackStateToken(
  tokenBase64: string,
  tokenKey: Uint8Array,
  tokenTtl: number,
  scope: TokenScope,
): UnpackedToken {
  let raw: Uint8Array;
  try {
    raw = base64ToBytes(tokenBase64);
  } catch {
    throw new Error("Malformed state token");
  }
  // Pre-check the envelope version separately so callers can distinguish
  // "wrong format" from "tampered". Mirrors the pre-refactor error shape.
  if (raw.length >= 1 && raw[0] !== TOKEN_VERSION) {
    throw new Error(`Unsupported state token version: ${raw[0]}`);
  }
  let plaintext: Uint8Array;
  try {
    plaintext = openBytes(raw, tokenKey, {
      aad: computeAad(scope),
      version: TOKEN_VERSION,
    });
  } catch (err) {
    if (err instanceof SealError) {
      throw new Error("State token signature verification failed");
    }
    throw err;
  }
  if (plaintext.length < 8 + CALL_ID_LEN) {
    throw new Error("State token truncated");
  }

  // Copy each bytes section into a freshly-allocated Uint8Array with
  // byteOffset=0. arrow-js's schema deserializer wraps the result as Int32Array
  // and throws 'RangeError: Byte offset is not aligned' if the slice happens
  // to start at a non-4-aligned offset. Copying normalizes the alignment.
  const view = new DataView(plaintext.buffer, plaintext.byteOffset, plaintext.byteLength);
  let offset = 0;
  const copyAligned = (start: number, len: number) => {
    const out = new Uint8Array(len);
    out.set(plaintext.subarray(start, start + len));
    return out;
  };

  const createdAt = Number(readU64LE(view, offset));
  offset += 8;

  if (tokenTtl > 0) {
    const now = Math.floor(Date.now() / 1000);
    if (now - createdAt > tokenTtl) {
      throw new Error("State token expired");
    }
  }

  const callId = copyAligned(offset, CALL_ID_LEN);
  offset += CALL_ID_LEN;

  const stateLen = readU32LE(view, offset);
  offset += 4;
  if (offset + stateLen > plaintext.length) {
    throw new Error("State token truncated (state)");
  }
  const stateBytes = copyAligned(offset, stateLen);

  return { stateBytes, callId, createdAt };
}

/**
 * Open and verify a call token, returning it paired with its embedded
 * `callId` so the caller can check it against the cursor that named it.
 */
export function unpackCallToken(
  token: string,
  tokenKey: Uint8Array,
  scope: TokenScope,
  tokenTtl = 0,
): { callId: Uint8Array; call: ResolvedCall } {
  const raw = base64ToBytes(token);
  if (raw.length >= 1 && raw[0] !== CALL_TOKEN_VERSION) {
    throw new Error(`Unsupported call token version ${raw[0]}`);
  }
  let plaintext: Uint8Array;
  try {
    plaintext = openBytes(raw, tokenKey, {
      aad: computeCallAad(scope),
      version: CALL_TOKEN_VERSION,
    });
  } catch (err) {
    if (err instanceof SealError) {
      throw new Error("State token signature verification failed");
    }
    throw err;
  }
  if (plaintext.length < 8 + CALL_ID_LEN) {
    throw new Error("State token truncated");
  }

  const view = new DataView(plaintext.buffer, plaintext.byteOffset, plaintext.byteLength);
  const copyAligned = (start: number, len: number) => {
    const out = new Uint8Array(len);
    out.set(plaintext.subarray(start, start + len));
    return out;
  };

  let offset = 0;
  const createdAt = Number(readU64LE(view, offset));
  offset += 8;
  if (tokenTtl > 0) {
    const now = Math.floor(Date.now() / 1000);
    if (now - createdAt > tokenTtl) {
      throw new Error("State token expired");
    }
  }

  const callId = copyAligned(offset, CALL_ID_LEN);
  offset += CALL_ID_LEN;

  const schemaLen = readU32LE(view, offset);
  offset += 4;
  if (offset + schemaLen > plaintext.length) {
    throw new Error("State token truncated (schema)");
  }
  const schemaBytes = copyAligned(offset, schemaLen);
  offset += schemaLen;

  const inputSchemaLen = readU32LE(view, offset);
  offset += 4;
  if (offset + inputSchemaLen > plaintext.length) {
    throw new Error("State token truncated (input schema)");
  }
  const inputSchemaBytes = copyAligned(offset, inputSchemaLen);
  offset += inputSchemaLen;
  if (offset + 16 !== plaintext.length) {
    throw new Error("State token truncated (response budget)");
  }
  const responseLimitRaw = readU64LE(view, offset);
  offset += 8;
  const preferredResponseRaw = readU64LE(view, offset);
  if (responseLimitRaw > BigInt(Number.MAX_SAFE_INTEGER) || preferredResponseRaw > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error("State token contains an unsafe response budget");
  }
  const responseLimitBytes = Number(responseLimitRaw) || undefined;
  const preferredResponseBytes = Number(preferredResponseRaw) || undefined;

  return { callId, call: { schemaBytes, inputSchemaBytes, responseLimitBytes, preferredResponseBytes } };
}
