// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { hmacSha256, hmacSha256Verify } from "../util/web-crypto.js";

const _UTF8 = new TextEncoder();

/**
 * Derive a per-principal signing key so that a state token issued to one
 * authenticated identity cannot be replayed by another. When `principal` is
 * null or empty (anonymous request), the base key is returned unchanged so
 * that wire-compatible behavior with non-authenticated peers is preserved.
 *
 * The derivation domain-separator ("vgi_rpc.principal\0") prevents collision
 * with other key-derivation uses of the same base key (e.g. oauth-pkce).
 */
export async function derivePrincipalKey(
  signingKey: Uint8Array,
  principal: string | null | undefined,
): Promise<Uint8Array> {
  if (!principal) return signingKey;
  return hmacSha256(signingKey, _UTF8.encode(`vgi_rpc.principal\0${principal}`));
}

const TOKEN_VERSION = 2;
const HMAC_LEN = 32;
// 1 (version) + 8 (created_at) + 4*3 (three length prefixes) + 32 (hmac)
const MIN_TOKEN_LEN = 1 + 8 + 12 + HMAC_LEN;

// Base64 helpers — `btoa`/`atob` exist on Node 16+, Bun, and workerd; we work
// in chunks to stay below the per-call argument limit (Latin-1 only, so we
// move byte-by-byte through `String.fromCharCode`).

function bytesToBase64(bytes: Uint8Array): string {
  let s = "";
  for (let i = 0; i < bytes.length; i += 0x8000) {
    s += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  }
  return btoa(s);
}

function base64ToBytes(b64: string): Uint8Array {
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
 * Pack a state token matching the Python v2 wire format.
 *
 * Layout:
 *   [1B version=2]
 *   [8B created_at uint64 LE (seconds since epoch)]
 *   [4B state_len uint32 LE] [state_len bytes]
 *   [4B schema_len uint32 LE] [schema_len bytes]
 *   [4B input_schema_len uint32 LE] [input_schema_len bytes]
 *   [32B HMAC-SHA256(signing_key, all above bytes)]
 */
export async function packStateToken(
  stateBytes: Uint8Array,
  schemaBytes: Uint8Array,
  inputSchemaBytes: Uint8Array,
  signingKey: Uint8Array,
  createdAt?: number,
): Promise<string> {
  const now = createdAt ?? Math.floor(Date.now() / 1000);

  const payloadLen = 1 + 8 + 4 + stateBytes.length + 4 + schemaBytes.length + 4 + inputSchemaBytes.length;
  const payload = new Uint8Array(payloadLen);
  const view = new DataView(payload.buffer);
  let offset = 0;

  // version
  payload[offset] = TOKEN_VERSION;
  offset += 1;

  // created_at as uint64 LE
  writeU64LE(view, offset, BigInt(now));
  offset += 8;

  // state
  writeU32LE(view, offset, stateBytes.length);
  offset += 4;
  payload.set(stateBytes, offset);
  offset += stateBytes.length;

  // output schema
  writeU32LE(view, offset, schemaBytes.length);
  offset += 4;
  payload.set(schemaBytes, offset);
  offset += schemaBytes.length;

  // input schema
  writeU32LE(view, offset, inputSchemaBytes.length);
  offset += 4;
  payload.set(inputSchemaBytes, offset);
  offset += inputSchemaBytes.length;

  // HMAC
  const mac = await hmacSha256(signingKey, payload);
  return bytesToBase64(concatBytes(payload, mac));
}

export interface UnpackedToken {
  stateBytes: Uint8Array;
  schemaBytes: Uint8Array;
  inputSchemaBytes: Uint8Array;
  createdAt: number;
}

/**
 * Unpack and verify a state token.
 * Throws on tampered, expired, or malformed tokens.
 */
export async function unpackStateToken(
  tokenBase64: string,
  signingKey: Uint8Array,
  tokenTtl: number,
): Promise<UnpackedToken> {
  const token = base64ToBytes(tokenBase64);

  if (token.length < MIN_TOKEN_LEN) {
    throw new Error("State token too short");
  }

  // Split payload and mac
  const payload = token.subarray(0, token.length - HMAC_LEN);
  const receivedMac = token.subarray(token.length - HMAC_LEN);

  // Verify HMAC first (before inspecting any fields). `crypto.subtle.verify`
  // is constant-time on conforming runtimes.
  const ok = await hmacSha256Verify(signingKey, payload, receivedMac);
  if (!ok) {
    throw new Error("State token HMAC verification failed");
  }

  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  let offset = 0;

  // Version
  const version = payload[offset];
  offset += 1;
  if (version !== TOKEN_VERSION) {
    throw new Error(`Unsupported state token version: ${version}`);
  }

  // created_at
  const createdAt = Number(readU64LE(view, offset));
  offset += 8;

  // TTL check
  if (tokenTtl > 0) {
    const now = Math.floor(Date.now() / 1000);
    if (now - createdAt > tokenTtl) {
      throw new Error("State token expired");
    }
  }

  // Copy each bytes section into a freshly-allocated Uint8Array with
  // byteOffset=0. arrow-js's schema deserializer wraps the result as Int32Array
  // and throws 'RangeError: Byte offset is not aligned' if the slice happens
  // to start at a non-4-aligned offset. Copying normalizes the alignment.
  const copyAligned = (start: number, len: number) => {
    const out = new Uint8Array(len);
    out.set(payload.subarray(start, start + len));
    return out;
  };

  // state bytes
  const stateLen = readU32LE(view, offset);
  offset += 4;
  if (offset + stateLen > payload.length) {
    throw new Error("State token truncated (state)");
  }
  const stateBytes = copyAligned(offset, stateLen);
  offset += stateLen;

  // output schema bytes
  const schemaLen = readU32LE(view, offset);
  offset += 4;
  if (offset + schemaLen > payload.length) {
    throw new Error("State token truncated (schema)");
  }
  const schemaBytes = copyAligned(offset, schemaLen);
  offset += schemaLen;

  // input schema bytes
  const inputSchemaLen = readU32LE(view, offset);
  offset += 4;
  if (offset + inputSchemaLen > payload.length) {
    throw new Error("State token truncated (input schema)");
  }
  const inputSchemaBytes = copyAligned(offset, inputSchemaLen);

  return { stateBytes, schemaBytes, inputSchemaBytes, createdAt };
}
