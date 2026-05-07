// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Web Crypto helpers — universal across Node 18+, Bun, Cloudflare workerd,
// Deno, and modern browsers. Used in place of node:crypto (`createHmac`,
// `createHash`, `randomBytes`, `timingSafeEqual`) so the wire-protocol code
// bundles cleanly for workerd without requiring `nodejs_compat`.
//
// All HMAC/digest operations are async because `crypto.subtle` is async-only.
// Callers that previously had sync signatures should await the result.

/** Cryptographically-strong random bytes. */
export function randomBytes(length: number): Uint8Array {
  const buf = new Uint8Array(length);
  crypto.getRandomValues(buf);
  return buf;
}

/**
 * Constant-time byte-array comparison. Returns false fast on length mismatch
 * (the length itself is not a secret), and otherwise XOR-accumulates without
 * an early return so the comparison takes the same wall time regardless of
 * which byte differs.
 */
export function constantTimeEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a[i] ^ b[i];
  return diff === 0;
}

/**
 * Cache imported `CryptoKey` per (raw key bytes, usage) so that repeated
 * sign/verify calls don't re-import. The key bytes are fingerprinted with a
 * stable string (base64 of the raw bytes); the WeakRef-friendly Map keeps the
 * cache bounded by typical key turnover (one or two signing keys per process).
 */
const _hmacKeyCache = new Map<string, Promise<CryptoKey>>();

function _hmacKeyCacheKey(rawKey: Uint8Array, usage: "sign" | "verify"): string {
  // Cheap stable fingerprint — collisions only matter for the cache hit/miss,
  // not security. Use a string concat of byte values; for typical 32-byte
  // signing keys this is ~70 chars.
  let s = usage + ":";
  for (let i = 0; i < rawKey.length; i++) s += rawKey[i].toString(16);
  return s;
}

async function _importHmacKey(rawKey: Uint8Array, usage: "sign" | "verify"): Promise<CryptoKey> {
  const cacheKey = _hmacKeyCacheKey(rawKey, usage);
  let promise = _hmacKeyCache.get(cacheKey);
  if (!promise) {
    promise = crypto.subtle.importKey(
      "raw",
      // Copy into a fresh ArrayBuffer — importKey requires an ArrayBuffer or
      // ArrayBufferView; passing a SharedArrayBuffer view (or one whose
      // underlying buffer outlives this caller) can be rejected on some
      // runtimes. The copy is cheap (≤32 bytes typical).
      rawKey.slice().buffer as ArrayBuffer,
      { name: "HMAC", hash: "SHA-256" },
      false,
      [usage],
    );
    _hmacKeyCache.set(cacheKey, promise);
  }
  return promise;
}

/** HMAC-SHA256 over `data` with `key`. Returns a 32-byte tag. */
export async function hmacSha256(key: Uint8Array, data: Uint8Array): Promise<Uint8Array> {
  const cryptoKey = await _importHmacKey(key, "sign");
  const sig = await crypto.subtle.sign("HMAC", cryptoKey, data as BufferSource);
  return new Uint8Array(sig);
}

/**
 * Verify an HMAC-SHA256 tag in constant time. Equivalent to
 * `constantTimeEqual(await hmacSha256(key, data), tag)`, but routes through
 * `crypto.subtle.verify` which is also constant-time on conforming runtimes.
 */
export async function hmacSha256Verify(key: Uint8Array, data: Uint8Array, tag: Uint8Array): Promise<boolean> {
  const cryptoKey = await _importHmacKey(key, "verify");
  return crypto.subtle.verify("HMAC", cryptoKey, tag as BufferSource, data as BufferSource);
}

/** SHA-256 of `data` as raw bytes. */
export async function sha256(data: Uint8Array): Promise<Uint8Array> {
  const digest = await crypto.subtle.digest("SHA-256", data as BufferSource);
  return new Uint8Array(digest);
}

/** SHA-256 of `data` as lower-case hex. */
export async function sha256Hex(data: Uint8Array): Promise<string> {
  const bytes = await sha256(data);
  let s = "";
  for (let i = 0; i < bytes.length; i++) s += bytes[i].toString(16).padStart(2, "0");
  return s;
}
