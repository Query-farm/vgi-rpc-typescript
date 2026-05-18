// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Generic AEAD seal/open primitives shared by stream-state and sticky-session
 * tokens. Mirrors Python's `vgi_rpc.crypto` module: a tiny envelope around
 * XChaCha20-Poly1305 with a leading version byte so future format bumps stay
 * self-describing.
 *
 * Wire format (returned by {@link sealBytes}, accepted by {@link openBytes}):
 *
 * ```
 *   [1B  version  (1..255)]
 *   [24B nonce    (XChaCha20-Poly1305, random per envelope)]
 *   [..  ciphertext + 16B Poly1305 tag]
 * ```
 *
 * The plaintext frame is fully up to the caller — only the version + nonce +
 * tag overhead is fixed. AAD (`aad`) is bound at the crypto layer so an
 * envelope sealed for one identity fails decryption when presented by another.
 */

import { xchacha20poly1305 } from "@noble/ciphers/chacha.js";
import { randomBytes } from "./util/web-crypto.js";

const NONCE_LEN = 24;
const TAG_LEN = 16;
const VERSION_LEN = 1;
const MIN_ENVELOPE_LEN = VERSION_LEN + NONCE_LEN + TAG_LEN;

/** Thrown by {@link openBytes} for any envelope it cannot open — malformed,
 *  tampered, wrong key, wrong AAD, wrong version, truncated, all surface the
 *  same way so callers cannot distinguish failure modes via error content. */
export class SealError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SealError";
  }
}

/** Normalise a key to 32 bytes by SHA-256 hashing when it isn't already 32B.
 *  Mirrors Python's `normalize_key` so any callers can pass operator-provided
 *  keys of arbitrary length without a separate stretching step. */
export async function normalizeKey(key: Uint8Array): Promise<Uint8Array> {
  if (key.length === 32) return key;
  const digest = await crypto.subtle.digest("SHA-256", key as BufferSource);
  return new Uint8Array(digest);
}

export interface SealOptions {
  /** Associated data bound at the crypto layer — typically a principal or
   *  request-scoped identifier. Must match between seal and open. */
  aad: Uint8Array;
  /** Envelope version byte. Defaults to 1; carry through to {@link openBytes}. */
  version?: number;
}

/** Seal `plaintext` under `key` with AEAD, returning the wire envelope. */
export function sealBytes(plaintext: Uint8Array, key: Uint8Array, opts: SealOptions): Uint8Array {
  if (key.length !== 32) {
    throw new Error("AEAD key must be 32 bytes — call normalizeKey() first");
  }
  const version = opts.version ?? 1;
  if (version < 1 || version > 255) {
    throw new Error(`AEAD envelope version must fit in one byte; got ${version}`);
  }
  const nonce = randomBytes(NONCE_LEN);
  const ciphertext = xchacha20poly1305(key, nonce, opts.aad as Uint8Array).encrypt(plaintext);
  const wire = new Uint8Array(VERSION_LEN + NONCE_LEN + ciphertext.length);
  wire[0] = version;
  wire.set(nonce, VERSION_LEN);
  wire.set(ciphertext, VERSION_LEN + NONCE_LEN);
  return wire;
}

/** Open and verify an envelope produced by {@link sealBytes}. */
export function openBytes(envelope: Uint8Array, key: Uint8Array, opts: SealOptions): Uint8Array {
  if (key.length !== 32) {
    throw new Error("AEAD key must be 32 bytes — call normalizeKey() first");
  }
  if (envelope.length < MIN_ENVELOPE_LEN) {
    throw new SealError("envelope truncated");
  }
  const expectedVersion = opts.version ?? 1;
  if (envelope[0] !== expectedVersion) {
    throw new SealError(`unsupported envelope version: ${envelope[0]}`);
  }
  const nonce = envelope.subarray(VERSION_LEN, VERSION_LEN + NONCE_LEN);
  const ciphertext = envelope.subarray(VERSION_LEN + NONCE_LEN);
  try {
    return xchacha20poly1305(key, nonce, opts.aad as Uint8Array).decrypt(ciphertext);
  } catch {
    throw new SealError("envelope verification failed");
  }
}
