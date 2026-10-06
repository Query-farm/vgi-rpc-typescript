// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Sealed grants: the framework's own `issue_grant` credential, and its verifier.
 *
 * `vgi_rpc.Identity.v1`'s `issue_grant` mints a standing delegation that
 * unattended automation later presents *as an ordinary bearer*. Until this
 * module nothing accepted one. A sealed grant closes the loop without storage
 * and without author code: when a deployment configures a **grant key**, the
 * framework mints grants itself (unless the worker supplies its own
 * `mintGrant`) and accepts them back as bearer credentials. When it does not,
 * nothing changes.
 *
 * Normative: the reference's `IDENTITY_V1_SPEC.md` §9 and `WIRE_PROTOCOL.md`
 * §16; `vgi_rpc/conformance/grant_token_vectors.json` pins the bytes.
 *
 * ```
 * token    = "vgig1." base64url_nopad( kid(8) || envelope )
 * envelope = 0x01 || nonce(24) || XChaCha20-Poly1305(payload, aad)   ; the state-token envelope
 * kid      = SHA-256("vgi_rpc.grant.kid.v1" 0x00 || key)[0:8]
 * aad      = "vgi_rpc.grant.v1" 0x00 || kid || UTF-8(audience)
 * payload  = issued_at i64 | expires_at i64 | grant_id s | principal s | purpose s
 *            | scope_count u16 | scope s * scope_count        ; little-endian, s = u16 len || UTF-8
 * ```
 *
 * **Not individually revocable.** A sealed grant is valid until it expires; the
 * levers are a short maximum lifetime with re-issue, and removing a key.
 *
 * Runtime-agnostic: Web Crypto and `@noble/ciphers` only.
 */

import { openBytes, SealError, sealBytes } from "./crypto.js";
import { type GrantMinter, GrantRefusedError, type IssuedGrant } from "./token-identity.js";
import { randomBytes, sha256 } from "./util/web-crypto.js";

/** Token prefix. The version is in the prefix, so an incompatible format is a
 *  different prefix -- routed elsewhere, never half-parsed. */
export const GRANT_TOKEN_PREFIX = "vgig1.";
/** Environment variable: comma-separated standard-base64 keys, minting key first. */
export const GRANT_KEYS_ENV = "VGI_RPC_GRANT_KEYS";
/** Environment variable: the audience bound into every token (default `""`). */
export const GRANT_AUDIENCE_ENV = "VGI_RPC_GRANT_AUDIENCE";
/** Environment variable: the lifetime ceiling in seconds (default 7 days). */
export const GRANT_MAX_TTL_ENV = "VGI_RPC_GRANT_MAX_TTL_SECONDS";
/** Longest lifetime a minted grant may have unless the deployment says otherwise.
 *  Short on purpose: expiry is the only revocation a sealed grant has. */
export const DEFAULT_GRANT_MAX_TTL_SECONDS = 7 * 24 * 3600;
/** Allowance for clocks disagreeing between minting and verifying workers. */
export const DEFAULT_GRANT_CLOCK_SKEW_SECONDS = 60;
/** Longest token text considered at all -- the `introspect_token` cap. */
export const MAX_GRANT_TOKEN_CHARS = 4096;

const KEY_LEN = 32;
const KID_LEN = 8;
const ENVELOPE_VERSION = 1;
const MAX_FIELD = 0xffff;
const TE = new TextEncoder();
const KID_DOMAIN = TE.encode("vgi_rpc.grant.kid.v1\u0000");
const AAD_DOMAIN = TE.encode("vgi_rpc.grant.v1\u0000");
const B64URL_RE = /^[A-Za-z0-9_-]+$/;
const STD_B64_RE = /^[A-Za-z0-9+/]*={0,2}$/;

/**
 * A token carrying the grant prefix that could not be accepted.
 *
 * One type for every cause -- malformed, wrong key, wrong audience, tampered,
 * expired -- so a caller cannot tell a forged token from a stale one except by
 * {@link expired}, which is only set once the token was proven authentic.
 */
export class GrantInvalidError extends Error {
  /** Authentic, but outside its lifetime (or not yet valid). */
  readonly expired: boolean;
  constructor(detail: string, expired = false) {
    super(detail);
    this.name = "GrantInvalidError";
    this.expired = expired;
  }
}

function concat(...parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let offset = 0;
  for (const p of parts) {
    out.set(p, offset);
    offset += p.length;
  }
  return out;
}

function sameBytes(a: Uint8Array, b: Uint8Array): boolean {
  return a.length === b.length && a.every((v, i) => v === b[i]);
}

/** The 8-byte key id a token names its sealing key with. */
export async function grantKeyId(key: Uint8Array): Promise<Uint8Array> {
  return (await sha256(concat(KID_DOMAIN, key))).slice(0, KID_LEN);
}

/** Options for {@link GrantKeys}. */
export interface GrantKeysOptions {
  /** Bound into every token's AAD. Deployments that (against advice) share a
   *  key still cannot accept each other's grants if their audiences differ. */
  audience?: string;
  /** Ceiling on a grant's lifetime, at minting and at verification. */
  maxTtlSeconds?: number;
  /** Tolerance applied to `issued_at` and `expires_at`. */
  clockSkewSeconds?: number;
}

/**
 * A deployment's grant configuration. The first key mints; every key verifies.
 * Rotation: add the new key first, keep the old one until its grants expire,
 * then remove it.
 */
export class GrantKeys {
  /** 32-byte keys, minting key first. */
  readonly keys: readonly Uint8Array[];
  /** See {@link GrantKeysOptions.audience}. */
  readonly audience: string;
  /** See {@link GrantKeysOptions.maxTtlSeconds}. */
  readonly maxTtlSeconds: number;
  /** See {@link GrantKeysOptions.clockSkewSeconds}. */
  readonly clockSkewSeconds: number;
  private kids: Promise<Uint8Array[]> | null = null;

  /** @throws Error No key, a key that is not 32 bytes, duplicate keys, or a
   *  non-positive lifetime. A worker refuses to start rather than run with a
   *  key it misread. */
  constructor(keys: readonly Uint8Array[], options: GrantKeysOptions = {}) {
    if (keys.length === 0) throw new Error("grant configuration needs at least one key");
    for (const key of keys) {
      if (!(key instanceof Uint8Array) || key.length !== KEY_LEN) {
        throw new Error(`every grant key must be exactly ${KEY_LEN} bytes`);
      }
    }
    for (let i = 0; i < keys.length; i++) {
      for (let j = i + 1; j < keys.length; j++) {
        if (sameBytes(keys[i], keys[j])) throw new Error("grant keys must be distinct");
      }
    }
    const maxTtl = options.maxTtlSeconds ?? DEFAULT_GRANT_MAX_TTL_SECONDS;
    if (!Number.isInteger(maxTtl) || maxTtl <= 0) throw new Error("maxTtlSeconds must be a positive integer");
    const skew = options.clockSkewSeconds ?? DEFAULT_GRANT_CLOCK_SKEW_SECONDS;
    if (!Number.isFinite(skew) || skew < 0) throw new Error("clockSkewSeconds must not be negative");
    const audience = options.audience ?? "";
    if (TE.encode(audience).length > MAX_FIELD) throw new Error("audience is too long");
    this.keys = keys.map((k) => new Uint8Array(k));
    this.audience = audience;
    this.maxTtlSeconds = maxTtl;
    this.clockSkewSeconds = skew;
  }

  /** Build from standard base64 key text (padding optional), minting key first.
   *  @throws Error A key that is not base64 of exactly 32 bytes. */
  static parse(encodedKeys: Iterable<string>, options: GrantKeysOptions = {}): GrantKeys {
    const keys: Uint8Array[] = [];
    let index = 0;
    for (const text of encodedKeys) {
      index++;
      const stripped = text.trim();
      const padded = stripped + "=".repeat((4 - (stripped.length % 4)) % 4);
      if (!STD_B64_RE.test(padded) || padded.length % 4 !== 0) {
        throw new Error(`grant key #${index} is not valid base64`);
      }
      let key: Uint8Array;
      try {
        key = Uint8Array.from(atob(padded), (c) => c.charCodeAt(0));
      } catch {
        throw new Error(`grant key #${index} is not valid base64`);
      }
      if (key.length !== KEY_LEN) {
        throw new Error(`grant key #${index} decodes to ${key.length} bytes; exactly ${KEY_LEN} are required`);
      }
      keys.push(key);
    }
    return new GrantKeys(keys, options);
  }

  /**
   * Read `VGI_RPC_GRANT_KEYS` (comma-separated, minting key first),
   * `VGI_RPC_GRANT_AUDIENCE` and `VGI_RPC_GRANT_MAX_TTL_SECONDS`.
   *
   * @param env The environment; default `process.env` where it exists.
   * @returns The configuration, or `null` when no key is set -- grants off.
   * @throws Error A malformed key or lifetime.
   */
  static fromEnv(env?: Record<string, string | undefined>): GrantKeys | null {
    const source = env ?? (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env ?? {};
    const raw = (source[GRANT_KEYS_ENV] ?? "").trim();
    if (!raw) return null;
    const ttlRaw = (source[GRANT_MAX_TTL_ENV] ?? "").trim();
    let maxTtlSeconds = DEFAULT_GRANT_MAX_TTL_SECONDS;
    if (ttlRaw) {
      if (!/^-?\d+$/.test(ttlRaw)) throw new Error(`${GRANT_MAX_TTL_ENV}=${JSON.stringify(ttlRaw)} is not an integer`);
      maxTtlSeconds = Number(ttlRaw);
    }
    return GrantKeys.parse(
      raw.split(",").filter((part) => part.trim() !== ""),
      { audience: source[GRANT_AUDIENCE_ENV] ?? "", maxTtlSeconds },
    );
  }

  /** The key ids, in key order. */
  keyIds(): Promise<Uint8Array[]> {
    this.kids ??= Promise.all(this.keys.map((k) => grantKeyId(k)));
    return this.kids;
  }

  /** The AAD a token sealed under `kid` is bound to. */
  aad(kid: Uint8Array): Uint8Array {
    return concat(AAD_DOMAIN, kid, TE.encode(this.audience));
  }
}

/** What a verified grant says. */
export interface GrantClaims {
  /** Whose standing delegation this is -- the caller it was minted for. */
  principal: string;
  /** What it may do; the worker interprets them. */
  scopes: string[];
  /** Why it was minted, for the audit trail. */
  purpose: string;
  /** Correlation handle. */
  grantId: string;
  /** Seconds since the Unix epoch. */
  issuedAt: number;
  /** Seconds since the Unix epoch. */
  expiresAt: number;
}

function packText(value: string): Uint8Array {
  const raw = TE.encode(value);
  if (raw.length > MAX_FIELD) throw new Error("grant field longer than 65535 bytes");
  const out = new Uint8Array(2 + raw.length);
  new DataView(out.buffer).setUint16(0, raw.length, true);
  out.set(raw, 2);
  return out;
}

function encodePayload(claims: GrantClaims): Uint8Array {
  if (claims.scopes.length > MAX_FIELD) throw new Error("too many scopes");
  const head = new Uint8Array(16);
  const view = new DataView(head.buffer);
  view.setBigInt64(0, BigInt(claims.issuedAt), true);
  view.setBigInt64(8, BigInt(claims.expiresAt), true);
  const count = new Uint8Array(2);
  new DataView(count.buffer).setUint16(0, claims.scopes.length, true);
  return concat(
    head,
    packText(claims.grantId),
    packText(claims.principal),
    packText(claims.purpose),
    count,
    ...claims.scopes.map(packText),
  );
}

/** Parse a payload strictly: exact lengths, valid UTF-8, no trailing bytes. */
function decodePayload(payload: Uint8Array): GrantClaims {
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  const strict = new TextDecoder("utf-8", { fatal: true });
  let pos = 0;
  const need = (n: number) => {
    if (pos + n > payload.length) throw new GrantInvalidError("grant payload is truncated");
  };
  const u16 = () => {
    need(2);
    const v = view.getUint16(pos, true);
    pos += 2;
    return v;
  };
  const i64 = () => {
    need(8);
    const v = view.getBigInt64(pos, true);
    pos += 8;
    return Number(v);
  };
  const text = () => {
    const len = u16();
    need(len);
    const bytes = payload.subarray(pos, pos + len);
    pos += len;
    try {
      return strict.decode(bytes);
    } catch {
      throw new GrantInvalidError("grant payload is not UTF-8");
    }
  };
  const issuedAt = i64();
  const expiresAt = i64();
  const grantId = text();
  const principal = text();
  const purpose = text();
  const count = u16();
  const scopes: string[] = [];
  for (let i = 0; i < count; i++) scopes.push(text());
  if (pos !== payload.length) throw new GrantInvalidError("grant payload has trailing bytes");
  return { principal, scopes, purpose, grantId, issuedAt, expiresAt };
}

function b64url(data: Uint8Array): string {
  let bin = "";
  for (const b of data) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

/** Decode unpadded base64url, rejecting any non-canonical spelling: re-encoding
 *  must reproduce the text, so one token has exactly one spelling. */
function b64urlStrict(text: string): Uint8Array {
  if (!B64URL_RE.test(text) || text.length % 4 === 1) {
    throw new GrantInvalidError("grant token is not unpadded base64url");
  }
  const std = text.replace(/-/g, "+").replace(/_/g, "/");
  const raw = Uint8Array.from(atob(std + "=".repeat((4 - (std.length % 4)) % 4)), (c) => c.charCodeAt(0));
  if (b64url(raw) !== text) throw new GrantInvalidError("grant token is not canonical base64url");
  return raw;
}

/** Inputs to {@link mintGrantToken}. */
export interface MintGrantRequest {
  /** The caller the grant is for. */
  principal: string;
  /** What it may do. */
  scopes: readonly string[];
  /** Why it is being minted. */
  purpose: string;
  /** Requested lifetime; capped at the configured maximum. Must be positive. */
  ttlSeconds: number;
  /** Override the clock (seconds), for tests and vectors. */
  now?: number;
  /** Override the random grant id, for tests and vectors. */
  grantId?: string;
  /** Fixed 24-byte nonce, **for test vectors only**. */
  nonce?: Uint8Array;
}

/** What {@link mintGrantToken} returns. */
export interface MintedGrant {
  /** The `vgig1.` bearer credential. */
  token: string;
  /** The claims sealed inside it. */
  claims: GrantClaims;
}

/** Mint a sealed grant with the first configured key.
 *  @throws Error A non-positive lifetime, or a field too long to encode. */
export async function mintGrantToken(keys: GrantKeys, request: MintGrantRequest): Promise<MintedGrant> {
  if (!(request.ttlSeconds > 0)) throw new Error("ttlSeconds must be positive");
  const issuedAt = Math.trunc(request.now ?? Date.now() / 1000);
  const claims: GrantClaims = {
    principal: request.principal,
    scopes: [...request.scopes],
    purpose: request.purpose,
    grantId: request.grantId ?? [...randomBytes(16)].map((b) => b.toString(16).padStart(2, "0")).join(""),
    issuedAt,
    expiresAt: issuedAt + Math.min(Math.trunc(request.ttlSeconds), keys.maxTtlSeconds),
  };
  const [kid] = await keys.keyIds();
  const envelope = sealBytes(encodePayload(claims), keys.keys[0], {
    aad: keys.aad(kid),
    version: ENVELOPE_VERSION,
    nonce: request.nonce,
  });
  return { token: GRANT_TOKEN_PREFIX + b64url(concat(kid, envelope)), claims };
}

/**
 * Verify a sealed grant and return its claims.
 *
 * Order, normative: prefix, length, canonical base64url, key id, AEAD open,
 * payload, then lifetime -- the lifetime is inside the ciphertext, so it is
 * only trusted after the tag verified.
 *
 * @param now Override the clock (seconds), for tests.
 * @throws GrantInvalidError For every cause; `expired` only for an authentic
 *   grant outside its lifetime.
 */
export async function verifyGrantToken(keys: GrantKeys, token: string, now?: number): Promise<GrantClaims> {
  if (!token.startsWith(GRANT_TOKEN_PREFIX)) throw new GrantInvalidError("not a sealed grant");
  if (token.length > MAX_GRANT_TOKEN_CHARS) throw new GrantInvalidError("grant token is too long");
  const raw = b64urlStrict(token.slice(GRANT_TOKEN_PREFIX.length));
  const kid = raw.subarray(0, KID_LEN);
  const envelope = raw.subarray(KID_LEN);
  const kids = await keys.keyIds();
  const index = kids.findIndex((k) => sameBytes(k, kid));
  if (index < 0) throw new GrantInvalidError("grant was sealed with a key this deployment does not hold");
  let payload: Uint8Array;
  try {
    payload = openBytes(envelope, keys.keys[index], { aad: keys.aad(kids[index]), version: ENVELOPE_VERSION });
  } catch (e) {
    if (e instanceof SealError) throw new GrantInvalidError("grant failed verification");
    throw e;
  }
  const claims = decodePayload(payload);
  if (!claims.principal) throw new GrantInvalidError("grant names no principal");
  if (claims.expiresAt <= claims.issuedAt || claims.expiresAt - claims.issuedAt > keys.maxTtlSeconds) {
    throw new GrantInvalidError("grant lifetime exceeds this deployment's maximum");
  }
  const current = now ?? Date.now() / 1000;
  const skew = keys.clockSkewSeconds;
  if (claims.issuedAt > current + skew) throw new GrantInvalidError("grant is not yet valid", true);
  if (current >= claims.expiresAt + skew) throw new GrantInvalidError("grant has expired", true);
  return claims;
}

/** A `mintGrant` hook that issues sealed grants -- what `IdentityImpl`
 *  installs when grant keys are configured and the worker supplied no hook. */
export function sealedMintGrant(keys: GrantKeys): GrantMinter {
  return async (principal, purpose, scopes, ttlSeconds): Promise<IssuedGrant> => {
    if (!(ttlSeconds > 0)) throw new GrantRefusedError("ttl_seconds must be positive");
    let minted: MintedGrant;
    try {
      minted = await mintGrantToken(keys, { principal, purpose, scopes, ttlSeconds });
    } catch (e) {
      throw new GrantRefusedError((e as Error).message);
    }
    return { token: minted.token, expiresAt: minted.claims.expiresAt, grantId: minted.claims.grantId };
  };
}
