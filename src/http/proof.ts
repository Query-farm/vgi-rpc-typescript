// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Proxy proof: HMAC evidence that a request arrived through a trusted proxy.
 *
 * A proxy mints a per-request HMAC-SHA256 over a timestamp, a fresh nonce and
 * the worker's own identifier, keyed by a secret shared only with that worker.
 * The proof establishes the *hop*, never the caller — it is ANDed with whatever
 * authenticates the user rather than replacing it.
 *
 * Unlike a forwarded assertion about what happened at a TLS terminator, a proof
 * cannot be produced by someone who merely reaches the worker directly: without
 * the secret there is nothing to replay.
 *
 * Built on WebCrypto (via `util/web-crypto.js`) rather than `node:crypto`, so
 * this module bundles cleanly for workerd and browsers.
 *
 * The normative cross-language contract is `docs/proxy-proof-spec.md` in the
 * vgi-rpc repository.
 */

import { AuthContext } from "../auth.js";
import { constantTimeEqual, hmacSha256 } from "../util/web-crypto.js";
import type { AuthenticateFn } from "./auth.js";
import { AuthReason } from "./unauthorized.js";

/** Header carrying the proof on the wire. */
export const PROOF_HEADER = "VGI-Proxy-Proof";

/** Header advertising that this worker rejects unproofed requests. */
export const PROOF_REQUIRED_HEADER = "VGI-Proxy-Proof-Required";

const VERSION = "v1";
const DOMAIN_PREFIX = "vgi.proxy.proof.v1";
const DERIVE_LABEL = "vgi.proxy.proof.v1/";
const MAX_HEADER_LEN = 512;
const SECRET_LEN = 32;
const CLAIMS_KEY = "vgi_proxy_proof";
const DEFAULT_REPLAY_CAPACITY = 100_000;

// Charsets are load-bearing, not cosmetic: the canonical string is
// NUL-separated, so framing is only unambiguous because no field can contain a
// NUL (and `kid` cannot contain the '.' separating wire fields).
const KID_RE = /^[A-Za-z0-9_-]{1,64}$/;
const TS_RE = /^[0-9]{1,20}$/;
const NONCE_RE = /^[A-Za-z0-9_-]{22}$/;
const ORIGIN_RE = /^[A-Za-z0-9._:/-]{1,255}$/;
// The MAC is charset-checked rather than left to the base64 decoder: decoders
// disagree about invalid input across languages, and the reason code is part of
// the wire contract.
const MAC_RE = /^[A-Za-z0-9_-]{43}$/;

const _enc = new TextEncoder();

/** How strictly a worker treats the proof header. */
export type ProofMode = "off" | "allow" | "require";

/** One accepted key and the proxy label it attributes to. */
export interface ProofSecret {
  secret: Uint8Array;
  label: string;
}

/** Worker-side proof configuration. */
export interface ProofConfig {
  mode: ProofMode;
  /** This worker's identifier. Folded into every MAC but never transmitted, so
   *  a proof minted for another worker cannot verify here. */
  originId: string;
  /** Maps a key id to its secret and proxy label. */
  secrets: Record<string, ProofSecret> | Map<string, ProofSecret>;
  /** Half-width of the timestamp acceptance window. Default 30. */
  skewSeconds?: number;
  /** Hard bound on the nonce cache. Default 100000. */
  replayCapacity?: number;
  /** Whether replayed nonces are rejected. Default true. */
  replayCache?: boolean;
  /** Injectable clock returning Unix seconds, for deterministic tests. */
  now?: () => number;
}

/** A proof rejection carrying its reason code. */
export class ProofError extends Error {
  readonly reason: string;

  /** Reason code for the standardized 401, set only when the gate denies (see
   *  {@link requireProxyProof}). Distinct from {@link reason}, which is the
   *  verifier's internal diagnosis and must never reach a caller. */
  vgiAuthReason?: AuthReason;

  constructor(reason: string, detail?: string) {
    super(detail ?? reason);
    // Plain `Error` name: `chainAuthenticate` treats a subclass as a
    // non-credential error and rethrows it, which would bypass composition.
    this.name = "Error";
    this.reason = reason;
  }
}

function b64urlEncode(raw: Uint8Array): string {
  let s = "";
  for (let i = 0; i < raw.length; i++) s += String.fromCharCode(raw[i]);
  return btoa(s).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function b64urlDecode(text: string): Uint8Array {
  const padded = text.replace(/-/g, "+").replace(/_/g, "/") + "=".repeat((4 - (text.length % 4)) % 4);
  const bin = atob(padded);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

function concatNulSeparated(parts: string[]): Uint8Array {
  const encoded = parts.map((p) => _enc.encode(p));
  const total = encoded.reduce((n, p) => n + p.length, 0) + encoded.length - 1;
  const out = new Uint8Array(total);
  let at = 0;
  for (let i = 0; i < encoded.length; i++) {
    if (i > 0) out[at++] = 0;
    out.set(encoded[i], at);
    at += encoded[i].length;
  }
  return out;
}

/**
 * Derive the secret shared between one proxy and one worker.
 *
 * A worker is configured with its derived secret only, never the base key —
 * otherwise it could mint proofs its siblings would accept.
 */
export async function deriveProofSecret(baseKey: Uint8Array, proxyId: string, originId: string): Promise<Uint8Array> {
  if (baseKey.length !== SECRET_LEN) {
    throw new Error(`base key must be exactly ${SECRET_LEN} bytes, got ${baseKey.length}`);
  }
  if (!ORIGIN_RE.test(proxyId) || !ORIGIN_RE.test(originId)) {
    throw new Error("proxyId and originId must match " + ORIGIN_RE.source);
  }
  // NUL-separated, and neither identifier may contain NUL — so ("a", "b\0c")
  // cannot collide with ("a\0b", "c").
  const label = _enc.encode(DERIVE_LABEL);
  const rest = concatNulSeparated([proxyId, originId]);
  const msg = new Uint8Array(label.length + rest.length);
  msg.set(label, 0);
  msg.set(rest, label.length);
  return hmacSha256(baseKey, msg);
}

/**
 * Build the MAC input.
 *
 * `originId` is folded in but never transmitted: the worker supplies its own,
 * which is what binds a proof to a single audience.
 */
export function canonicalString(kid: string, ts: string, nonce: string, originId: string): Uint8Array {
  return concatNulSeparated([DOMAIN_PREFIX, kid, ts, nonce, originId]);
}

/** Mint a proof token. Primarily for tests and for clients fronting a worker. */
export async function mintProof(
  secret: Uint8Array,
  kid: string,
  originId: string,
  now: number,
  nonce: string,
): Promise<string> {
  if (!KID_RE.test(kid)) throw new Error(`kid must match ${KID_RE.source}`);
  if (!ORIGIN_RE.test(originId)) throw new Error(`originId must match ${ORIGIN_RE.source}`);
  const ts = String(Math.floor(now));
  const mac = await hmacSha256(secret, canonicalString(kid, ts, nonce, originId));
  return [VERSION, kid, ts, nonce, b64urlEncode(mac)].join(".");
}

function secretFor(cfg: ProofConfig, kid: string): ProofSecret | undefined {
  return cfg.secrets instanceof Map ? cfg.secrets.get(kid) : cfg.secrets[kid];
}

/**
 * Verify a proof token, returning the claims to record.
 *
 * Cheap rejections run before any MAC is computed, so an unparseable header
 * costs a few pattern matches rather than a hash.
 */
export async function verifyProof(
  token: string,
  cfg: ProofConfig,
  cache: NonceCache | null,
  now: number,
): Promise<Record<string, string>> {
  if (token.length > MAX_HEADER_LEN) throw new ProofError("malformed", "proof header too long");
  const parts = token.split(".");
  if (parts.length !== 5) throw new ProofError("malformed", `expected 5 fields, got ${parts.length}`);
  const [version, kid, tsRaw, nonce, macB64] = parts;
  if (version !== VERSION || !KID_RE.test(kid) || !TS_RE.test(tsRaw) || !NONCE_RE.test(nonce) || !MAC_RE.test(macB64)) {
    throw new ProofError("malformed", "field shape");
  }

  const entry = secretFor(cfg, kid);
  if (!entry) throw new ProofError("unknown_kid", "no secret for kid");

  // Two-sided. Checking only the upper bound would let a far-future timestamp
  // pass forever.
  const skew = cfg.skewSeconds ?? 30;
  const age = now - Number(tsRaw);
  if (age > skew) throw new ProofError("expired", `age=${age}s`);
  if (-age > skew) throw new ProofError("not_yet_valid", `age=${age}s`);

  const expected = await hmacSha256(entry.secret, canonicalString(kid, tsRaw, nonce, cfg.originId));
  // `kid` is public, so selecting one candidate secret is a safe branch; only
  // the resulting MAC needs the constant-time compare.
  if (!constantTimeEqual(b64urlDecode(macB64), expected)) {
    throw new ProofError("bad_mac", "signature mismatch");
  }

  if (cache && !cache.checkAndAdd(nonce, now)) {
    throw new ProofError("replayed", "nonce already seen");
  }

  return {
    verified: "true",
    proxy: entry.label,
    kid,
    origin_id: cfg.originId,
    reason: "ok",
  };
}

/**
 * A bounded, TTL-expiring set of recently-seen nonces.
 *
 * The capacity cap is not optional: a TTL bounds how long an entry lives, never
 * how many arrive inside the window, so a TTL-only cache is a remote
 * memory-exhaustion vector.
 */
export class NonceCache {
  // A Map preserves insertion order, and a uniform TTL makes insertion order
  // expiry order — so expired entries are always a prefix and the sweep is exact.
  private entries = new Map<string, number>();

  constructor(
    private readonly ttlSeconds: number,
    private readonly capacity: number,
  ) {}

  /**
   * Report whether a nonce is fresh, remembering it if so.
   *
   * JavaScript's single-threaded model makes this atomic by construction; the
   * other ports lock explicitly.
   */
  checkAndAdd(nonce: string, now: number): boolean {
    for (const [key, expires] of this.entries) {
      if (expires > now) break;
      this.entries.delete(key);
    }
    if (this.entries.has(nonce)) return false;
    // Evict oldest rather than refuse: a burst past capacity is an availability
    // problem, not an authentication one, and the timestamp window still bounds
    // the evicted nonce's usefulness.
    while (this.entries.size >= this.capacity) {
      const oldest = this.entries.keys().next();
      if (oldest.done) break;
      this.entries.delete(oldest.value);
    }
    this.entries.set(nonce, now + this.ttlSeconds);
    return true;
  }

  /** Number of retained nonces. */
  get size(): number {
    return this.entries.size;
  }
}

/**
 * Wrap an authenticate callback with a proof precondition.
 *
 * The gate runs first; on failure `inner` is never invoked. This is an AND, not
 * an alternative — do not pass a proof gate to `chainAuthenticate`, whose
 * first-return-wins semantics would let any later credential bypass it.
 *
 * `inner` may be omitted: proof alone means "only my proxy may call this
 * worker", with user identity handled upstream.
 */
export function requireProxyProof(cfg: ProofConfig, inner?: AuthenticateFn): AuthenticateFn {
  if (cfg.mode === "off") {
    throw new Error("requireProxyProof called with mode='off'; install no gate instead");
  }
  if (cfg.mode !== "allow" && cfg.mode !== "require") {
    throw new Error(`unknown proof mode ${cfg.mode}`);
  }
  if (!ORIGIN_RE.test(cfg.originId)) {
    throw new Error(`originId is required and must match ${ORIGIN_RE.source}`);
  }
  const kids = cfg.secrets instanceof Map ? [...cfg.secrets.keys()] : Object.keys(cfg.secrets);
  if (kids.length === 0) throw new Error("at least one secret is required");
  for (const kid of kids) {
    if (!KID_RE.test(kid)) throw new Error(`kid must match ${KID_RE.source}, got ${kid}`);
    const entry = secretFor(cfg, kid);
    if (!entry || entry.secret.length !== SECRET_LEN) {
      throw new Error(`secret for kid ${kid} must be exactly ${SECRET_LEN} bytes`);
    }
  }
  const skew = cfg.skewSeconds ?? 30;
  if (skew <= 0) throw new Error(`skewSeconds must be positive, got ${skew}`);

  const cache = cfg.replayCache === false ? null : new NonceCache(skew, cfg.replayCapacity ?? DEFAULT_REPLAY_CAPACITY);
  const required = cfg.mode === "require";
  const clock = cfg.now ?? (() => Math.floor(Date.now() / 1000));

  return async (request: Request): Promise<AuthContext> => {
    let claims: Record<string, string>;
    try {
      const raw = request.headers.get(PROOF_HEADER);
      if (!raw) throw new ProofError("no_proof", "header absent");
      // The Headers API joins repeated values with ", ". A proof contains no
      // comma, so this catches header injection without a multi-value API.
      if (raw.includes(",")) throw new ProofError("malformed", "multiple proof headers");
      claims = await verifyProof(raw, cfg, cache, clock());
    } catch (err) {
      const reason = err instanceof ProofError ? err.reason : "malformed";
      if (required) {
        // Uniform message: the caller controls `kid`, so echoing any detail
        // would reflect attacker-supplied text back to them.
        const denial = new ProofError(reason, "proxy proof required");
        // Every outcome — absent, malformed, unknown key, expired, bad MAC,
        // replayed — collapses onto the one code, so the 401 cannot be used
        // to tell which check the caller tripped (proxy-proof spec §6).
        denial.vgiAuthReason = AuthReason.ProxyRequired;
        throw denial;
      }
      claims = { verified: "false", proxy: "", kid: "", origin_id: cfg.originId, reason };
    }

    if (!inner) {
      return new AuthContext(CLAIMS_KEY, true, claims.proxy || null, { [CLAIMS_KEY]: claims });
    }
    const ctx = await inner(request);
    return new AuthContext(ctx.domain, ctx.authenticated, ctx.principal, {
      ...ctx.claims,
      [CLAIMS_KEY]: claims,
    });
  };
}

/**
 * Parse a `kid:hex,kid:hex` secret specification.
 *
 * The `kid` doubles as the proxy's label, so attribution needs no extra
 * configuration. Any malformed entry fails the whole parse rather than silently
 * dropping one proxy's access.
 */
export function parseProofSecrets(raw: string): Record<string, ProofSecret> {
  const out: Record<string, ProofSecret> = {};
  for (const chunk of raw.split(",")) {
    const item = chunk.trim();
    if (!item) continue;
    const sep = item.indexOf(":");
    if (sep < 0) throw new Error(`expected 'kid:hex', got ${item}`);
    const kid = item.slice(0, sep);
    const hex = item.slice(sep + 1);
    if (!KID_RE.test(kid)) throw new Error(`kid must match ${KID_RE.source}, got ${kid}`);
    if (hex.length !== SECRET_LEN * 2 || !/^[0-9a-fA-F]+$/.test(hex)) {
      throw new Error(`secret for kid ${kid} must be ${SECRET_LEN * 2} hex chars`);
    }
    const secret = new Uint8Array(SECRET_LEN);
    for (let i = 0; i < SECRET_LEN; i++) secret[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
    out[kid] = { secret, label: kid };
  }
  if (Object.keys(out).length === 0) throw new Error("no secrets parsed");
  return out;
}
