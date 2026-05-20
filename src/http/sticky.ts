// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Sticky-session machinery for the HTTP transport.
 *
 * Sticky sessions let an RPC method bind a state object (a DB cursor, a
 * loaded model, an open file handle) to the worker process that opened it,
 * keyed by a short-lived AEAD-sealed token. Subsequent requests carrying
 * the same `VGI-Session` header restore the object as `ctx.session`;
 * requests that miss (wrong worker, expired, evicted) surface as a typed
 * {@link SessionLostError}.
 *
 * HTTP-only and opt-in: the non-sticky wire path is byte-identical to the
 * pre-sticky framework.
 *
 * Mirrors Python's `vgi_rpc/http/server/_sticky.py`.
 */

import { openBytes, SealError, sealBytes } from "../crypto.js";
import { ServerDrainingError, SessionLostError } from "../errors.js";
import { randomBytes } from "../util/web-crypto.js";

const _UTF8 = new TextEncoder();
const _UTF8DEC = new TextDecoder("utf-8", { fatal: false });

const TOKEN_VERSION = 1;
const SESSION_ID_LEN = 12; // bytes — matches Python's `_SESSION_ID_LEN`

// Plaintext frame layout (little-endian):
//   [u64 created_at]
//   [u8  server_id_len]
//   [server_id_len bytes ASCII server_id]
//   [12B session_id]
//   [u64 expires_at]
const PREFIX_LEN = 8 + 1; // created_at + server_id_len
const SUFFIX_LEN = 8; // expires_at

// ---------------------------------------------------------------------------
// base64url helpers (no padding) — `VGI-Session` is header-safe
// ---------------------------------------------------------------------------

function base64UrlEncode(bytes: Uint8Array): string {
  let s = "";
  for (let i = 0; i < bytes.length; i += 0x8000) {
    s += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  }
  return btoa(s).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function base64UrlDecode(s: string): Uint8Array {
  let b64 = s.replace(/-/g, "+").replace(/_/g, "/");
  // Re-pad to multiple of 4 for `atob`.
  const pad = b64.length % 4;
  if (pad === 2) b64 += "==";
  else if (pad === 3) b64 += "=";
  else if (pad === 1) throw new Error("invalid base64url length");
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

// ---------------------------------------------------------------------------
// Token sealing
// ---------------------------------------------------------------------------

/** Seal a sticky-session token. Returns the base64url-encoded value for the
 *  `VGI-Session` header. */
export function sealSessionToken(
  serverId: string,
  sessionId: Uint8Array,
  expiresAt: number,
  tokenKey: Uint8Array,
  aad: Uint8Array,
  now?: number,
): string {
  if (sessionId.length !== SESSION_ID_LEN) {
    throw new Error(`session_id must be ${SESSION_ID_LEN} bytes, got ${sessionId.length}`);
  }
  const serverIdBytes = _UTF8.encode(serverId);
  if (serverIdBytes.length > 255) {
    throw new Error(`server_id too long (${serverIdBytes.length} bytes); max 255`);
  }
  const plaintext = new Uint8Array(PREFIX_LEN + serverIdBytes.length + SESSION_ID_LEN + SUFFIX_LEN);
  const view = new DataView(plaintext.buffer);
  let offset = 0;
  view.setBigUint64(offset, BigInt(now ?? Math.floor(Date.now() / 1000)), true);
  offset += 8;
  plaintext[offset] = serverIdBytes.length;
  offset += 1;
  plaintext.set(serverIdBytes, offset);
  offset += serverIdBytes.length;
  plaintext.set(sessionId, offset);
  offset += SESSION_ID_LEN;
  view.setBigUint64(offset, BigInt(expiresAt), true);
  const sealed = sealBytes(plaintext, tokenKey, { aad, version: TOKEN_VERSION });
  return base64UrlEncode(sealed);
}

export interface OpenedSessionToken {
  serverId: string;
  sessionId: Uint8Array;
  expiresAt: number;
}

/** Open a sticky-session token. Raises {@link SessionLostError} on any failure
 *  — wrong AAD (cross-principal replay) is indistinguishable from garbage. */
export function openSessionToken(token: string, tokenKey: Uint8Array, aad: Uint8Array): OpenedSessionToken {
  let raw: Uint8Array;
  try {
    raw = base64UrlDecode(token);
  } catch {
    throw new SessionLostError("malformed session token");
  }
  let plaintext: Uint8Array;
  try {
    plaintext = openBytes(raw, tokenKey, { aad, version: TOKEN_VERSION });
  } catch (err) {
    if (err instanceof SealError) {
      throw new SessionLostError("session token verification failed");
    }
    throw err;
  }
  if (plaintext.length < PREFIX_LEN) {
    throw new SessionLostError("malformed session token");
  }
  const view = new DataView(plaintext.buffer, plaintext.byteOffset, plaintext.byteLength);
  const serverIdLen = plaintext[8];
  const sidPos = PREFIX_LEN + serverIdLen;
  const endPos = sidPos + SESSION_ID_LEN + SUFFIX_LEN;
  if (plaintext.length !== endPos) {
    throw new SessionLostError("malformed session token");
  }
  const serverId = _UTF8DEC.decode(plaintext.subarray(PREFIX_LEN, sidPos));
  // Copy session_id into a fresh buffer so callers can hold it without
  // pinning the larger plaintext buffer.
  const sessionId = new Uint8Array(SESSION_ID_LEN);
  sessionId.set(plaintext.subarray(sidPos, sidPos + SESSION_ID_LEN));
  const expiresAt = Number(view.getBigUint64(sidPos + SESSION_ID_LEN, true));
  return { serverId, sessionId, expiresAt };
}

// ---------------------------------------------------------------------------
// Async mutex — serializes same-session calls
// ---------------------------------------------------------------------------

/** Minimal promise-based mutex. The HTTP handler awaits `acquire()` before
 *  dispatching on a resumed session and calls the returned release in a
 *  `finally` so concurrent calls on the same session run sequentially. */
class AsyncMutex {
  private locked = false;
  private waiters: Array<() => void> = [];

  async acquire(): Promise<() => void> {
    if (!this.locked) {
      this.locked = true;
      return () => this.release();
    }
    await new Promise<void>((resolve) => this.waiters.push(resolve));
    this.locked = true;
    return () => this.release();
  }

  private release(): void {
    const next = this.waiters.shift();
    if (next) {
      // Hand the lock straight to the next waiter (still `locked = true`).
      next();
    } else {
      this.locked = false;
    }
  }
}

// ---------------------------------------------------------------------------
// Session registry
// ---------------------------------------------------------------------------

/** A live session in the per-worker registry. */
export interface SessionEntry {
  state: unknown;
  expiresAt: number; // seconds since epoch
  principalKey: string;
  lock: AsyncMutex;
}

/**
 * Derive the registry partition key for a request principal.
 *
 * Both the dispatch path and the `DELETE /__session__` teardown path MUST
 * compute this identically — otherwise a session opened on one path can't
 * be looked up on the other. The NUL separator (rather than a space)
 * keeps `{domain:"a", principal:"b "}` from colliding with
 * `{domain:"a ", principal:"b"}`. Anonymous requests collapse to a
 * single sentinel.
 *
 * `domain` / `principal` are the authenticated identity fields, or
 * null/undefined for anonymous.
 */
export function sessionPrincipalKey(
  authenticated: boolean,
  domain: string | null | undefined,
  principal: string | null | undefined,
): string {
  if (!authenticated) return "\u0000anonymous";
  return `${domain ?? ""}\u0000${principal ?? ""}`;
}

/** Hex-encode a session_id Uint8Array (24-char lowercase hex). */
export function sessionIdHex(sessionId: Uint8Array): string {
  let s = "";
  for (let i = 0; i < sessionId.length; i++) s += sessionId[i].toString(16).padStart(2, "0");
  return s;
}

/** Compare two byte arrays for equality. */
function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

/** Per-worker in-process map of live sticky sessions. */
export class SessionRegistry {
  // Use a Map keyed by hex id (Uint8Array isn't a valid Map key for equality).
  private entries = new Map<string, { id: Uint8Array; entry: SessionEntry }>();
  private _draining = false;

  constructor(public readonly defaultTtl: number) {}

  get draining(): boolean {
    return this._draining;
  }

  setDraining(value: boolean): void {
    this._draining = value;
  }

  /** Register a session. Throws {@link ServerDrainingError} when draining. */
  open(state: unknown, ttl: number | undefined, principalKey: string): { sessionId: Uint8Array; expiresAt: number } {
    if (this._draining) {
      throw new ServerDrainingError("server is draining — new sessions are rejected");
    }
    const effective = ttl ?? this.defaultTtl;
    const expiresAt = Math.floor(Date.now() / 1000) + effective;
    const sessionId = randomBytes(SESSION_ID_LEN);
    const key = sessionIdHex(sessionId);
    this.entries.set(key, {
      id: sessionId,
      entry: { state, expiresAt, principalKey, lock: new AsyncMutex() },
    });
    return { sessionId, expiresAt };
  }

  /** Look up a session. Returns null on miss, expiry, or principal mismatch.
   *  Expired entries are evicted in-line (and `state.close?.()` invoked). */
  get(sessionId: Uint8Array, principalKey: string): SessionEntry | null {
    const key = sessionIdHex(sessionId);
    const slot = this.entries.get(key);
    if (!slot) return null;
    if (!bytesEqual(slot.id, sessionId)) return null;
    const now = Math.floor(Date.now() / 1000);
    if (slot.entry.expiresAt < now) {
      this.entries.delete(key);
      closeStateSuppressed(slot.entry.state);
      return null;
    }
    if (slot.entry.principalKey !== principalKey) return null;
    return slot.entry;
  }

  /** Remove a session and invoke `state.close?.()`. Returns true on hit. */
  close(sessionId: Uint8Array): boolean {
    const key = sessionIdHex(sessionId);
    const slot = this.entries.get(key);
    if (!slot || !bytesEqual(slot.id, sessionId)) return false;
    this.entries.delete(key);
    closeStateSuppressed(slot.entry.state);
    return true;
  }

  /** Evict every entry past its TTL. Returns the eviction count. */
  drainExpired(now?: number): number {
    const cutoff = now ?? Math.floor(Date.now() / 1000);
    let count = 0;
    for (const [key, slot] of this.entries) {
      if (slot.entry.expiresAt < cutoff) {
        this.entries.delete(key);
        closeStateSuppressed(slot.entry.state);
        count++;
      }
    }
    return count;
  }

  /** Invoke `state.close?.()` on every live session and clear the registry. */
  shutdown(): void {
    const slots = Array.from(this.entries.values());
    this.entries.clear();
    for (const slot of slots) closeStateSuppressed(slot.entry.state);
  }

  get size(): number {
    return this.entries.size;
  }
}

function closeStateSuppressed(state: unknown): void {
  const closer = (state as { close?: unknown })?.close;
  if (typeof closer !== "function") return;
  try {
    (closer as () => unknown).call(state);
  } catch {
    // Suppress — eviction must not crash the reaper.
  }
}

// ---------------------------------------------------------------------------
// Reaper
// ---------------------------------------------------------------------------

/** Start a periodic reaper that evicts expired sessions. Returns a stop fn.
 *  Uses `setInterval().unref()` where available so the reaper does not keep
 *  the process alive. */
export function startSessionReaper(registry: SessionRegistry, tickMs = 1000): () => void {
  const handle = setInterval(() => {
    try {
      registry.drainExpired();
    } catch {
      // never crash the reaper
    }
  }, tickMs);
  (handle as { unref?: () => void }).unref?.();
  return () => clearInterval(handle);
}

// ---------------------------------------------------------------------------
// Per-request sticky sink — bridges CallContext to the handler
// ---------------------------------------------------------------------------

/** Per-request handle that the HTTP handler installs on the OutputCollector.
 *  `CallContext.openSession` / `closeSession` / `session` read and mutate
 *  this object; the handler then emits the resulting headers on the
 *  response. */
export interface StickySink {
  /** True iff the request carried `VGI-Session-Accept: true`. */
  acceptOpens: boolean;
  /** Live session state (resumed or just-opened). Null until `openSession`
   *  or a successful resume populates it. */
  state: unknown | null;
  /** Hex session_id for the access log. Populated on resume + open;
   *  preserved across `closeSession` so close records still carry the id. */
  sessionId: string | null;
  /** Set by `openSession` so `process_response` emits `VGI-Session: <token>`. */
  mintToken: string | null;
  /** Set by `closeSession` so `process_response` emits `VGI-Session-Close: true`. */
  closed: boolean;
  /** Sticky-session lifecycle action observed during dispatch — one of
   *  "none" / "resume" / "open" / "close". Surfaced on the access log. */
  action: "none" | "resume" | "open" | "close";
  /** Bound by the handler: registers `state` in the registry, mints the
   *  AEAD-sealed token, and stamps `mintToken` + `sessionId`. */
  _open(state: unknown, ttl: number | undefined): void;
  /** Bound by the handler: removes the registry entry + invokes
   *  `state.close?.()`. Idempotent. */
  _close(): void;
}

/** Build a `StickySink` for a request without sticky support — `_open` /
 *  `_close` throw the same RuntimeError shape as Python's implementation so
 *  call sites get a clear message. */
export function unavailableStickySink(): StickySink {
  return {
    acceptOpens: false,
    state: null,
    sessionId: null,
    mintToken: null,
    closed: false,
    action: "none",
    _open() {
      throw new Error("sticky sessions not available on this transport");
    },
    _close() {
      throw new Error("sticky sessions not available on this transport");
    },
  };
}

// ---------------------------------------------------------------------------
// Drain handle — operator-facing API for graceful shutdown
// ---------------------------------------------------------------------------

/** Operator-facing handle returned by `createHttpHandler` (when sticky is
 *  enabled) so SIGTERM hooks / worker-exit hooks can drain in-flight
 *  sessions cleanly. Mirrors Python's `DrainHandle`. */
export interface DrainHandle {
  /** Flip the registry's drain flag — subsequent `ctx.openSession` raises
   *  {@link ServerDrainingError}. Existing sessions continue. */
  drain(): void;
  /** Invoke `state.close?.()` on every live session and clear the registry. */
  shutdown(): void;
  /** Return whether `drain()` has been invoked. */
  isDraining(): boolean;
  /** Test-only / advanced: flip the drain flag back. Production deployments
   *  only ever drain in one direction; conformance tests use this to clean
   *  up the fixture between tests. */
  setDraining(value: boolean): void;
}

/** Build a {@link DrainHandle} for *registry*. `stopReaper`, when supplied,
 *  is invoked by `shutdown()` so the periodic reaper interval is cleared and
 *  the handle fully releases its resources. */
export function makeDrainHandle(registry: SessionRegistry, stopReaper?: () => void): DrainHandle {
  return {
    drain: () => registry.setDraining(true),
    shutdown: () => {
      stopReaper?.();
      registry.shutdown();
    },
    isDraining: () => registry.draining,
    setDraining: (v) => registry.setDraining(v),
  };
}

// AAD: sticky tokens reuse the existing `computeAad` from `./token.ts` so the
// principal binding is identical for state and session tokens. The envelope
// version byte (`TOKEN_VERSION = 1` here vs. `TOKEN_VERSION = 4` for state
// tokens) keeps the two formats discriminated under the same key.
