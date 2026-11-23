// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * `StateRef`, argument references and the turn budget (`STATE_REF_V1.md`).
 *
 * On HTTP every stream continuation echoes the stream's state back to the
 * server. A {@link StateRef} is how a server says "this part of my stream state
 * does not change": store it once, carry a small sealed tag in the state, read
 * it back on any later turn on any replica.
 *
 * ```ts
 * protocol.producer<{ query: StateRef; offset: number }>("scan", {
 *   params: { query: bytes },
 *   outputSchema,
 *   // The init handler is the one place stateRef() is allowed.
 *   init: (p, ctx) => ({ query: ctx.stateRef(p.query), offset: 0 }),
 *   produce: async (state, out) => {
 *     const query = await out.resolve(state.query); // any turn, any replica
 *     ...
 *   },
 * });
 * ```
 *
 * The client never parses a `StateRef`: it echoes the opaque state token as it
 * always has. Worker code never branches on transport or storage: the same
 * calls work on pipe, stdio, Unix and TCP (where the state never leaves the
 * process, so the value is simply held in memory) and on HTTP with or without
 * storage configured.
 *
 * Byte layouts (`STATE_REF_V1.md` §16):
 *
 * ```
 * StateRef (inline)  = 0x01 || codec || body
 * StateRef (stored)  = 0x02 || 0x01 || nonce24 || AEAD(tag_plaintext, STATEREF_TAG_AAD)
 * tag_plaintext      = 0x01 || created_at u64le || size u64le || sha256[32] || id_len u16le || object_id
 * stored object      = 0x01 || nonce24 || AEAD(codec || body, STATEREF_OBJECT_AAD)
 * argument ref       = 0x01 || nonce24 || AEAD(0x01 || size u64le || sha256[32] || id_len u16le || object_id, ARGREF_AAD)
 * ```
 *
 * Sealing scopes, all under the server's one token key:
 *
 * ```
 * STATEREF_TAG_AAD    = "vgi_rpc.stateref.v1\0"        || BASE_AAD(auth, protocol) || call_id
 * STATEREF_OBJECT_AAD = "vgi_rpc.stateref.object.v1\0" || BASE_AAD(auth, protocol) || call_id
 * ARGREF_AAD          = "vgi_rpc.argref.v1\0"          || BASE_AAD(auth, SERVER_SCOPE)
 * ```
 *
 * `BASE_AAD` is exactly the cursor token's AAD ({@link computeAad}).
 */

import type { AuthContext } from "./auth.js";
import { openBytes, SealError, sealBytes } from "./crypto.js";
import type { ErrorCode } from "./error-model.js";
import { DEFAULT_MAX_FETCH_BYTES, isObjectStore, ObjectMissingError, type ObjectStore } from "./external.js";
import { computeAad, SERVER_SCOPE, type TokenScope } from "./http/token.js";
import { defaultObjectCache, type ObjectCache } from "./object-cache.js";
import { emitStateRefEvent } from "./state-ref-events.js";
import {
  MAX_TOKEN_PLAINTEXT_BYTES,
  packToken,
  packTokenAsync,
  TokenCodecError,
  unpackTokenAsync,
} from "./token-codec.js";

const UTF8 = new TextEncoder();
const UTF8_DECODER = new TextDecoder("utf-8", { fatal: true });

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Domain prefix of a stored `StateRef`'s tag AAD. */
export const STATEREF_TAG_AAD_PREFIX = UTF8.encode("vgi_rpc.stateref.v1\0");
/** Domain prefix of a stored `StateRef`'s object AAD. */
export const STATEREF_OBJECT_AAD_PREFIX = UTF8.encode("vgi_rpc.stateref.object.v1\0");
/** Domain prefix of an argument reference's AAD. */
export const ARGREF_AAD_PREFIX = UTF8.encode("vgi_rpc.argref.v1\0");

/** Default `VGI_RPC_STATE_REF_INLINE_BYTES`. */
export const DEFAULT_STATE_REF_INLINE_BYTES = 16384;
/** Environment variable capping the inline payload size. */
export const STATE_REF_INLINE_BYTES_ENV = "VGI_RPC_STATE_REF_INLINE_BYTES";
/** Room left in a continuation request for everything but the two tokens. */
export const TURN_BUDGET_HEADROOM_BYTES = 4096;
/** A request cap below this disables the cursor rule (with a startup warning). */
export const CURSOR_RULE_FLOOR_BYTES = 16384;
/** Default maximum age of cursor tokens, call tokens and stored tags (6 h). */
export const DEFAULT_TOKEN_TTL_SECONDS = 21600;

const FORM_INLINE = 0x01;
const FORM_STORED = 0x02;
const SEAL_VERSION = 0x01;
const TAG_VERSION = 0x01;
const ARGREF_VERSION = 0x01;
const SHA256_LEN = 32;
const MAX_OBJECT_ID_BYTES = 1024;
const TAG_FIXED_LEN = 1 + 8 + 8 + SHA256_LEN + 2;
const ARGREF_FIXED_LEN = 1 + 8 + SHA256_LEN + 2;

/** What the server does with a cursor over its turn budget. */
export type CursorRule = "warn" | "error";

// ---------------------------------------------------------------------------
// Errors (STATE_REF_V1.md §6, vgi large-values-3.4.md §5a)
// ---------------------------------------------------------------------------

/** Base of every `StateRef` / argument-reference failure; carries the canonical code and kind. */
export class StateRefError extends Error {
  /** Canonical code hoisted as `vgi_rpc.error_code`. */
  static readonly errorCode: ErrorCode = "INTERNAL";
  /** The reason hoisted as `vgi_rpc.error_kind`. */
  static readonly errorKind: string = "";
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

/** Unknown form, tag unseal or scope failure, bad layout, expired tag, digest mismatch.
 *  Answered with HTTP 400 like any other token failure. */
export class InvalidStateRefError extends StateRefError {
  /** Canonical code hoisted as `vgi_rpc.error_code`. */
  static override readonly errorCode: ErrorCode = "INVALID_ARGUMENT";
  /** The reason hoisted as `vgi_rpc.error_kind`. */
  static override readonly errorKind = "invalid_state_ref";
}

/** Storage reports a stored `StateRef`'s object absent. */
export class StateRefMissingError extends StateRefError {
  /** Canonical code hoisted as `vgi_rpc.error_code`. */
  static override readonly errorCode: ErrorCode = "NOT_FOUND";
  /** The reason hoisted as `vgi_rpc.error_kind`. */
  static override readonly errorKind = "state_ref_missing";
  constructor() {
    super("stream state object is gone (StateRef object expired or deleted); restart the query");
  }
}

/** A stored object exceeds the server's `maxFetchBytes` (at store or resolve). */
export class StateRefTooLargeError extends StateRefError {
  /** Canonical code hoisted as `vgi_rpc.error_code`. */
  static override readonly errorCode: ErrorCode = "RESOURCE_EXHAUSTED";
  /** The reason hoisted as `vgi_rpc.error_kind`. */
  static override readonly errorKind = "state_ref_too_large";
}

/** `stateRef()` was called outside the handling of a stream's init request. */
export class StateRefMisuseError extends StateRefError {
  /** Canonical code hoisted as `vgi_rpc.error_code`. */
  static override readonly errorCode: ErrorCode = "FAILED_PRECONDITION";
  /** The reason hoisted as `vgi_rpc.error_kind`. */
  static override readonly errorKind = "state_ref_misuse";
}

/** A cursor token exceeds the server's turn budget under the `error` cursor rule. */
export class CursorTooLargeError extends StateRefError {
  /** Canonical code hoisted as `vgi_rpc.error_code`. */
  static override readonly errorCode: ErrorCode = "RESOURCE_EXHAUSTED";
  /** The reason hoisted as `vgi_rpc.error_kind`. */
  static override readonly errorKind = "cursor_too_large";
}

/** A transient storage failure (5xx, timeout) while fetching an object. */
export class StorageUnavailableError extends StateRefError {
  /** Canonical code hoisted as `vgi_rpc.error_code`. */
  static override readonly errorCode: ErrorCode = "UNAVAILABLE";
  /** The reason hoisted as `vgi_rpc.error_kind`. */
  static override readonly errorKind = "storage_unavailable";
}

/** An argument reference does not open, does not match its location, or cannot be resolved here. */
export class InvalidArgumentsRefError extends StateRefError {
  /** Canonical code hoisted as `vgi_rpc.error_code`. */
  static override readonly errorCode: ErrorCode = "INVALID_ARGUMENT";
  /** The reason hoisted as `vgi_rpc.error_kind`. */
  static override readonly errorKind = "invalid_arguments_ref";
}

/** The fetched argument object's length or SHA-256 differs from the sealed values. */
export class ArgumentsRefIntegrityError extends StateRefError {
  /** Canonical code hoisted as `vgi_rpc.error_code`. */
  static override readonly errorCode: ErrorCode = "DATA_LOSS";
  /** The reason hoisted as `vgi_rpc.error_kind`. */
  static override readonly errorKind = "arguments_ref_integrity";
}

/** Storage reports an argument object absent (deleted by lifecycle, never uploaded). */
export class ArgumentsRefMissingError extends StateRefError {
  /** Canonical code hoisted as `vgi_rpc.error_code`. */
  static override readonly errorCode: ErrorCode = "NOT_FOUND";
  /** The reason hoisted as `vgi_rpc.error_kind`. */
  static override readonly errorKind = "arguments_ref_missing";
}

// ---------------------------------------------------------------------------
// Byte helpers
// ---------------------------------------------------------------------------

function concat(...parts: Uint8Array[]): Uint8Array {
  let total = 0;
  for (const p of parts) total += p.byteLength;
  const out = new Uint8Array(total);
  let offset = 0;
  for (const p of parts) {
    out.set(p, offset);
    offset += p.byteLength;
  }
  return out;
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.byteLength !== b.byteLength) return false;
  for (let i = 0; i < a.byteLength; i++) if (a[i] !== b[i]) return false;
  return true;
}

async function sha256(data: Uint8Array): Promise<Uint8Array> {
  const buf = new ArrayBuffer(data.byteLength);
  new Uint8Array(buf).set(data);
  return new Uint8Array(await crypto.subtle.digest("SHA-256", buf));
}

function toHex(bytes: Uint8Array): string {
  let s = "";
  for (const b of bytes) s += b.toString(16).padStart(2, "0");
  return s;
}

function fromHex(hex: string): Uint8Array {
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return out;
}

const SHA256_HEX = /^[0-9a-f]{64}$/;

function u64(value: number): Uint8Array {
  const out = new Uint8Array(8);
  new DataView(out.buffer).setBigUint64(0, BigInt(value), true);
  return out;
}

function u16(value: number): Uint8Array {
  const out = new Uint8Array(2);
  new DataView(out.buffer).setUint16(0, value, true);
  return out;
}

// ---------------------------------------------------------------------------
// AAD
// ---------------------------------------------------------------------------

/**
 * The AEAD scope of a request: who is calling, and which protocol owns the
 * stream. Exactly what the cursor token is bound to.
 */
export function authTokenScope(auth: AuthContext | undefined | null, protocol: string): TokenScope {
  const binding = auth?.claims?.peer_evidence_binding;
  return {
    protocol,
    principal: auth?.authenticated ? (auth.principal ?? "") : null,
    evidenceBinding: typeof binding === "string" && binding ? binding : undefined,
    domain: auth?.domain,
  };
}

function tagAad(base: Uint8Array, callId: Uint8Array): Uint8Array {
  return concat(STATEREF_TAG_AAD_PREFIX, base, callId);
}

function objectAad(base: Uint8Array, callId: Uint8Array): Uint8Array {
  return concat(STATEREF_OBJECT_AAD_PREFIX, base, callId);
}

/** `ARGREF_AAD` for `auth`: bound to the principal (and any peer-evidence binding), not to a protocol. */
export function argumentRefAad(auth: AuthContext | undefined | null): Uint8Array {
  return concat(ARGREF_AAD_PREFIX, computeAad(authTokenScope(auth, SERVER_SCOPE)));
}

// ---------------------------------------------------------------------------
// Configuration and the turn budget
// ---------------------------------------------------------------------------

type EnvLike = Record<string, string | undefined>;

/**
 * Read `VGI_RPC_STATE_REF_INLINE_BYTES` (default 16384).
 *
 * @throws Error when the variable is set to anything but a positive integer.
 */
export function stateRefInlineBytesFromEnv(env?: EnvLike): number {
  const source = env ?? (globalThis as { process?: { env?: EnvLike } }).process?.env ?? {};
  const raw = source[STATE_REF_INLINE_BYTES_ENV];
  if (raw === undefined || raw.trim() === "") return DEFAULT_STATE_REF_INLINE_BYTES;
  const text = raw.trim();
  const value = /^\d+$/.test(text) ? Number(text) : 0;
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${STATE_REF_INLINE_BYTES_ENV} must be a positive integer, got ${JSON.stringify(raw)}`);
  }
  return value;
}

/**
 * The sealed bytes a cursor may take on a continuation, or `null` for unbounded:
 * `floor((maxRequestBytes - 4096) / 1.34) - callTokenBytes`. The `1.34`
 * converts the request's base64 metadata size to sealed bytes. May be negative
 * under a tiny cap.
 */
export function turnBudget(maxRequestBytes: number | null | undefined, callTokenBytes: number): number | null {
  if (maxRequestBytes == null || maxRequestBytes <= 0) return null;
  return Math.floor(((maxRequestBytes - TURN_BUDGET_HEADROOM_BYTES) * 100) / 134) - callTokenBytes;
}

/** Sealed (base64-decoded) length of a base64 token, without decoding it. */
export function sealedLength(tokenBase64: string): number {
  let end = tokenBase64.length;
  while (end > 0 && tokenBase64.charCodeAt(end - 1) === 61 /* = */) end--;
  return Math.floor((end * 3) / 4);
}

/** Options for {@link StateRefRuntime}. */
export interface StateRefRuntimeOptions {
  /** The server's 32-byte AEAD key (shared by every replica). */
  tokenKey: Uint8Array;
  /** Maximum age of a stored tag in seconds; `0` disables. Default 21600. */
  tokenTtl?: number;
  /** Returns the configured storage (or `undefined`), read per use. A storage
   *  that is not an {@link ObjectStore} counts as none: refs stay inline. */
  storage?: () => unknown;
  /** The server's inbound fetch cap. Default 256 MiB. */
  maxFetchBytes?: number;
  /** The advertised request cap (`VGI-Max-Request-Bytes`); unset or `<= 0` is unbounded. */
  maxRequestBytes?: number | null;
  /** `VGI_RPC_STATE_REF_INLINE_BYTES`. Default 16384. */
  inlineBytesCap?: number;
  /** `warn` logs `cursor_large`; `error` raises `cursor_too_large` (only with a request cap). */
  cursorRule?: CursorRule;
  /** The object cache; defaults to the process-wide one. */
  cache?: ObjectCache;
  /** Wall clock in seconds, injectable for tests. */
  clock?: () => number;
}

/**
 * One server's `StateRef` machinery: key, storage, budgets and cache.
 *
 * Built by the HTTP handler; the byte-stream transports never need one, because
 * their state never leaves the process.
 */
export class StateRefRuntime {
  /** The server's 32-byte AEAD key. */
  readonly tokenKey: Uint8Array;
  /** Maximum age of a stored tag in seconds; `0` disables. */
  readonly tokenTtl: number;
  /** The server's inbound fetch cap. */
  readonly maxFetchBytes: number;
  /** The advertised request cap, or `null` when unbounded. */
  readonly maxRequestBytes: number | null;
  /** `VGI_RPC_STATE_REF_INLINE_BYTES`. */
  readonly inlineBytesCap: number;
  /** What an over-budget cursor does. */
  readonly cursorRule: CursorRule;
  private readonly storageSource: (() => unknown) | undefined;
  private readonly cacheOverride: ObjectCache | undefined;
  /** Wall clock in seconds. */
  readonly clock: () => number;
  private readonly cursorRuleDisabled: boolean;

  constructor(options: StateRefRuntimeOptions) {
    const inline = options.inlineBytesCap ?? DEFAULT_STATE_REF_INLINE_BYTES;
    if (!Number.isSafeInteger(inline) || inline <= 0) {
      throw new Error(`stateRefInlineBytes must be a positive integer, got ${inline}`);
    }
    const rule = options.cursorRule ?? "warn";
    if (rule !== "warn" && rule !== "error") {
      throw new Error(`cursorRule must be 'warn' or 'error', got ${JSON.stringify(rule)}`);
    }
    this.tokenKey = options.tokenKey;
    this.tokenTtl = options.tokenTtl ?? DEFAULT_TOKEN_TTL_SECONDS;
    this.storageSource = options.storage;
    this.maxFetchBytes = options.maxFetchBytes ?? DEFAULT_MAX_FETCH_BYTES;
    this.maxRequestBytes =
      options.maxRequestBytes != null && options.maxRequestBytes > 0 ? options.maxRequestBytes : null;
    this.inlineBytesCap = inline;
    this.cursorRule = rule;
    this.cacheOverride = options.cache;
    this.clock = options.clock ?? (() => Math.floor(Date.now() / 1000));
    this.cursorRuleDisabled = this.maxRequestBytes !== null && this.maxRequestBytes < CURSOR_RULE_FLOOR_BYTES;
    if (this.cursorRuleDisabled) {
      emitStateRefEvent(
        "cursor_rule_disabled",
        {
          max_request_bytes: this.maxRequestBytes!,
          message: `max_request_bytes (${this.maxRequestBytes}) is below ${CURSOR_RULE_FLOOR_BYTES}: the cursor rule is disabled and stream continuations may not fit`,
        },
        "warn",
      );
    }
  }

  /** The object cache (the process-wide one unless overridden). */
  get cache(): ObjectCache {
    return this.cacheOverride ?? defaultObjectCache();
  }

  /** The configured object store, or `null` when there is none. */
  get storage(): ObjectStore | null {
    const candidate = this.storageSource?.();
    return isObjectStore(candidate) ? candidate : null;
  }

  /** Whether over-budget cursors warn or error at all (off below the 16 KiB floor). */
  get cursorRuleEnabled(): boolean {
    return !this.cursorRuleDisabled;
  }

  /** Largest decompressed `StateRef` value: 64 MiB or `maxFetchBytes`, whichever is larger. */
  get decompressCap(): number {
    return Math.max(MAX_TOKEN_PLAINTEXT_BYTES, this.maxFetchBytes);
  }

  /** {@link turnBudget} for this server's request cap. */
  turnBudget(callTokenBytes: number): number | null {
    return turnBudget(this.maxRequestBytes, callTokenBytes);
  }

  /** `state_ref_inline_bytes`: `min(cap, floor(turn_budget / 4))`, floored at 0. */
  inlineBytes(callTokenBytes: number): number {
    const budget = this.turnBudget(callTokenBytes);
    if (budget === null) return this.inlineBytesCap;
    return Math.max(0, Math.min(this.inlineBytesCap, Math.floor(budget / 4)));
  }

  /**
   * Apply the cursor rule to one sealed cursor.
   *
   * @returns `null` when the cursor fits (or the rule is off), else the
   *   `cursor_large` warning text (`warn` rule).
   * @throws CursorTooLargeError over budget under the `error` rule.
   */
  checkCursor(args: { cursorBytes: number; callTokenBytes: number; protocol: string; method: string }): string | null {
    if (this.cursorRuleDisabled) return null;
    const budget = this.turnBudget(args.callTokenBytes);
    if (budget === null || args.cursorBytes <= budget) return null;
    const detail =
      `stream state for ${args.protocol}.${args.method} is ${args.cursorBytes} bytes, over this server's turn budget of ` +
      `${budget} bytes (VGI-Max-Request-Bytes ${this.maxRequestBytes}). Wrap data that does not change ` +
      "across turns in ctx.state_ref().";
    const fields = {
      protocol: args.protocol,
      method: args.method,
      cursor_bytes: args.cursorBytes,
      budget,
    };
    if (this.cursorRule === "error") {
      emitStateRefEvent("cursor_too_large", { ...fields, message: `cursor_too_large: ${detail}` }, "warn");
      throw new CursorTooLargeError(`cursor_too_large: ${detail}`);
    }
    emitStateRefEvent("cursor_large", { ...fields, message: `cursor_large: ${detail}` }, "warn");
    return `cursor_large: ${detail}`;
  }
}

// ---------------------------------------------------------------------------
// StateRef
// ---------------------------------------------------------------------------

const STATE_REF_BRAND = Symbol.for("vgi_rpc.StateRef");

/** The JSON key a `StateRef` serializes under in the default JSON state serializer. */
export const STATE_REF_JSON_KEY = "$vgi_rpc.state_ref";

/** Something `stateRef()` can serialize: a record with a byte form. */
export interface StateRefRecord {
  /** The record's serialized bytes. */
  toBytes?(): Uint8Array;
  /** Alternative spelling, for records that already expose it. */
  serializeToBytes?(): Uint8Array;
}

/** A value `stateRef()` accepts: raw bytes or a serializable record. */
export type StateRefValue = Uint8Array | ArrayBuffer | StateRefRecord;

/** Refs serialized during a mint's collect pass (synchronous, so a module slot is safe). */
let collecting: Set<StateRef> | null = null;

function base64(bytes: Uint8Array): string {
  let s = "";
  for (let i = 0; i < bytes.length; i += 0x8000) s += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  return btoa(s);
}

function unbase64(text: string): Uint8Array {
  const bin = atob(text);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

/**
 * An immutable value carried in stream state by reference.
 *
 * Create one with `ctx.stateRef(value)` while handling a stream's init request;
 * keep it in a field of your state; read it back with `ctx.resolve(ref)` on any
 * turn.
 *
 * The value is held in memory until the state is first serialized into an
 * HTTP state token. That serialization decides the form -- inline (a
 * compressed payload inside the token) or stored (uploaded once, a sealed tag
 * in the token) -- and caches the encoded bytes, so later serializations
 * re-emit them exactly. A ref read back from a token holds only those bytes.
 * On pipe, stdio, Unix and TCP nothing is ever encoded.
 *
 * A custom state serializer stores {@link toBytes} and rebuilds with
 * {@link StateRef.fromBytes}; the default JSON serializer does it through
 * `toJSON` and its reviver.
 */
export class StateRef {
  /** @internal */
  _value: Uint8Array | null;
  /** @internal */
  _record: unknown;
  /** @internal */
  _encoded: Uint8Array | null;

  private constructor(value: Uint8Array | null, record: unknown, encoded: Uint8Array | null) {
    this._value = value;
    this._record = record;
    this._encoded = encoded;
    Object.defineProperty(this, STATE_REF_BRAND, { value: true });
  }

  /** @internal */
  static _fromValue(value: Uint8Array, record: unknown = null): StateRef {
    return new StateRef(value, record, null);
  }

  /** Rebuild a ref from its encoded bytes (validated only when resolved). */
  static fromBytes(encoded: Uint8Array): StateRef {
    const copy = new Uint8Array(encoded.byteLength);
    copy.set(encoded);
    return new StateRef(null, null, copy);
  }

  /** Whether `value` is a `StateRef` (also one built by another copy of this module). */
  static isStateRef(value: unknown): value is StateRef {
    return value !== null && typeof value === "object" && (value as Record<symbol, unknown>)[STATE_REF_BRAND] === true;
  }

  /** Where the value lives: `memory` (not yet encoded), `inline` or `stored`. */
  get form(): "memory" | "inline" | "stored" | "invalid" {
    if (this._encoded === null) return "memory";
    if (this._encoded[0] === FORM_INLINE) return "inline";
    if (this._encoded[0] === FORM_STORED) return "stored";
    return "invalid";
  }

  /** The encoded bytes once the form is decided, else `null`. */
  get encoded(): Uint8Array | null {
    return this._encoded;
  }

  /**
   * The field's wire bytes. Inside a state-token mint the form has already
   * been decided and these are the cached encoded bytes. Outside one -- a
   * caller serializing state for its own reasons -- an un-encoded ref renders
   * inline without caching, so the next token mint still decides properly.
   */
  toBytes(): Uint8Array {
    if (this._encoded !== null) return this._encoded;
    if (collecting !== null) collecting.add(this);
    return concat(new Uint8Array([FORM_INLINE]), packToken(this._value!));
  }

  /** JSON form used by the default state serializer. */
  toJSON(): Record<string, string> {
    return { [STATE_REF_JSON_KEY]: base64(this.toBytes()) };
  }

  /** Describe the form and size, never the value. */
  toString(): string {
    if (this._encoded === null) return `StateRef(form='memory', bytes=${this._value!.byteLength})`;
    return `StateRef(form='${this.form}', encodedBytes=${this._encoded.byteLength})`;
  }
}

/** Reviver for {@link STATE_REF_JSON_KEY} objects (used by `jsonStateSerializer`). */
export function reviveStateRef(value: unknown): unknown {
  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    const encoded = (value as Record<string, unknown>)[STATE_REF_JSON_KEY];
    if (typeof encoded === "string" && Object.keys(value).length === 1) {
      return StateRef.fromBytes(unbase64(encoded));
    }
  }
  return value;
}

/** Everything needed to decide a ref's form and seal it, for one stream's mint. */
export interface StateRefEncoder {
  runtime: StateRefRuntime;
  baseAad: Uint8Array;
  callId: Uint8Array;
  inlineBytes: number;
  protocol: string;
  method: string;
}

/** Build the encoder for one stream. */
export function stateRefEncoder(
  runtime: StateRefRuntime,
  args: { auth?: AuthContext | null; protocol: string; method: string; callId: Uint8Array; callTokenBytes: number },
): StateRefEncoder {
  return {
    runtime,
    baseAad: computeAad(authTokenScope(args.auth, args.protocol)),
    callId: args.callId,
    inlineBytes: runtime.inlineBytes(args.callTokenBytes),
    protocol: args.protocol,
    method: args.method,
  };
}

/** Decide a value's form and encode it (§5.2), storing it when it is stored. */
export async function encodeStateRefValue(value: Uint8Array, encoder: StateRefEncoder): Promise<Uint8Array> {
  // Compressed on every runtime: raw DEFLATE where there is no zstd encoder.
  const payload = await packTokenAsync(value);
  const runtime = encoder.runtime;
  const storage = runtime.storage;
  if (storage === null || payload.byteLength <= encoder.inlineBytes) {
    emitStateRefEvent("state_ref.store", {
      protocol: encoder.protocol,
      method: encoder.method,
      payload_bytes: payload.byteLength,
      object_bytes: 0,
      form: "inline",
    });
    return concat(new Uint8Array([FORM_INLINE]), payload);
  }
  const objectBytes = sealBytes(payload, runtime.tokenKey, {
    aad: objectAad(encoder.baseAad, encoder.callId),
    version: SEAL_VERSION,
  });
  if (objectBytes.byteLength > runtime.maxFetchBytes) {
    throw new StateRefTooLargeError(
      `StateRef object for ${encoder.protocol}.${encoder.method} is ${objectBytes.byteLength} bytes, over this ` +
        `server's max_fetch_bytes (${runtime.maxFetchBytes}); no replica could fetch it back`,
    );
  }
  const objectId = await storage.putObject(objectBytes);
  const idBytes = UTF8.encode(objectId);
  if (idBytes.byteLength < 1 || idBytes.byteLength > MAX_OBJECT_ID_BYTES) {
    throw new Error(`storage returned an object_id of ${idBytes.byteLength} bytes (must be 1-1024)`);
  }
  const tagPlaintext = concat(
    new Uint8Array([TAG_VERSION]),
    u64(Math.floor(runtime.clock())),
    u64(objectBytes.byteLength),
    await sha256(objectBytes),
    u16(idBytes.byteLength),
    idBytes,
  );
  const tag = sealBytes(tagPlaintext, runtime.tokenKey, {
    aad: tagAad(encoder.baseAad, encoder.callId),
    version: SEAL_VERSION,
  });
  emitStateRefEvent("state_ref.store", {
    protocol: encoder.protocol,
    method: encoder.method,
    payload_bytes: payload.byteLength,
    object_bytes: objectBytes.byteLength,
    form: "stored",
  });
  return concat(new Uint8Array([FORM_STORED]), tag);
}

/**
 * Serialize a stream state with its `StateRef`s encoded (the HTTP cursor mint).
 *
 * `serialize` runs once to discover which refs the state holds that are still
 * un-encoded (refs created on this init turn); those are encoded -- the form
 * decided and, when stored, uploaded once -- and `serialize` runs again so the
 * state carries the cached encoded bytes. A state with no un-encoded refs
 * (every continuation) is serialized exactly once.
 */
export async function serializeWithStateRefs(
  serialize: () => Uint8Array,
  encoder: StateRefEncoder | null,
): Promise<Uint8Array> {
  if (encoder === null) return serialize();
  const found = new Set<StateRef>();
  const outer = collecting;
  collecting = found;
  let first: Uint8Array;
  try {
    first = serialize();
  } finally {
    collecting = outer;
  }
  if (found.size === 0) return first;
  for (const ref of found) {
    if (ref._encoded === null) ref._encoded = await encodeStateRefValue(ref._value!, encoder);
  }
  return serialize();
}

async function unpackValue(payload: Uint8Array, cap: number): Promise<Uint8Array> {
  try {
    return await unpackTokenAsync(payload, cap);
  } catch (error) {
    if (error instanceof TokenCodecError)
      throw new InvalidStateRefError(`StateRef payload is malformed: ${error.message}`);
    throw error;
  }
}

async function fetchObject(
  storage: ObjectStore,
  objectId: string,
  maxBytes: number,
  missing: () => Error,
): Promise<Uint8Array> {
  try {
    return await storage.get(objectId, { maxBytes });
  } catch (error) {
    if (error instanceof ObjectMissingError || (error as Error)?.name === "ObjectMissingError") throw missing();
    if (error instanceof StateRefError) throw error;
    const kind = error instanceof Error ? error.name : typeof error;
    const message = error instanceof Error ? error.message : String(error);
    throw new StorageUnavailableError(`object storage fetch failed (${kind}): ${message}`);
  }
}

async function resolveStored(
  envelope: Uint8Array,
  runtime: StateRefRuntime,
  base: Uint8Array,
  callId: Uint8Array,
): Promise<Uint8Array> {
  let tag: Uint8Array;
  try {
    tag = openBytes(envelope, runtime.tokenKey, { aad: tagAad(base, callId), version: SEAL_VERSION });
  } catch (error) {
    if (error instanceof SealError) {
      throw new InvalidStateRefError("StateRef tag does not open (tampered, or minted for another stream)");
    }
    throw error;
  }
  if (tag.byteLength < TAG_FIXED_LEN || tag[0] !== TAG_VERSION) {
    throw new InvalidStateRefError("StateRef tag has an unknown version or a malformed layout");
  }
  const view = new DataView(tag.buffer, tag.byteOffset, tag.byteLength);
  const createdAt = Number(view.getBigUint64(1, true));
  const size = Number(view.getBigUint64(9, true));
  const digest = tag.slice(17, 17 + SHA256_LEN);
  const idLen = view.getUint16(17 + SHA256_LEN, true);
  if (idLen < 1 || idLen > MAX_OBJECT_ID_BYTES || TAG_FIXED_LEN + idLen !== tag.byteLength) {
    throw new InvalidStateRefError("StateRef tag has a malformed layout");
  }
  let objectId: string;
  try {
    objectId = UTF8_DECODER.decode(tag.subarray(TAG_FIXED_LEN));
  } catch {
    throw new InvalidStateRefError("StateRef tag has a malformed layout");
  }
  if (runtime.tokenTtl > 0 && Math.floor(runtime.clock()) - createdAt > runtime.tokenTtl) {
    throw new InvalidStateRefError("StateRef expired");
  }
  if (size > runtime.maxFetchBytes) {
    throw new StateRefTooLargeError(
      `StateRef object is ${size} bytes, over this server's max_fetch_bytes (${runtime.maxFetchBytes})`,
    );
  }
  const storage = runtime.storage;
  if (storage === null) {
    throw new InvalidStateRefError("a stored StateRef cannot be resolved: this server has no object storage");
  }
  const { value, hit } = await runtime.cache.getOrFetch(objectId, async () => {
    const objectBytes = await fetchObject(storage, objectId, runtime.maxFetchBytes, () => new StateRefMissingError());
    if (objectBytes.byteLength !== size) throw new InvalidStateRefError("StateRef object size mismatch");
    if (!bytesEqual(await sha256(objectBytes), digest)) {
      throw new InvalidStateRefError("StateRef object digest mismatch");
    }
    let payload: Uint8Array;
    try {
      payload = openBytes(objectBytes, runtime.tokenKey, { aad: objectAad(base, callId), version: SEAL_VERSION });
    } catch (error) {
      if (error instanceof SealError) throw new InvalidStateRefError("StateRef object does not open");
      throw error;
    }
    return await unpackValue(payload, runtime.decompressCap);
  });
  emitStateRefEvent("state_ref.resolve", { form: "stored", cache: hit ? "hit" : "miss", bytes: value.byteLength });
  return value;
}

/**
 * Resolve encoded `StateRef` bytes in one stream's scope (also the call
 * token's `schemas_ref`).
 *
 * @param runtime The server runtime, or `null` off HTTP (only inline refs then).
 */
export async function resolveEncodedStateRef(
  encoded: Uint8Array,
  runtime: StateRefRuntime | null,
  scope: { auth?: AuthContext | null; protocol: string; callId?: Uint8Array | null },
): Promise<Uint8Array> {
  if (encoded.byteLength === 0) throw new InvalidStateRefError("StateRef is empty");
  const form = encoded[0];
  if (form === FORM_INLINE) {
    const value = await unpackValue(encoded.subarray(1), runtime?.decompressCap ?? MAX_TOKEN_PLAINTEXT_BYTES);
    emitStateRefEvent("state_ref.resolve", { form: "inline", cache: "miss", bytes: value.byteLength });
    return value;
  }
  if (form === FORM_STORED) {
    if (runtime === null || !scope.callId) {
      throw new InvalidStateRefError("a stored StateRef cannot be resolved outside an HTTP stream turn");
    }
    return resolveStored(
      encoded.subarray(1),
      runtime,
      computeAad(authTokenScope(scope.auth, scope.protocol)),
      scope.callId,
    );
  }
  throw new InvalidStateRefError(`StateRef has unknown form 0x${form.toString(16).padStart(2, "0")}`);
}

/** Encode a value directly as a `StateRef` (the call token's `schemas_ref`). */
export function inlineStateRefBytes(value: Uint8Array): Uint8Array {
  return concat(new Uint8Array([FORM_INLINE]), packToken(value));
}

// ---------------------------------------------------------------------------
// The per-request scope (what CallContext delegates to)
// ---------------------------------------------------------------------------

/** Options for {@link StateRefScope}. */
export interface StateRefScopeOptions {
  /** The server's runtime (HTTP), or omitted off HTTP. */
  runtime?: StateRefRuntime | null;
  /** The request's authentication context. */
  auth?: AuthContext | null;
  /** Owning protocol's wire name. */
  protocol?: string;
  /** Method name. */
  method?: string;
  /** The stream's call id, known on continuations. */
  callId?: Uint8Array | null;
  /** Whether this is the stream init handler's own context. */
  init?: boolean;
}

function fnv1a(bytes: Uint8Array): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < bytes.byteLength; i++) {
    h ^= bytes[i];
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return (h ^ bytes.byteLength) >>> 0;
}

/**
 * The `StateRef` view of one request. The framework attaches one to every
 * call context it builds: on HTTP it carries the server runtime and the
 * stream's call id; on the byte-stream transports there is no runtime (values
 * never leave memory). `init` is true only for the stream init handler's own
 * context -- the one place {@link stateRef} is allowed.
 */
export class StateRefScope {
  /** The server runtime (HTTP), or `null` off HTTP. */
  readonly runtime: StateRefRuntime | null;
  /** The request's authentication context. */
  readonly auth: AuthContext | null;
  /** Owning protocol's wire name. */
  readonly protocol: string;
  /** Method name. */
  readonly method: string;
  /** @internal Set by the HTTP dispatcher once the stream's call id is known. */
  callId: Uint8Array | null;
  /** Whether this is the stream init handler's own context. */
  readonly init: boolean;
  private readonly memoByIdentity = new WeakMap<object, StateRef>();
  private readonly memoByBytes = new Map<number, StateRef[]>();

  constructor(options: StateRefScopeOptions = {}) {
    this.runtime = options.runtime ?? null;
    this.auth = options.auth ?? null;
    this.protocol = options.protocol ?? "";
    this.method = options.method ?? "";
    this.callId = options.callId ?? null;
    this.init = options.init ?? false;
  }

  /**
   * Wrap `value` in a `StateRef`. Byte-equal or identical values within one
   * init share one ref (and so at most one stored object).
   *
   * @throws StateRefMisuseError when not handling a stream's init request.
   * @throws TypeError when `value` is neither bytes nor a serializable record.
   */
  stateRef(value: StateRefValue): StateRef {
    if (!this.init) {
      throw new StateRefMisuseError(
        `state_ref() may only be called while handling a stream's init request (${this.protocol}.${this.method})`,
      );
    }
    const key = value as object;
    const remembered = this.memoByIdentity.get(key);
    if (remembered !== undefined) return remembered;
    let data: Uint8Array;
    let record: unknown = null;
    if (value instanceof Uint8Array) {
      data = new Uint8Array(value.byteLength);
      data.set(value);
    } else if (value instanceof ArrayBuffer) {
      data = new Uint8Array(value.slice(0));
    } else if (value !== null && typeof value === "object") {
      const serialize = (value as StateRefRecord).toBytes ?? (value as StateRefRecord).serializeToBytes;
      if (typeof serialize !== "function") {
        throw new TypeError("stateRef() takes bytes or a record with toBytes()/serializeToBytes()");
      }
      const out = serialize.call(value);
      if (!(out instanceof Uint8Array)) throw new TypeError("a stateRef() record's toBytes() did not return bytes");
      data = out;
      record = value;
    } else {
      throw new TypeError("stateRef() takes bytes or a record with toBytes()/serializeToBytes()");
    }
    const hash = fnv1a(data);
    const bucket = this.memoByBytes.get(hash) ?? [];
    let ref = bucket.find((candidate) => bytesEqual(candidate._value!, data));
    if (ref === undefined) {
      ref = StateRef._fromValue(data, record);
      bucket.push(ref);
      this.memoByBytes.set(hash, bucket);
    }
    this.memoByIdentity.set(key, ref);
    return ref;
  }

  /** The value bytes `ref` names; valid on every turn and transport. */
  resolve(ref: StateRef): Promise<Uint8Array>;
  /**
   * Resolve `ref` and decode it with `decode`. A ref still in memory that
   * was made from a record returns that original record instance.
   */
  resolve<T>(ref: StateRef, decode: (bytes: Uint8Array) => T | Promise<T>): Promise<T>;
  async resolve<T>(ref: StateRef, decode?: (bytes: Uint8Array) => T | Promise<T>): Promise<Uint8Array | T> {
    if (!StateRef.isStateRef(ref)) throw new TypeError("resolve() takes a StateRef");
    if (decode !== undefined && ref._value !== null && ref._record !== null) return ref._record as T;
    const bytes = await this.resolveBytes(ref);
    return decode === undefined ? bytes : decode(bytes);
  }

  private async resolveBytes(ref: StateRef): Promise<Uint8Array> {
    if (ref._value !== null) return ref._value;
    return resolveEncodedStateRef(ref._encoded!, this.runtime, {
      auth: this.auth,
      protocol: this.protocol,
      callId: this.callId,
    });
  }

  /**
   * Resolve an argument reference minted by this server's `__upload_url__`
   * grant, for the presenting caller.
   *
   * @throws InvalidArgumentsRefError off HTTP (byte-stream transports vend no
   *   grants), or for any failure {@link resolveArgumentRef} maps to it.
   */
  async resolveArgumentRef(ref: Uint8Array, location: { size: number; sha256: string }): Promise<Uint8Array> {
    if (this.runtime === null) {
      throw new InvalidArgumentsRefError("argument references can only be resolved by an HTTP server");
    }
    return resolveArgumentRef(ref, {
      size: location.size,
      sha256: location.sha256,
      tokenKey: this.runtime.tokenKey,
      auth: this.auth,
      storage: this.runtime.storage,
      maxFetchBytes: this.runtime.maxFetchBytes,
      cache: this.runtime.cache,
    });
  }
}

// ---------------------------------------------------------------------------
// Argument references (STATE_REF_V1.md §10.2)
// ---------------------------------------------------------------------------

/** The opened contents of an argument reference. */
export interface ArgumentRef {
  /** The sealed byte length. */
  size: number;
  /** The sealed SHA-256, 64 lowercase hex. */
  sha256: string;
  /** The storage id. */
  objectId: string;
}

/**
 * Seal an argument reference for one granted upload.
 *
 * Minted from the *claimed* `(size, sha256)` without seeing the bytes: storage
 * enforces them on the PUT where it can, and the resolver verifies them on the
 * GET. No expiry: on an unauthenticated server a reference is a bearer
 * capability for as long as the object lives.
 */
export function mintArgumentRef(
  tokenKey: Uint8Array,
  auth: AuthContext | undefined | null,
  args: { size: number; sha256: string; objectId: string },
): Uint8Array {
  if (!Number.isSafeInteger(args.size) || args.size < 1) {
    throw new Error(`argument ref size must be an integer >= 1, got ${args.size}`);
  }
  if (!SHA256_HEX.test(args.sha256)) throw new Error("argument ref sha256 must be 64 lowercase hex characters");
  const idBytes = UTF8.encode(args.objectId);
  if (idBytes.byteLength < 1 || idBytes.byteLength > MAX_OBJECT_ID_BYTES) {
    throw new Error(`object_id must be 1-${MAX_OBJECT_ID_BYTES} bytes of UTF-8, got ${idBytes.byteLength}`);
  }
  const plaintext = concat(
    new Uint8Array([ARGREF_VERSION]),
    u64(args.size),
    fromHex(args.sha256),
    u16(idBytes.byteLength),
    idBytes,
  );
  const sealed = sealBytes(plaintext, tokenKey, { aad: argumentRefAad(auth), version: SEAL_VERSION });
  emitStateRefEvent("arguments_ref.mint", { size: args.size });
  return sealed;
}

/**
 * Open an argument reference sealed for `auth`.
 *
 * @throws InvalidArgumentsRefError for a wrong key, principal or scope,
 *   tampering, an unknown version, or a malformed layout.
 */
export function openArgumentRef(
  tokenKey: Uint8Array,
  auth: AuthContext | undefined | null,
  ref: Uint8Array,
): ArgumentRef {
  let plaintext: Uint8Array;
  try {
    plaintext = openBytes(ref, tokenKey, { aad: argumentRefAad(auth), version: SEAL_VERSION });
  } catch (error) {
    if (error instanceof SealError) {
      throw new InvalidArgumentsRefError(
        "argument reference does not open (tampered, or granted to another principal or server)",
      );
    }
    throw error;
  }
  if (plaintext.byteLength < ARGREF_FIXED_LEN || plaintext[0] !== ARGREF_VERSION) {
    throw new InvalidArgumentsRefError("argument reference has an unknown version or a malformed layout");
  }
  const view = new DataView(plaintext.buffer, plaintext.byteOffset, plaintext.byteLength);
  const size = Number(view.getBigUint64(1, true));
  const digest = plaintext.subarray(9, 9 + SHA256_LEN);
  const idLen = view.getUint16(9 + SHA256_LEN, true);
  if (idLen < 1 || idLen > MAX_OBJECT_ID_BYTES || ARGREF_FIXED_LEN + idLen !== plaintext.byteLength) {
    throw new InvalidArgumentsRefError("argument reference has a malformed layout");
  }
  let objectId: string;
  try {
    objectId = UTF8_DECODER.decode(plaintext.subarray(ARGREF_FIXED_LEN));
  } catch {
    throw new InvalidArgumentsRefError("argument reference has a malformed layout");
  }
  return { size, sha256: toHex(digest), objectId };
}

/** Options for {@link resolveArgumentRef}. */
export interface ResolveArgumentRefOptions {
  /** The claimed byte length (`ArgumentsLocation.size`). */
  size: number;
  /** The claimed SHA-256, 64 lowercase hex (`ArgumentsLocation.sha256`). */
  sha256: string;
  /** The server's AEAD key. */
  tokenKey: Uint8Array;
  /** The presenting request's authentication context. */
  auth?: AuthContext | null;
  /** The server's object store, or `null`. */
  storage: ObjectStore | null | undefined;
  /** The server's inbound fetch cap. */
  maxFetchBytes: number;
  /** The object cache; defaults to the process-wide one. */
  cache?: ObjectCache;
}

/**
 * Open, fetch (through the object cache) and verify an argument reference --
 * the library primitive the vgi layer resolves `BindRequest.arguments_location`
 * with.
 *
 * Order: validate the claimed location; open the ref with `ARGREF_AAD`; check
 * the sealed size and SHA-256 equal the claimed ones; look the `object_id` up
 * in the object cache; on a miss, single-flight `storage.get`; verify length
 * and SHA-256; return.
 *
 * @throws InvalidArgumentsRefError malformed location, no storage, a ref that
 *   does not open for this caller, or sealed values that differ from the claim.
 * @throws ArgumentsRefIntegrityError the fetched bytes' length or digest differs.
 * @throws ArgumentsRefMissingError storage reports the object absent.
 * @throws StorageUnavailableError a transient storage failure.
 */
export async function resolveArgumentRef(ref: Uint8Array, options: ResolveArgumentRefOptions): Promise<Uint8Array> {
  const { size, sha256: claimed, maxFetchBytes } = options;
  if (!Number.isSafeInteger(size) || size < 1) {
    throw new InvalidArgumentsRefError(`argument location size must be an integer >= 1, got ${size}`);
  }
  if (typeof claimed !== "string" || !SHA256_HEX.test(claimed)) {
    throw new InvalidArgumentsRefError("argument location sha256 must be 64 lowercase hex characters");
  }
  const storage = options.storage ?? null;
  if (storage === null) {
    throw new InvalidArgumentsRefError("this server has no object storage configured to resolve argument references");
  }
  const opened = openArgumentRef(options.tokenKey, options.auth, ref);
  if (opened.size !== size || opened.sha256 !== claimed) {
    throw new InvalidArgumentsRefError("argument reference does not match the size and sha256 it was sent with");
  }
  if (size > maxFetchBytes) {
    throw new InvalidArgumentsRefError(
      `argument object is ${size} bytes, over this server's max_fetch_bytes (${maxFetchBytes})`,
    );
  }
  const cache = options.cache ?? defaultObjectCache();
  const expected = fromHex(claimed);
  const { value, hit } = await cache.getOrFetch(opened.objectId, async () => {
    const data = await fetchObject(
      storage,
      opened.objectId,
      maxFetchBytes,
      () => new ArgumentsRefMissingError("the uploaded argument object is gone (expired or deleted)"),
    );
    if (data.byteLength !== size) {
      throw new ArgumentsRefIntegrityError(`argument object size mismatch: expected ${size}, got ${data.byteLength}`);
    }
    if (!bytesEqual(await sha256(data), expected)) {
      throw new ArgumentsRefIntegrityError("argument object sha256 mismatch");
    }
    return data;
  });
  if (value.byteLength !== size) {
    // A cached value always passed the checks above for this object id; a
    // length change means the id was reused, which storage never does.
    throw new ArgumentsRefIntegrityError(`argument object size mismatch: expected ${size}, got ${value.byteLength}`);
  }
  emitStateRefEvent("arguments_ref.resolve", { cache: hit ? "hit" : "miss", bytes: value.byteLength });
  return value;
}
