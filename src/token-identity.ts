// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// `vgi_rpc.Identity.v1` -- resolving a credential, and minting a grant.
//
// Identity lives here, at the RPC layer, rather than in any application
// protocol: a bearer token is not a VGI concept, the auth primitives it builds
// on (`AuthContext`, `chainAuthenticate`, `AuthUnavailableError`) are already
// here, and implementing it once is the whole point. It was previously an HTTP
// JSON route, `POST {prefix}/__introspect_token__` (still served by
// `src/http/introspect.ts`), which meant it existed only on one transport and
// had to be hand-written in every port.
//
// Two methods share this module's guards, and they are guarded *differently*
// on purpose.
//
// `introspect_token` answers "which principal is this credential" for a reverse
// proxy that terminates the only public listener. The answer is an identity
// assertion made by the thing being protected, which the asker then acts on
// using credentials the worker does not hold -- storage credentials,
// entitlement lookups, policy-tier selection. "Trust it as much as you trust
// the worker" is the wrong frame: it must be trusted *more*. So every rejection
// is uniform, the caller must be on an allowlist with no permissive default, a
// JWS-shaped subject never reaches the resolver, and the whole thing is rate
// limited.
//
// `issue_grant` mints a credential for the *calling* user, so it is not an
// oracle about anybody else. It therefore needs no allowlist and no rate limit,
// and its rejections are deliberately *actionable*: a console that cannot tell
// "your login is too old" from "no" cannot know to re-prompt.
//
// Errors carry a stable `errorKind`. That is load-bearing rather than
// decorative: these used to be a bespoke HTTP route whose callers classified
// definitive-vs-transient on the HTTP status (404 vs 503). As protocol methods
// every handler exception surfaces the same way -- hoisted onto the error batch
// as `vgi_rpc.error_kind` -- so the kind is now the *only* signal a caller has.
// A caller that negative-caches a transient failure locks out valid users; one
// that retries a definitive rejection hammers the worker.

import {
  batchFromColumns,
  binary,
  field,
  float64,
  int64,
  list,
  schema as makeSchema,
  serializeBatch,
  utf8,
} from "./arrow/index.js";
import type { AuthContext } from "./auth.js";
import { AuthUnavailableError } from "./http/unauthorized.js";
import { Protocol } from "./protocol.js";
import type { CallContext } from "./types.js";
import { sha256Hex } from "./util/web-crypto.js";

/** The wire name of the identity protocol.
 *
 *  Framework-owned, under the reserved `vgi_rpc.` prefix, so an application
 *  cannot register a protocol that impersonates it. */
export const IDENTITY_PROTOCOL_NAME = "vgi_rpc.Identity.v1";

/** Three dot-separated base64url segments -- a JWS. Such a credential is
 *  validated locally against a key set and MUST NOT be routed to a resolver:
 *  doing so sends a bearer token the asker may itself have rejected (expired,
 *  wrong audience) to a third party that might accept it. */
const JWS_SHAPED = /^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*$/;

/** Cap on a credential we will even attempt to resolve. Anything longer is not
 *  a bearer token; refusing early keeps a resolver from being handed
 *  megabytes.
 *
 *  Measured in **UTF-8 bytes**, which is the unit the purpose implies -- what
 *  is bounded is what a resolver would have to handle, and megabytes are
 *  bytes. Spelled out because the ports reached for three different units:
 *  codepoints in Python and Rust, UTF-16 code units in Java, C# and (until
 *  now) TypeScript, bytes in Go and C++. All three agree for an ASCII
 *  credential, which every real bearer token is, so this bites only on a
 *  multibyte one -- and `"x".length` counting UTF-16 units means this port was
 *  measuring a multibyte credential *short*. Bytes is also the most
 *  conservative of the three, so standardising on it can only refuse earlier. */
export const MAX_TOKEN_BYTES = 4096;

/** Default cache window handed to a caller when a resolver names none.
 *
 *  Treat it as an authorization window -- and therefore as the revocation lag
 *  -- for any path the asker serves without re-presenting the credential. */
export const DEFAULT_IDENTITY_TTL_SECONDS = 300;

/** Introspections admitted per caller per one-second window, by default. */
export const DEFAULT_INTROSPECT_RATE_LIMIT = 20;

/** How recently a caller must have authenticated to mint a grant, in seconds. */
export const DEFAULT_MAX_AUTH_AGE_SECONDS = 900;

/**
 * Return a SHA-256 hex digest of `token`, for diagnostics.
 *
 * The credential itself must never reach a log, a span, or an error message. A
 * digest is stable enough to correlate one credential's failures across records
 * without being the credential.
 */
export async function tokenDigest(token: string): Promise<string> {
  return sha256Hex(new TextEncoder().encode(token));
}

// ---------------------------------------------------------------------------
// The error taxonomy
// ---------------------------------------------------------------------------
//
// Every class below extends `Error` rather than being a bare `Error` with a
// name -- which matters more here than it looks. `chainAuthenticate` classifies
// by constructor identity (`err.constructor === Error`), so a *plain* Error
// thrown from an authenticator is read as "not my credential, try the next
// one". A subclass is not, and propagates. That is the property
// `IdentityUnavailableError` depends on; see its docstring.

/**
 * The caller may not introspect.
 *
 * Definitive: a caller may cache this. Authentication is not the same
 * capability as introspection -- a deployment where any valid credential may
 * introspect lets any user test guesses of any other user's credential at
 * unlimited rate, and resolve a stolen one to its owner.
 */
export class IntrospectionRefusedError extends Error {
  /** Typed `vgi_rpc.error_kind` marker for this error class. */
  static readonly errorKind = "introspection_refused";
  /** Typed marker hoisted onto the error batch metadata. */
  readonly errorKind = "introspection_refused";
  constructor(message: string) {
    super(message);
    this.name = "IntrospectionRefusedError";
  }
}

/**
 * The subject credential did not resolve.
 *
 * Definitive, and deliberately uniform: unknown, expired and malformed are one
 * answer, because reporting which would confirm that a guessed credential
 * exists.
 */
export class TokenUnresolvedError extends Error {
  /** Typed `vgi_rpc.error_kind` marker for this error class. */
  static readonly errorKind = "token_unresolved";
  /** Typed marker hoisted onto the error batch metadata. */
  readonly errorKind = "token_unresolved";
  constructor(message: string) {
    super(message);
    this.name = "TokenUnresolvedError";
  }
}

/**
 * The caller has not authenticated recently enough to mint a grant.
 *
 * Definitive but *actionable*, unlike the introspection rejections: this is
 * always about the caller themselves, so naming the reason leaks nothing and is
 * the only way a console learns to re-prompt.
 */
export class StaleAuthError extends Error {
  /** Typed `vgi_rpc.error_kind` marker for this error class. */
  static readonly errorKind = "stale_auth";
  /** Typed marker hoisted onto the error batch metadata. */
  readonly errorKind = "stale_auth";
  constructor(message: string) {
    super(message);
    this.name = "StaleAuthError";
  }
}

/**
 * The worker declined to mint this grant.
 *
 * Definitive. The worker holds the policy; the framework only asked.
 */
export class GrantRefusedError extends Error {
  /** Typed `vgi_rpc.error_kind` marker for this error class. */
  static readonly errorKind = "grant_refused";
  /** Typed marker hoisted onto the error batch metadata. */
  readonly errorKind = "grant_refused";
  constructor(message: string) {
    super(message);
    this.name = "GrantRefusedError";
  }
}

/**
 * The answer is not *knowable* -- a backing store is down, a 5xx upstream.
 *
 * Transient, and distinct from a definitive rejection: a caller that
 * negative-caches "unknown" must not cache this.
 *
 * Deliberately **not** a plain `Error` and not a `TokenUnresolvedError`.
 * `chainAuthenticate` advances to the next authenticator on a plain `Error`
 * (see `isCredentialError` in `src/http/bearer.ts`), so a sidecar outage raised
 * as one is read as "not my credential, try the next" and emerges as a 401 from
 * the end of the chain -- turning a thirty-second blip into a fleet-wide
 * re-login. Being a subclass is what makes it propagate instead.
 */
export class IdentityUnavailableError extends Error {
  /** Typed `vgi_rpc.error_kind` marker for this error class. */
  static readonly errorKind = "identity_unavailable";
  /** Typed marker hoisted onto the error batch metadata. */
  readonly errorKind = "identity_unavailable";
  /** Seconds the caller should wait. A hint to retry, not a backoff schedule. */
  readonly retryAfter: number;
  /** Operator-facing text. Must not contain the credential. */
  readonly detail: string;

  constructor(detail = "", retryAfter = 5) {
    super(detail || "identity lookup unavailable");
    this.name = "IdentityUnavailableError";
    this.detail = detail;
    this.retryAfter = retryAfter;
  }
}

// ---------------------------------------------------------------------------
// The guards
// ---------------------------------------------------------------------------

/**
 * Fixed-window request limiter, keyed by caller.
 *
 * Present because introspection is a credential-to-identity oracle even when
 * correctly restricted: an allowlisted caller whose own credential leaks can
 * still test guesses. Rate limiting does not close that, it bounds it.
 *
 * Fixed-window rather than a token bucket: a window admits at most twice the
 * rate across a boundary, which is a rounding error here, and the state is two
 * integers per caller rather than a float that has to be aged.
 *
 * `now` is milliseconds (`Date.now()` by default) so the injectable clock and
 * the ambient one agree on units.
 */
export class RateLimiter {
  private readonly counts = new Map<string, number>();
  private windowStart = 0;

  constructor(
    private readonly perWindow: number,
    private readonly windowMs = 1000,
  ) {}

  /** Return `true` if `key` may make a request in the current window. */
  allow(key: string, now: number = Date.now()): boolean {
    if (now - this.windowStart >= this.windowMs) {
      // Whole-map reset rather than per-key ageing: an attacker cycling keys
      // cannot grow the map beyond one window's worth.
      this.counts.clear();
      this.windowStart = now;
    }
    const count = this.counts.get(key) ?? 0;
    if (count >= this.perWindow) return false;
    this.counts.set(key, count + 1);
    return true;
  }

  /** Number of callers tracked in the current window. Diagnostics only. */
  get size(): number {
    return this.counts.size;
  }
}

/**
 * Validate the introspector allowlist, returning it as a set.
 *
 * Throws when the allowlist is missing or empty. There is no permissive
 * default: "any authenticated caller" is precisely the configuration that turns
 * introspection into an open oracle, so it cannot be reached by omission.
 */
export function normalisePrincipals(principals: Iterable<string> | undefined): ReadonlySet<string> {
  const allowed = new Set([...(principals ?? [])].filter((p) => p));
  if (allowed.size === 0) {
    throw new Error(
      "introspectPrincipals must name at least one principal. Introspection is a " +
        "distinct capability from authentication: allowing any authenticated caller " +
        "lets any user resolve any other user's credential to its owner.",
    );
  }
  return allowed;
}

/**
 * Return the caller principal, or refuse.
 *
 * Checked before anything touches the subject credential: an unauthorized
 * caller must not learn anything about it, including how long it took.
 */
export function checkIntrospector(auth: AuthContext, principals: ReadonlySet<string>): string {
  const caller = auth.principal ?? "";
  if (!auth.authenticated || !principals.has(caller)) {
    throw new IntrospectionRefusedError("caller is not an introspector");
  }
  return caller;
}

/** The whitespace every port MUST trim before the JWS shape test.
 *
 *  Enumerated rather than delegated to the language, because "whitespace" is
 *  itself a divergence one layer down. Measured, not assumed: this port's
 *  `String.prototype.trim` covers seven of these eight and **not** `U+0085`
 *  (NEL) -- it is neither a `LineTerminator` nor in `Space_Separator`, so the
 *  spec's `WhiteSpace` production excludes it. Java's `Character.isWhitespace`
 *  excludes it too, and an ASCII literal misses `U+0085` and `U+00A0` both.
 *
 *  Left to `trim()` alone, this port would route `"aaa.bbb.ccc\u0085"` --
 *  still a JWS to anyone who strips it -- straight to a resolver, while
 *  Python, Go, Rust and C# refuse it. That is the same hole the trim was added
 *  to close, one level down.
 *
 *  A port MAY trim more, and this one does: {@link trimForShapeTest} takes the
 *  union of this floor with whatever `trim()` calls whitespace. Trimming wider
 *  can only add refusals; trimming narrower is a leak. */
const TRIM_FLOOR = new Set([
  "\u0009", // tab
  "\u000A", // line feed
  "\u000B", // vertical tab
  "\u000C", // form feed
  "\u000D", // carriage return
  "\u0020", // space
  "\u0085", // next line -- NOT trimmed by String.prototype.trim
  "\u00A0", // no-break space
]);

/** True when one code unit is trimmable: in the enumerated floor, or whatever
 *  this runtime's `trim()` calls whitespace. The union, in one predicate, so
 *  an interleaved `"\u0085\u2028"` tail is stripped whichever order it comes
 *  in -- two sequential passes would leave the leading set's characters
 *  stranded behind the other's. */
function isTrimmable(unit: string): boolean {
  return TRIM_FLOOR.has(unit) || unit.trim() === "";
}

/** Strip trimmable code units from both ends, for the shape test only.
 *
 *  Never applied to what reaches a resolver: rewriting a credential before
 *  resolving it would make the worker answer about a string the caller never
 *  sent. */
export function trimForShapeTest(token: string): string {
  let start = 0;
  let end = token.length;
  while (start < end && isTrimmable(token[start])) start++;
  while (end > start && isTrimmable(token[end - 1])) end--;
  return token.slice(start, end);
}

/** Whether `token` is a JWS -- three dot-separated base64url segments.
 *
 *  Tested against the whitespace-**trimmed** credential, and that is the whole
 *  portability story. This port's unflagged `$` anchors at end-of-string, so
 *  `"a.b.c\n"` is not JWS-shaped to it and would be routed straight to a
 *  resolver -- the one outcome this guard exists to prevent. Python's `$`
 *  happened to match before a single trailing newline and refused it, but not
 *  before two, so it was neither strict nor consistent; seven regex dialects
 *  disagree about anchors and always will. Trimming first depends on none of
 *  them, and can only ever *add* refusals.
 *
 *  Shared with the HTTP `__introspect_token__` route so the two surfaces cannot
 *  drift into refusing different credentials. */
export function isJwsShaped(token: string): boolean {
  return JWS_SHAPED.test(trimForShapeTest(token));
}

/** Refuse a blank, over-long or JWS-shaped subject before it reaches a resolver.
 *
 *  Whitespace-only is refused because it is not a credential. The length check
 *  is against the **original**, in UTF-8 bytes: the cap is about what we were
 *  handed, not about what is left after trimming, and not about how many UTF-16
 *  code units the runtime happens to store it in.
 *
 *  Trimming is for the shape test only. The resolver still receives exactly
 *  what the caller sent -- rewriting a credential before resolving it would
 *  make the worker answer about a string the caller never sent. */
export function rejectJwsShaped(token: string): void {
  if (!trimForShapeTest(token) || utf8Length(token) > MAX_TOKEN_BYTES || isJwsShaped(token)) {
    throw new TokenUnresolvedError("unresolved");
  }
}

/** The credential's length in UTF-8 bytes -- the unit {@link MAX_TOKEN_BYTES}
 *  is measured in, and not what `String#length` reports. */
export function utf8Length(token: string): number {
  return new TextEncoder().encode(token).length;
}

/**
 * Return the caller's `auth_time`, or refuse if it is missing or stale.
 *
 * A credential with no verifiable `auth_time` cannot mint. That single rule is
 * what stops a grant being used to mint another grant: a grant is not an
 * IdP-issued token, so it carries no `auth_time`, so the lineage cannot escape
 * the identity provider. It also makes subprocess and unix transports fail
 * closed for free -- there is no authenticated principal there at all.
 *
 * A static bearer proves a machine holds a secret, never that a human just
 * authenticated, so it is refused here too.
 *
 * **Warning:** `auth_time` is an OIDC claim meaning *when this session began*,
 * which can be arbitrarily old while still present and cryptographically valid.
 * Requiring it is not the same as requiring a recent login: the deployment must
 * send `max_age` (or an appropriate `acr`) at the authorize endpoint for this
 * guard to mean what it says.
 *
 * `maxAuthAge` and `now` are seconds, matching the claim's own units.
 */
export function checkFreshness(auth: AuthContext, maxAuthAge: number, now?: number): number {
  if (!auth.authenticated || !auth.principal) {
    throw new StaleAuthError("caller is not authenticated");
  }
  const raw = auth.claims?.auth_time;
  if (raw === undefined || raw === null) {
    throw new StaleAuthError("credential carries no auth_time; only a recently authenticated user may mint a grant");
  }
  const authTime = typeof raw === "number" ? raw : Number(raw);
  if (!Number.isFinite(authTime)) {
    throw new StaleAuthError("credential carries an unusable auth_time");
  }
  const age = (now ?? Date.now() / 1000) - authTime;
  if (age > maxAuthAge) {
    throw new StaleAuthError(
      `last authentication was ${age.toFixed(0)}s ago, which exceeds the ` +
        `${maxAuthAge.toFixed(0)}s ceiling for minting a grant; re-authenticate`,
    );
  }
  return authTime;
}

// ---------------------------------------------------------------------------
// The payloads
// ---------------------------------------------------------------------------

/**
 * The identity an opaque credential authenticates as.
 *
 * **Never carries claims.** A pass-through claims field would let a worker
 * choose its caller's tenant routing, its row scope, and its policy branch, and
 * the asker derives everything it needs from the principal alone.
 */
export interface TokenIdentity {
  /** The canonical principal, in the exact form the worker itself would derive
   *  -- so an asker that normalises differently does not authorize as one
   *  identity while the worker serves another. */
  principal: string;
  /** Human-readable name for the credential, for audit trails. Never the
   *  credential. Defaults to `""` on the wire. */
  tokenName?: string;
  /** How long the answer may be cached. The caller does the caching. Treat it
   *  as an authorization window: for any path the asker serves without
   *  re-presenting the credential it is exactly that, and therefore also the
   *  revocation lag. Defaults to {@link DEFAULT_IDENTITY_TTL_SECONDS}. */
  ttlSeconds?: number;
}

/**
 * A standing delegation credential.
 *
 * OAuth cannot express durable delegation: it fuses the grant, the credential
 * and the session into one refresh token, so an IdP shortening session lifetime
 * shortens the grant. This is the durable record -- minted while the user is
 * present, presented later by unattended automation as an ordinary bearer.
 */
export interface IssuedGrant {
  /** The credential. **Opaque to the framework** -- the worker owns the format
   *  entirely (a sealed envelope, a database row, or a credential brokered from
   *  the IdP are all equally valid and equally invisible here). Never parsed,
   *  never logged. */
  token: string;
  /** Unix timestamp after which the worker will stop honouring the grant.
   *  Required *because* the framework cannot enforce it: the real lifetime
   *  lives inside the opaque token, so this is a declaration rather than an
   *  enforcement. A worker that must state a lifetime has thought about one. */
  expiresAt: number;
  /** Correlation handle for the audit trail. Not a credential and not secret --
   *  it is what ties a mint record to later use. */
  grantId?: string;
}

/**
 * Resolves an opaque credential, returning `null` when it does not resolve.
 *
 * Throw {@link IdentityUnavailableError} when the answer is not knowable -- a
 * backing store that is down is not the same as a credential that is unknown,
 * and a caller that negative-caches the second must not cache the first. The
 * HTTP `__introspect_token__` route shares this type and spells that
 * `AuthUnavailableError`; either is accepted on either surface.
 */
export type TokenResolver = (credential: string) => TokenIdentity | null | Promise<TokenIdentity | null>;

/**
 * Mints a standing grant for `principal` -- always the *calling* user.
 *
 * `ttlSeconds` is a request, not an instruction: the worker may return a
 * shorter lifetime, and the returned `expiresAt` is authoritative.
 */
export type GrantMinter = (
  principal: string,
  purpose: string,
  scopes: readonly string[],
  ttlSeconds: number,
) => IssuedGrant | Promise<IssuedGrant>;

// The payload schemas. Field *declaration order* is significant: it is part of
// the schema, and therefore of the protocol hash.
//
// Both methods return their payload as a single-row Arrow IPC stream carried in
// a `result` binary column -- the framework's ordinary convention for a
// structured return, the same one `vgi_rpc.Reflection.v1` uses.

/** @internal */
export const TOKEN_IDENTITY_SCHEMA = makeSchema([
  field("principal", utf8(), false),
  field("token_name", utf8(), false),
  field("ttl_seconds", int64(), false),
]);

/** @internal */
export const ISSUED_GRANT_SCHEMA = makeSchema([
  field("token", utf8(), false),
  field("expires_at", float64(), false),
  field("grant_id", utf8(), false),
]);

/** Encode a {@link TokenIdentity} as a single-row Arrow IPC stream. */
export function encodeTokenIdentity(identity: TokenIdentity): Uint8Array {
  return serializeBatch(
    batchFromColumns(TOKEN_IDENTITY_SCHEMA, {
      principal: [identity.principal],
      token_name: [identity.tokenName ?? ""],
      ttl_seconds: [BigInt(Math.trunc(identity.ttlSeconds ?? DEFAULT_IDENTITY_TTL_SECONDS))],
    }),
  );
}

/** Encode an {@link IssuedGrant} as a single-row Arrow IPC stream. */
export function encodeIssuedGrant(grant: IssuedGrant): Uint8Array {
  return serializeBatch(
    batchFromColumns(ISSUED_GRANT_SCHEMA, {
      token: [grant.token],
      expires_at: [grant.expiresAt],
      grant_id: [grant.grantId ?? ""],
    }),
  );
}

// ---------------------------------------------------------------------------
// The implementation
// ---------------------------------------------------------------------------

/** How a deployment configures {@link IdentityImpl}. */
export interface IdentityOptions {
  /** `(token) -> TokenIdentity | null`. `null` means the store answered and the
   *  credential is unknown; throw {@link IdentityUnavailableError} for "not
   *  knowable". */
  resolveToken?: TokenResolver;
  /** `(principal, purpose, scopes, ttlSeconds) -> IssuedGrant`. */
  mintGrant?: GrantMinter;
  /** Who may call `introspect_token`. Required whenever `resolveToken` is
   *  supplied; there is no permissive default. */
  introspectPrincipals?: Iterable<string>;
  /** Introspections allowed per caller per second. */
  introspectRateLimit?: number;
  /** How recently a caller must have authenticated to mint a grant, in
   *  seconds. */
  maxAuthAge?: number;
}

/**
 * Applies this module's guards, then delegates to worker-supplied hooks.
 *
 * The framework owns the guards and owns none of the policy. It decides who may
 * ask, how often, and what shape of credential is refused outright; the worker
 * decides what a credential resolves to and whether a grant is minted. That
 * split is deliberate -- the guards are the part that is identical in every
 * deployment and catastrophic to get wrong, and the policy is the part that is
 * different in every deployment and cannot be guessed.
 *
 * **A method whose hook is absent is not registered at all**, so the protocol a
 * server hosts describes what it actually does. A worker that resolves
 * credentials but does not mint grants hosts `introspect_token` and not
 * `issue_grant`, and a client discovers that through ordinary reflection rather
 * than by calling and reading an error. Absent beats routed-and-refusing: it is
 * what keeps a dependency upgrade from growing a credential-to-identity oracle
 * on every existing worker.
 */
export class IdentityImpl {
  private readonly resolveTokenHook?: TokenResolver;
  private readonly mintGrantHook?: GrantMinter;
  private readonly principals: ReadonlySet<string>;
  private readonly limiter: RateLimiter;
  private readonly maxAuthAge: number;

  constructor(options: IdentityOptions = {}) {
    this.resolveTokenHook = options.resolveToken;
    this.mintGrantHook = options.mintGrant;
    this.maxAuthAge = options.maxAuthAge ?? DEFAULT_MAX_AUTH_AGE_SECONDS;
    // Validated at construction, not at first call: a worker that would refuse
    // every introspection should fail to start rather than serve traffic until
    // someone tries.
    this.principals = this.resolveTokenHook ? normalisePrincipals(options.introspectPrincipals) : new Set<string>();
    this.limiter = new RateLimiter(options.introspectRateLimit ?? DEFAULT_INTROSPECT_RATE_LIMIT);
  }

  /**
   * Return the methods this deployment can actually answer.
   *
   * A method whose hook is absent is not registered, so the protocol a server
   * hosts describes what it does -- and a client learns that from reflection
   * rather than by calling and reading an error.
   */
  offeredMethods(): ReadonlySet<string> {
    const offered = new Set<string>();
    if (this.resolveTokenHook) offered.add("introspect_token");
    if (this.mintGrantHook) offered.add("issue_grant");
    return offered;
  }

  /**
   * Resolve `token`, after checking the caller may ask.
   *
   * The guard order is load-bearing and must not be tidied: authorization and
   * the rate limit come **before** anything looks at the subject credential --
   * including its length and its shape -- so an unauthorized caller learns
   * nothing about it, not even how long looking at it took.
   */
  async introspectToken(token: string, auth: AuthContext): Promise<TokenIdentity> {
    if (!this.resolveTokenHook) {
      throw new IntrospectionRefusedError("this worker does not resolve credentials");
    }

    const caller = checkIntrospector(auth, this.principals);
    if (!this.limiter.allow(caller)) {
      throw new IntrospectionRefusedError("introspection rate limit exceeded");
    }
    rejectJwsShaped(token);

    let identity: TokenIdentity | null;
    try {
      identity = await this.resolveTokenHook(token);
    } catch (err) {
      // The HTTP `__introspect_token__` route and this protocol share one
      // resolver type, and that route's "I could not find out" is
      // `AuthUnavailableError`. Translated rather than propagated as-is:
      // transient-versus-definitive reaches a caller only through
      // `error_kind`, and an untranslated error carries none -- so a resolver
      // written against the older surface would report an outage as an
      // unclassified failure, which is precisely the distinction this taxonomy
      // exists to preserve.
      if (err instanceof AuthUnavailableError) {
        throw new IdentityUnavailableError(err.detail, err.retryAfter);
      }
      throw err;
    }
    if (identity == null) {
      // Uniform with malformed and expired: reporting which would confirm that
      // a guessed credential exists.
      throw new TokenUnresolvedError("unresolved");
    }
    return identity;
  }

  /** Mint a grant for the caller, after checking they authenticated recently. */
  async issueGrant(
    purpose: string,
    scopes: readonly string[],
    ttlSeconds: number,
    auth: AuthContext,
  ): Promise<IssuedGrant> {
    if (!this.mintGrantHook) {
      throw new GrantRefusedError("this worker does not mint grants");
    }
    checkFreshness(auth, this.maxAuthAge);
    // The subject is the caller, never a parameter: cross-subject minting is
    // closed by construction rather than by a check that could be forgotten in
    // one of seven ports.
    return this.mintGrantHook(auth.principal ?? "", purpose, scopes, ttlSeconds);
  }
}

// ---------------------------------------------------------------------------
// The protocol
// ---------------------------------------------------------------------------

/** Coerce a decoded `scopes` column into a plain array of strings.
 *
 *  The list item is nullable because that is Arrow's convention for a list
 *  child and what every other port declares -- so a null can arrive. A null
 *  scope authorizes nothing, and dropping it silently would change the request
 *  the worker was asked to judge, so it arrives as `""` and the worker's policy
 *  decides what to do with it. */
function toScopes(raw: unknown): string[] {
  if (raw == null) return [];
  const items = Array.isArray(raw) ? raw : [...(raw as Iterable<unknown>)];
  return items.map((v) => (v == null ? "" : String(v)));
}

/**
 * Build `vgi_rpc.Identity.v1` for `identity`, or `null` when the deployment
 * configured neither hook.
 *
 * Only the methods whose hooks exist are registered, so the binding's method
 * set -- and therefore its `protocol_hash` -- narrows with the deployment. A
 * server offering half the methods is not offering the same surface, and saying
 * so in the hash is the point rather than a side effect.
 *
 * Both methods are unary and return their payload as serialized Arrow IPC in a
 * single `result` binary column.
 */
export function buildIdentityProtocol(identity: IdentityImpl): Protocol | null {
  const offered = identity.offeredMethods();
  if (offered.size === 0) return null;

  const p = new Protocol(IDENTITY_PROTOCOL_NAME);

  if (offered.has("introspect_token")) {
    p.unary("introspect_token", {
      params: { token: utf8() },
      result: { result: binary() },
      doc: "Resolve an opaque bearer credential to the identity it authenticates as.",
      handler: async (params, ctx) => {
        // `UnaryHandler` types the context as `LogContext`; the dispatchers
        // always pass an `OutputCollector`, which is a full `CallContext`.
        const auth = (ctx as CallContext).auth;
        return { result: encodeTokenIdentity(await identity.introspectToken(String(params.token ?? ""), auth)) };
      },
    });
  }

  if (offered.has("issue_grant")) {
    p.unary("issue_grant", {
      // `scopes` is `list<item?: utf8>` -- the ITEM IS NULLABLE, which is
      // Arrow's own convention for a list child and part of the type, and
      // therefore part of the protocol hash. The column itself is non-null.
      params: { purpose: utf8(), scopes: list(field("item", utf8(), true)), ttl_seconds: int64() },
      result: { result: binary() },
      doc: "Mint a standing delegation credential for the calling user.",
      handler: async (params, ctx) => {
        const auth = (ctx as CallContext).auth;
        // There is no subject parameter, and none may be added: the subject is
        // always the caller's authenticated principal, so cross-subject minting
        // is closed by construction.
        const grant = await identity.issueGrant(
          String(params.purpose ?? ""),
          toScopes(params.scopes),
          Number(params.ttl_seconds ?? 0),
          auth,
        );
        return { result: encodeIssuedGrant(grant) };
      },
    });
  }

  return p;
}
