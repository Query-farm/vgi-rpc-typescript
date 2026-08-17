// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Token introspection — resolving an opaque bearer credential to a principal.
 *
 * A reverse proxy that terminates the only public listener has to know *which
 * principal a credential authenticates as* before it can authorize anything:
 * that principal becomes the policy principal, the row-rule literal, and the
 * bind parameter of every entitlement query. When the credential is opaque the
 * proxy may hold no local copy of it, so it has to ask the worker.
 *
 * **The response is an identity assertion made by the thing being protected,
 * and the asker acts on it with credentials the worker does not hold** —
 * storage credentials on the data-plane host, service-credential attachments in
 * an entitlement resolver, policy-tier selection. "Trust it as much as you
 * trust the worker" is therefore the wrong frame: it must be trusted *more*,
 * because it steers privileges the worker never has. Every guard below follows
 * from that.
 *
 * What the endpoint returns is deliberately tiny: a principal, a display name
 * for the credential, and how long the answer may be cached. **It never returns
 * claims.** A pass-through claims field would let a worker choose its caller's
 * tenant routing, its row scope, and its policy branch — the single most
 * dangerous thing this feature could grow.
 *
 * It is also **not** "replay the credential through the worker's own
 * authenticate chain", which is the attractive design and breaks four ways: a
 * precondition gate wrapping the chain makes the replay unimplementable; it
 * would run the worker's independently-configured audience/issuer set, so a
 * credential the *asker* rejected could be accepted here; cookie- and
 * mTLS/IP-derived identity cannot be replayed at all, and a synthesized request
 * carries the proxy's own address, silently elevating any address-allowlist
 * member; and it invents a fake-request contract every future authenticator
 * would have to honour with no type to enforce it. The resolver is a narrow
 * callable instead.
 */

import type { AuthContext } from "../auth.js";
import { sha256Hex } from "../util/web-crypto.js";
import { AuthUnavailableError } from "./unauthorized.js";

/** Endpoint path, appended to the handler's prefix. Matches the de-facto
 *  contract the existing proxy client already speaks. */
export const INTROSPECT_ENDPOINT = "/__introspect_token__";

/** Advertised on every response (including `OPTIONS /health`) when the route is
 *  enabled, so a proxy can preflight at boot rather than discovering at first
 *  login that the worker it depends on cannot answer. */
export const INTROSPECT_ENABLED_HEADER = "VGI-Token-Introspection";

/** Three dot-separated base64url segments — a JWS. Such a credential is
 *  validated locally against a key set and MUST NOT be routed here: doing so
 *  sends a bearer token the asker may itself have rejected (expired, wrong
 *  audience) to a third party that might accept it. */
const JWS_SHAPED = /^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*$/;

/** Hard cap on the request body. The generic request-size limit would otherwise
 *  admit megabytes into a JSON parse for a body whose only legitimate content
 *  is one credential. */
const MAX_BODY_BYTES = 8192;

/** Cap on a credential we will even attempt to resolve. Anything longer is not
 *  a bearer token; refusing early keeps a resolver from being handed megabytes. */
const MAX_TOKEN_CHARS = 4096;

/** Default cache window handed to the caller when a resolver names none. */
export const DEFAULT_INTROSPECT_TTL_SECONDS = 300;

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

/** The identity an opaque credential authenticates as. */
export interface TokenIdentity {
  /** The canonical principal. Return it in the exact form the worker itself
   *  would derive, so an asker that normalises differently does not authorize
   *  as one identity while the worker serves another. */
  principal: string;
  /** Human-readable name for the credential, for audit trails. Never the
   *  credential. */
  tokenName?: string;
  /** How long the answer may be cached. The caller does the caching; this
   *  endpoint holds none of its own. Treat it as an authorization window,
   *  because for any path the asker serves without re-presenting the credential
   *  it is exactly that. */
  ttlSeconds?: number;
}

/**
 * Resolves an opaque credential, returning `null` when it does not resolve.
 *
 * Throw {@link AuthUnavailableError} when the answer is not knowable — a
 * backing store that is down is not the same as a credential that is unknown,
 * and a caller that negative-caches the second must not cache the first.
 */
export type TokenResolver = (credential: string) => TokenIdentity | null | Promise<TokenIdentity | null>;

/**
 * Fixed-window request limiter, keyed by caller.
 *
 * Present because the endpoint is a credential→identity oracle even when
 * correctly restricted: an allowlisted caller whose own credential leaks can
 * still test guesses. Rate limiting does not close that, it bounds it — a lower
 * ceiling on how fast an attacker converts guesses to answers.
 *
 * Fixed-window rather than a token bucket: a window admits at most twice the
 * rate across a boundary, which is a rounding error here, and the state is one
 * integer per caller rather than a float that has to be aged.
 */
class RateLimiter {
  private readonly counts = new Map<string, number>();
  private windowStart = 0;

  constructor(
    private readonly perWindow: number,
    private readonly windowMs = 1000,
  ) {}

  allow(key: string, now: number = Date.now()): boolean {
    if (now - this.windowStart >= this.windowMs) {
      // Whole-map reset rather than per-key ageing: a caller cycling keys
      // cannot grow the map beyond one window's worth.
      this.counts.clear();
      this.windowStart = now;
    }
    const count = this.counts.get(key) ?? 0;
    if (count >= this.perWindow) return false;
    this.counts.set(key, count + 1);
    return true;
  }
}

/** The endpoint, bound to one validated configuration. */
export interface Introspector {
  /** Answer one `POST {prefix}/__introspect_token__`, for the caller `auth`
   *  already resolved for this request. */
  handle(request: Request, auth: AuthContext | undefined): Promise<Response>;
}

/**
 * Validate the introspection options and bind the endpoint to them.
 *
 * Called at handler construction so a misconfiguration fails there rather than
 * at the first proxy preflight. Throws when the allowlist is missing or empty:
 * there is no permissive default, because "any authenticated caller" is
 * precisely the configuration that turns this endpoint into an open oracle, so
 * it must not be reachable by omission.
 */
export function createIntrospector(options: {
  resolver: TokenResolver;
  principals: Iterable<string> | undefined;
  ttlSeconds?: number;
  rateLimit?: number;
}): Introspector {
  const principals = new Set([...(options.principals ?? [])].filter((p) => p));
  if (principals.size === 0) {
    throw new Error(
      "introspectPrincipals must name at least one principal. Introspection is a " +
        "distinct capability from authentication: allowing any authenticated caller " +
        "lets any user resolve any other user's credential to its owner.",
    );
  }
  const ttlSeconds = options.ttlSeconds ?? DEFAULT_INTROSPECT_TTL_SECONDS;
  if (!Number.isFinite(ttlSeconds) || ttlSeconds <= 0) {
    // A non-finite or non-positive TTL silently disables the caller's cache and
    // turns every request it serves into a round trip.
    throw new Error("introspectTtlSeconds must be a finite, positive number of seconds");
  }
  const limiter = new RateLimiter(options.rateLimit ?? 20);
  return {
    handle: (request, auth) => introspect(request, auth, options.resolver, principals, ttlSeconds, limiter),
  };
}

/** Write a rejection carrying no detail about why. */
function refuse(status: number, error: string, extra?: Record<string, string>): Response {
  const headers = new Headers({
    "Content-Type": "application/json",
    // A credential's resolution can change; nothing here may sit in a shared cache.
    "Cache-Control": "no-store",
    ...extra,
  });
  return new Response(JSON.stringify({ error }), { status, headers });
}

/**
 * `POST {prefix}/__introspect_token__` when introspection is **off**.
 *
 * The oracle is still absent in every sense that matters: no resolver is held,
 * nothing is looked up, and the answer does not depend on the request. What
 * this adds is a *definitive* answer for a caller that asks anyway.
 *
 * Without it the path falls through to the generic dispatch route, which
 * rejects a JSON body with `415`. A caller that classifies `401/403/404` as
 * definitive and everything else as transient — which is the sensible
 * classification, and the one the existing proxy client uses — reads `415` as
 * "try again later" and retries forever against a worker that will never
 * support the feature. A misconfiguration should stop, not spin.
 *
 * Deliberately no authentication requirement of its own: "this worker does not
 * do introspection" is not a secret, and a caller needs to learn it at
 * preflight rather than after arranging credentials.
 */
export function introspectionDisabledResponse(): Response {
  return refuse(404, "not_enabled");
}

/**
 * Extract the subject credential, or `null` when the body is unusable.
 *
 * Uniform `null` rather than a per-failure signal: a malformed body is not
 * worth its own answer, and giving one lets a caller probe the parser.
 */
async function readSubjectToken(request: Request): Promise<string | null> {
  const declared = request.headers.get("Content-Length");
  if (declared && Number(declared) > MAX_BODY_BYTES) return null;
  const raw = new Uint8Array(await request.arrayBuffer());
  if (raw.byteLength > MAX_BODY_BYTES) return null;
  let body: unknown;
  try {
    body = JSON.parse(new TextDecoder().decode(raw));
  } catch {
    return null;
  }
  if (typeof body !== "object" || body === null || Array.isArray(body)) return null;
  const token = (body as Record<string, unknown>).token;
  if (typeof token !== "string" || !token || token.length > MAX_TOKEN_CHARS) return null;
  return token;
}

/**
 * `POST {prefix}/__introspect_token__` — credential to principal.
 *
 * Reached only when a resolver was supplied, so a worker cannot grow this
 * oracle by upgrading a dependency.
 *
 * Two rejection axes, deliberately distinguishable from each other and
 * deliberately uniform within themselves:
 *
 * - **403** — the caller may not introspect. Authentication is not the same
 *   capability as introspection: a deployment where any valid credential can
 *   introspect lets any user test guesses of any other user's credential at
 *   unlimited rate, and resolve a stolen one to its owner.
 * - **404** — the *subject* credential did not resolve. Unknown, expired and
 *   malformed are one answer, because reporting which would confirm that a
 *   guessed credential exists.
 *
 * Both are definitive: a caller may cache them. Anything transient must reach
 * the caller as 5xx so it is retried rather than cached.
 */
async function introspect(
  request: Request,
  auth: AuthContext | undefined,
  resolver: TokenResolver,
  principals: ReadonlySet<string>,
  defaultTtlSeconds: number,
  limiter: RateLimiter,
): Promise<Response> {
  const caller = auth?.principal ?? "";

  // Caller authorization first: an unauthorized caller must not learn anything
  // about a subject credential.
  if (!auth?.authenticated || !principals.has(caller)) {
    console.warn("[introspect] refused: caller is not an introspector", { principal: caller });
    return refuse(403, "not_an_introspector");
  }

  if (!limiter.allow(caller)) {
    console.warn("[introspect] rate limit exceeded", { principal: caller });
    return refuse(429, "rate_limited", { "Retry-After": "1" });
  }

  const token = await readSubjectToken(request);
  if (token === null) {
    return refuse(404, "unresolved");
  }

  const digest = await tokenDigest(token);

  if (JWS_SHAPED.test(token)) {
    // Refused without ever reaching the resolver. A JWS is validated locally
    // against a key set; one arriving here is either a caller bug or an attempt
    // to have this worker vouch for a token its asker already rejected.
    console.warn("[introspect] refused: JWS-shaped subject", { principal: caller, tokenDigest: digest });
    return refuse(404, "unresolved");
  }

  let identity: TokenIdentity | null;
  try {
    identity = await resolver(token);
  } catch (err) {
    if (!(err instanceof AuthUnavailableError)) {
      throw err;
    }
    // "I could not find out" is not "it did not resolve". Refusing with 404 here
    // would hand the caller a *definitive* answer for an outage, and a caller
    // that negative-caches definitive answers — which is the correct thing to do
    // — would remember an unreachable backing store as a bad credential for the
    // cache's lifetime. Deliberately not `refuse`: that shape is for definitive
    // rejections and carries `Cache-Control: no-store`; a transient needs
    // `Retry-After`.
    console.warn("[introspect] unavailable", {
      principal: caller,
      tokenDigest: digest,
      error: err.message,
    });
    return new Response(JSON.stringify({ error: "unavailable" }), {
      status: 503,
      headers: new Headers({
        "Content-Type": "application/json",
        "Retry-After": String(err.retryAfter),
      }),
    });
  }
  if (identity == null) {
    console.info("[introspect] credential did not resolve", { principal: caller, tokenDigest: digest });
    return refuse(404, "unresolved");
  }

  console.info("[introspect] resolved", {
    principal: caller,
    tokenDigest: digest,
    resolvedPrincipal: identity.principal,
  });
  // A closed set of three keys. Anything more — a claims passthrough above all
  // — would let this worker steer its caller's tenant routing and policy branch.
  const body = JSON.stringify({
    principal: identity.principal,
    token_name: identity.tokenName ?? "",
    ttl_seconds: identity.ttlSeconds ?? defaultTtlSeconds,
  });
  return new Response(body, {
    status: 200,
    headers: new Headers({ "Content-Type": "application/json", "Cache-Control": "no-store" }),
  });
}
