// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * The `vgi_rpc.Identity.v1` conformance policy of the reference's
 * `IDENTITY_CONFORMANCE_FIXTURE.md`, shared by this port's conformance worker
 * and by any SDK fixture worker that opts into Identity.
 *
 * Every constant here is part of the fixture's contract, pinned identically in
 * every port, rather than a choice a port gets to make.
 */

// ---------------------------------------------------------------------------
// !!! THIS WORKER'S AUTHENTICATION IS A TEST FIXTURE AND MUST NEVER BE DEPLOYED
// ---------------------------------------------------------------------------
//
// `introspect_token` requires an authenticated caller on an allowlist, and
// `issue_grant` requires an `auth_time` claim — which in a real deployment
// means a JWT from an identity provider. Six language ports cannot each stand
// up an IdP and a baked JWT would expire, so the caller simply *names itself*
// in two request headers. That is trivially spoofable by anyone who can reach
// the port. It exercises exactly what Identity's guards read — `authenticated`,
// `principal`, `claims.auth_time` — and nothing about JWT validation, which
// happens in the authenticator before Identity sees anything and has its own
// tests.

import { AuthContext } from "../auth.js";
import { GrantKeys } from "../grants.js";
import type { AuthenticateFn } from "../http/auth.js";
import { AuthUnavailableError } from "../http/unauthorized.js";
import { Protocol } from "../protocol.js";
import { str } from "../schema.js";
import {
  GrantRefusedError,
  IdentityImpl,
  IdentityUnavailableError,
  type IssuedGrant,
  type TokenIdentity,
} from "../token-identity.js";
import type { CallContext } from "../types.js";

// ---------------------------------------------------------------------------
// The two headers that stand in for an identity provider
// ---------------------------------------------------------------------------

/** Names the authenticated principal. **Absent means unauthenticated** — not
 *  "anonymous but authenticated". Health checks and capability probes have to
 *  keep working without it, and the group relies on the absence to reach the
 *  fail-closed paths. Already the convention for the sticky fixture; reused
 *  rather than invented. */
export const PRINCIPAL_HEADER = "X-Conformance-Principal";

/** Carries the `auth_time` claim, placed in the claim map **verbatim as a
 *  string and unparsed**.
 *
 *  Verbatim is load-bearing. A fixture that parses the header and drops it
 *  when parsing fails collapses "the credential carries an unusable auth_time"
 *  into "the credential carries no auth_time at all". Both answer
 *  `stale_auth`, so the test stays green while the property it names goes
 *  untested. The *guard* parses; the fixture only transports. */
export const AUTH_TIME_HEADER = "X-Conformance-Auth-Time";

/**
 * Derive the caller's identity from the two fixture headers, and nothing else.
 *
 * Nothing else goes in the claim map: the group asserts against what the
 * guards read, and a claim nobody pinned is a claim that could differ between
 * ports without anything noticing.
 */
export const conformanceAuthenticate: AuthenticateFn = (request: Request) => {
  const principal = request.headers.get(PRINCIPAL_HEADER);
  if (!principal) {
    // A bearer and no principal header: not ours. A plain Error so the chain
    // moves on to the identity bearer authenticators the handler appends
    // (sealed grants, resolveToken) -- IDENTITY_CONFORMANCE_FIXTURE.md §10.
    if (request.headers.get("Authorization")) throw new Error("no conformance principal header");
    return AuthContext.anonymous();
  }
  const authTime = request.headers.get(AUTH_TIME_HEADER);
  return new AuthContext("conformance", true, principal, authTime === null ? {} : { auth_time: authTime });
};

// ---------------------------------------------------------------------------
// Deployment policy (§3.1)
// ---------------------------------------------------------------------------

/** The single principal on the introspector allowlist. One, so that "on the
 *  list" and "authenticated but not on the list" are both reachable. */
export const INTROSPECTOR_PRINCIPAL = "conformance-introspector";

/** The documented default, restated so the fixture does not inherit a change
 *  to it silently. */
export const MAX_AUTH_AGE = 900.0;

// There is no introspection rate limit to configure: introspection is not rate
// limited (IDENTITY_V1_SPEC §4, "No rate limiter"), and the shared group's
// `TestIntrospectionIsNotThrottled` asserts it with a concurrent burst of 60.
// This fixture used to raise a limiter to 100,000 to keep it out of the group's
// way; it went with the limiter.

// ---------------------------------------------------------------------------
// What the resolver answers (§3.3)
// ---------------------------------------------------------------------------

/** The identity every resolvable credential maps to. */
export const SUBJECT_PRINCIPAL = "subject@conformance.example";
export const SUBJECT_TOKEN_NAME = "conformance-subject";
export const SUBJECT_TTL = 300;

/** The one credential this resolver calls **unknown**. */
export const TOKEN_UNKNOWN = "conformance-unknown-token";

/** The credential whose answer is *unknowable* rather than unknown. A caller
 *  may negative-cache the second; caching the first locks out a valid user for
 *  as long as the cache holds, so they must not share an answer. */
export const TOKEN_UNAVAILABLE = "conformance-unavailable-token";

/** Retry hint {@link TOKEN_UNAVAILABLE} carries. `identity_unavailable` MUST
 *  carry `RetryInfo`, so the value is pinned rather than each port's default. */
export const UNAVAILABLE_RETRY_AFTER = 5;

/** The credential the resolver answers by raising the **transport-auth**
 *  "could not find out" error -- `AuthUnavailableError` -- rather than the
 *  identity one. The framework must translate it to `identity_unavailable` and
 *  keep its retry hint. The translation belongs in the framework, never here:
 *  a fixture raising the identity error directly would pass with the rule
 *  unimplemented. */
export const TOKEN_AUTH_UNAVAILABLE = "conformance-auth-unavailable-token";

/** Retry hint the transport-auth error carries. Deliberately not any port's
 *  default, so a translation that substitutes its own hint is caught. */
export const AUTH_UNAVAILABLE_RETRY_AFTER = 7;

/** A resolver naming a TTL of zero is saying *do not cache this*. The tempting
 *  normalisation of `<= 0` up to the 300 default converts that into five
 *  minutes of continued access after revocation, silently. */
export const TOKEN_ZERO_TTL = "conformance-zero-ttl-token";

/** Resolves to an identity built with **only** the principal supplied, so the
 *  other two fields land on their documented defaults (`""` and 300). Passing
 *  those values explicitly would test the wrong thing: a default is for an
 *  omitted field, never a coercion applied to a value a hook actually set. */
export const TOKEN_MINIMAL = "conformance-minimal-token";

/** Two leading and two trailing ASCII spaces (U+0020), resolving to a
 *  *distinguishable* `token_name`.
 *
 *  The shape test runs on the trimmed credential while the resolver receives
 *  the untrimmed original. A port that trims once, up front, and resolves the
 *  result passes every other case in the group — the padded credential still
 *  resolves, just via the catch-all rule — so this is the only thing that can
 *  see it. */
export const TOKEN_PADDED_PROBE = "  conformance-padded-probe  ";
export const TOKEN_PADDED_PROBE_NAME = "conformance-padded";

/**
 * Resolve a credential under the fixed conformance policy.
 *
 * **This resolver resolves almost everything**, and that is the single most
 * important thing about it. Rejections are deliberately uniform — unknown,
 * expired, malformed and over-long are one answer — so an over-long credential
 * is *also* an unknown one, and a test that probes the cap with a credential
 * the resolver does not know cannot distinguish "the cap refused it" from "the
 * cap let it through and the resolver refused it": delete the cap and the test
 * stays green. With a resolver that answers for whatever it is handed, a
 * rejection can only have come from a guard — and a guard that fails to fire
 * produces a *success*, which uniformity cannot disguise.
 *
 * A pure function of its argument: no clock, no counter, no shared state, so
 * the worker answers identically on the first call and the thousandth.
 */
export function conformanceResolveToken(token: string): TokenIdentity | null {
  if (token === TOKEN_UNAVAILABLE) {
    throw new IdentityUnavailableError("conformance: mapping store unreachable", UNAVAILABLE_RETRY_AFTER);
  }
  if (token === TOKEN_AUTH_UNAVAILABLE) {
    throw new AuthUnavailableError("conformance: authority unreachable", AUTH_UNAVAILABLE_RETRY_AFTER);
  }
  if (token === TOKEN_UNKNOWN) return null;
  if (token === TOKEN_ZERO_TTL) {
    return { principal: SUBJECT_PRINCIPAL, tokenName: SUBJECT_TOKEN_NAME, ttlSeconds: 0 };
  }
  // Principal only — `tokenName` and `ttlSeconds` must land on their defaults,
  // not on values this fixture supplied.
  if (token === TOKEN_MINIMAL) return { principal: SUBJECT_PRINCIPAL };
  if (token === TOKEN_PADDED_PROBE) {
    return { principal: SUBJECT_PRINCIPAL, tokenName: TOKEN_PADDED_PROBE_NAME, ttlSeconds: SUBJECT_TTL };
  }
  return { principal: SUBJECT_PRINCIPAL, tokenName: SUBJECT_TOKEN_NAME, ttlSeconds: SUBJECT_TTL };
}

// ---------------------------------------------------------------------------
// What the minter answers (§3.4)
// ---------------------------------------------------------------------------

/** Prefix of a minted grant's token. The caller's principal is appended, which
 *  is how "the subject is the caller, never a parameter" becomes observable:
 *  two callers making identical requests get two different tokens. */
export const GRANT_TOKEN_PREFIX = "conformance-grant-for:";

/** Separates the principal from the echoed scopes, so the scope list's round
 *  trip is visible in the response. An empty scope list yields a token ending
 *  in the separator. */
export const SCOPE_SEPARATOR = "|";

/** Fixed rather than `now + ttl`: a constant can be asserted exactly, which
 *  also pins the float64 round trip. `expires_at` is a declaration rather than
 *  an enforcement — the real lifetime lives inside the opaque token — so
 *  nothing is lost. 2030-01-01T00:00:00Z. */
export const GRANT_EXPIRES_AT = 1893456000.0;

/** Correlation handle a full grant carries. */
export const GRANT_ID = "conformance-grant-id";

/** The purpose this policy refuses, so `grant_refused` reaches the wire. */
export const REFUSED_PURPOSE = "conformance-refused";

/** The purpose that mints a grant built without `grant_id`, so that field's
 *  documented default (`""`) is observable. Omitted, not passed as `""`. */
export const MINIMAL_PURPOSE = "conformance-minimal";

/** The purpose the minter answers by raising the transport-auth unavailable
 *  error, so the translation rule is observable on `issue_grant` too. */
export const AUTH_UNAVAILABLE_PURPOSE = "conformance-auth-unavailable";

/**
 * Mint a grant under the fixed conformance policy.
 *
 * `ttlSeconds` is deliberately ignored. It is a *request*, the returned
 * `expiresAt` is authoritative, and a fixture that honoured it would need a
 * clock and become unassertable.
 *
 * `principal` is whatever the framework passed — never a parameter of the
 * call. Echoing it into the token is what makes that observable from outside.
 */
export function conformanceMintGrant(principal: string, purpose: string, scopes: readonly string[]): IssuedGrant {
  if (purpose === AUTH_UNAVAILABLE_PURPOSE) {
    throw new AuthUnavailableError("conformance: grant store unreachable", AUTH_UNAVAILABLE_RETRY_AFTER);
  }
  if (purpose === REFUSED_PURPOSE) {
    throw new GrantRefusedError("conformance: this purpose is refused");
  }
  const token = GRANT_TOKEN_PREFIX + principal + SCOPE_SEPARATOR + scopes.join(",");
  // `grantId` omitted rather than empty, so the wire shows the default.
  if (purpose === MINIMAL_PURPOSE) return { token, expiresAt: GRANT_EXPIRES_AT };
  return { token, expiresAt: GRANT_EXPIRES_AT, grantId: GRANT_ID };
}

/** Which hooks a fixture worker configures. */
export type ConformanceIdentityMode = "both" | "introspect-only";

/** The fixture's `IdentityImpl`: the resolver always, the minter for `both`,
 *  the pinned allowlist and maximum auth age. */
export function conformanceIdentity(mode: ConformanceIdentityMode = "both"): IdentityImpl {
  return new IdentityImpl({
    resolveToken: conformanceResolveToken,
    // The narrowing fixture. Leaving the hook out must drop one *method*,
    // not the protocol, and must shrink the protocol_hash with it.
    ...(mode === "both" ? { mintGrant: conformanceMintGrant } : {}),
    introspectPrincipals: [INTROSPECTOR_PRINCIPAL],
    maxAuthAge: MAX_AUTH_AGE,
  });
}

// ---------------------------------------------------------------------------
// Sealed grants and bearer acceptance (IDENTITY_CONFORMANCE_FIXTURE.md §10)
// ---------------------------------------------------------------------------

/** The grant worker's minting key: bytes 0x10..0x2f. Published on purpose --
 *  the shared suite mints with it to test this port's verifier and decodes
 *  this port's grants to test its minter. A fixture key, never a deployment one. */
export const GRANT_KEY_CURRENT = Uint8Array.from({ length: 32 }, (_, i) => 0x10 + i);
/** The previous key, still configured to verify (rotation): bytes 0x30..0x4f. */
export const GRANT_KEY_PREVIOUS = Uint8Array.from({ length: 32 }, (_, i) => 0x30 + i);
/** Audience bound into the grant worker's tokens. */
export const GRANT_AUDIENCE = "conformance";
/** The grant worker's lifetime ceiling, in seconds. */
export const GRANT_MAX_TTL = 3600;

/** The grant worker's configuration: current key mints, both verify. */
export function conformanceGrantKeys(): GrantKeys {
  return new GrantKeys([GRANT_KEY_CURRENT, GRANT_KEY_PREVIOUS], {
    audience: GRANT_AUDIENCE,
    maxTtlSeconds: GRANT_MAX_TTL,
  });
}

/** The grant worker's identity: the fixture resolver, the allowlist and auth
 *  age of §3, the grant keys, and **no** mint hook -- the framework mints. */
export function conformanceGrantIdentity(): IdentityImpl {
  return new IdentityImpl({
    resolveToken: conformanceResolveToken,
    introspectPrincipals: [INTROSPECTOR_PRINCIPAL],
    maxAuthAge: MAX_AUTH_AGE,
    grantKeys: conformanceGrantKeys(),
  });
}

/** Routing key of the probe that reports how a request was authenticated. */
export const WHOAMI_PROTOCOL_NAME = "conformance.Whoami.v1";
/** Pinned digest of `conformance.Whoami.v1`. */
export const WHOAMI_PROTOCOL_HASH = "a280333ba72432020e162cab388a78355969a30aa74f0665ad9d2932d7a10b8f";

/** JSON with object keys sorted at every level, compact -- Python's
 *  `json.dumps(sort_keys=True, separators=(",", ":"))`. */
function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value !== null && typeof value === "object") {
    const obj = value as Record<string, unknown>;
    return `{${Object.keys(obj)
      .sort()
      .filter((k) => obj[k] !== undefined)
      .map((k) => `${JSON.stringify(k)}:${canonicalJson(obj[k])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}

/** Build `conformance.Whoami.v1`: `whoami() -> utf8`, the caller's
 *  `AuthContext` as `{"authenticated","claims","domain","principal"}` with
 *  sorted keys (`""` for an absent domain or principal). Hosted only by the
 *  grant worker. */
export function buildWhoamiProtocol(): Protocol {
  return new Protocol(WHOAMI_PROTOCOL_NAME).unary("whoami", {
    params: {},
    result: { result: str },
    doc: "Report how this request was authenticated.",
    handler: (_params, ctx) => {
      const auth = (ctx as CallContext).auth;
      return {
        result: canonicalJson({
          authenticated: auth.authenticated,
          claims: { ...(auth.claims ?? {}) },
          domain: auth.domain ?? "",
          principal: auth.principal ?? "",
        }),
      };
    },
  });
}
