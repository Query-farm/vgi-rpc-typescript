// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Conformance HTTP server hosting `vgi_rpc.Identity.v1` under the pinned
 * deployment policy of `IDENTITY_CONFORMANCE_FIXTURE.md`.
 *
 * Identity is almost entirely *guards*, and every guard reads deployment
 * policy: who may introspect, what a credential resolves to, whether a grant
 * is minted, how recently the caller authenticated. Against a worker whose
 * allowlist and hooks are unknown no cross-port assertion exists — every
 * answer is explicable as policy. So the policy is pinned, identically in six
 * ports, and every constant below is part of the fixture's contract rather
 * than a choice this port gets to make.
 *
 * One binary, two fixtures, selected by `--identity`:
 *
 * | flag | hooks | runner fixture |
 * |---|---|---|
 * | `both` | resolve **and** mint | `conformance_http_identity_port` |
 * | `introspect-only` | resolve only | `conformance_http_identity_introspect_only_port` |
 * | `off` | neither | (unused by the group — see below) |
 *
 * `off` is here for the mutation checks §8 asks for and for symmetry with the
 * reference's flag, not because the group drives it. "A worker configuring no
 * hook hosts no identity protocol at all" is asserted against the *plain*
 * conformance worker (`examples/conformance-http.ts`), which must stay
 * identity-free: a fixture that opted in would make the property untestable
 * by making every worker in the suite an opt-in one.
 *
 * Run: `bun run examples/conformance-http-identity.ts --identity both`
 *
 * @packageDocumentation
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

import { AuthContext } from "../src/auth.js";
import type { AuthenticateFn } from "../src/http/auth.js";
import { createHttpHandler } from "../src/http/index.js";
import { VgiRpcServer } from "../src/server.js";
import {
  GrantRefusedError,
  IdentityImpl,
  IdentityUnavailableError,
  type IssuedGrant,
  type TokenIdentity,
} from "../src/token-identity.js";
import { protocol } from "./conformance-protocol.js";

// ---------------------------------------------------------------------------
// The two headers that stand in for an identity provider
// ---------------------------------------------------------------------------

/** Names the authenticated principal. **Absent means unauthenticated** — not
 *  "anonymous but authenticated". Health checks and capability probes have to
 *  keep working without it, and the group relies on the absence to reach the
 *  fail-closed paths. Already the convention for the sticky fixture; reused
 *  rather than invented. */
const PRINCIPAL_HEADER = "X-Conformance-Principal";

/** Carries the `auth_time` claim, placed in the claim map **verbatim as a
 *  string and unparsed**.
 *
 *  Verbatim is load-bearing. A fixture that parses the header and drops it
 *  when parsing fails collapses "the credential carries an unusable auth_time"
 *  into "the credential carries no auth_time at all". Both answer
 *  `stale_auth`, so the test stays green while the property it names goes
 *  untested. The *guard* parses; the fixture only transports. */
const AUTH_TIME_HEADER = "X-Conformance-Auth-Time";

/**
 * Derive the caller's identity from the two fixture headers, and nothing else.
 *
 * Nothing else goes in the claim map: the group asserts against what the
 * guards read, and a claim nobody pinned is a claim that could differ between
 * ports without anything noticing.
 */
const conformanceAuthenticate: AuthenticateFn = (request: Request) => {
  const principal = request.headers.get(PRINCIPAL_HEADER);
  if (!principal) return AuthContext.anonymous();
  const authTime = request.headers.get(AUTH_TIME_HEADER);
  return new AuthContext("conformance", true, principal, authTime === null ? {} : { auth_time: authTime });
};

// ---------------------------------------------------------------------------
// Deployment policy (§3.1)
// ---------------------------------------------------------------------------

/** The single principal on the introspector allowlist. One, so that "on the
 *  list" and "authenticated but not on the list" are both reachable. */
const INTROSPECTOR_PRINCIPAL = "conformance-introspector";

/** The documented default, restated so the fixture does not inherit a change
 *  to it silently. */
const MAX_AUTH_AGE = 900.0;

// There is no introspection rate limit to configure: introspection is not rate
// limited (IDENTITY_V1_SPEC §4, "No rate limiter"), and the shared group's
// `TestIntrospectionIsNotThrottled` asserts it with a concurrent burst of 60.
// This fixture used to raise a limiter to 100,000 to keep it out of the group's
// way; it went with the limiter.

// ---------------------------------------------------------------------------
// What the resolver answers (§3.3)
// ---------------------------------------------------------------------------

/** The identity every resolvable credential maps to. */
const SUBJECT_PRINCIPAL = "subject@conformance.example";
const SUBJECT_TOKEN_NAME = "conformance-subject";
const SUBJECT_TTL = 300;

/** The one credential this resolver calls **unknown**. */
const TOKEN_UNKNOWN = "conformance-unknown-token";

/** The credential whose answer is *unknowable* rather than unknown. A caller
 *  may negative-cache the second; caching the first locks out a valid user for
 *  as long as the cache holds, so they must not share an answer. */
const TOKEN_UNAVAILABLE = "conformance-unavailable-token";

/** A resolver naming a TTL of zero is saying *do not cache this*. The tempting
 *  normalisation of `<= 0` up to the 300 default converts that into five
 *  minutes of continued access after revocation, silently. */
const TOKEN_ZERO_TTL = "conformance-zero-ttl-token";

/** Resolves to an identity built with **only** the principal supplied, so the
 *  other two fields land on their documented defaults (`""` and 300). Passing
 *  those values explicitly would test the wrong thing: a default is for an
 *  omitted field, never a coercion applied to a value a hook actually set. */
const TOKEN_MINIMAL = "conformance-minimal-token";

/** Two leading and two trailing ASCII spaces (U+0020), resolving to a
 *  *distinguishable* `token_name`.
 *
 *  The shape test runs on the trimmed credential while the resolver receives
 *  the untrimmed original. A port that trims once, up front, and resolves the
 *  result passes every other case in the group — the padded credential still
 *  resolves, just via the catch-all rule — so this is the only thing that can
 *  see it. */
const TOKEN_PADDED_PROBE = "  conformance-padded-probe  ";
const TOKEN_PADDED_PROBE_NAME = "conformance-padded";

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
function conformanceResolveToken(token: string): TokenIdentity | null {
  if (token === TOKEN_UNAVAILABLE) {
    throw new IdentityUnavailableError("conformance: mapping store unreachable");
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
const GRANT_TOKEN_PREFIX = "conformance-grant-for:";

/** Separates the principal from the echoed scopes, so the scope list's round
 *  trip is visible in the response. An empty scope list yields a token ending
 *  in the separator. */
const SCOPE_SEPARATOR = "|";

/** Fixed rather than `now + ttl`: a constant can be asserted exactly, which
 *  also pins the float64 round trip. `expires_at` is a declaration rather than
 *  an enforcement — the real lifetime lives inside the opaque token — so
 *  nothing is lost. 2030-01-01T00:00:00Z. */
const GRANT_EXPIRES_AT = 1893456000.0;

/** Correlation handle a full grant carries. */
const GRANT_ID = "conformance-grant-id";

/** The purpose this policy refuses, so `grant_refused` reaches the wire. */
const REFUSED_PURPOSE = "conformance-refused";

/** The purpose that mints a grant built without `grant_id`, so that field's
 *  documented default (`""`) is observable. Omitted, not passed as `""`. */
const MINIMAL_PURPOSE = "conformance-minimal";

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
function conformanceMintGrant(principal: string, purpose: string, scopes: readonly string[]): IssuedGrant {
  if (purpose === REFUSED_PURPOSE) {
    throw new GrantRefusedError("conformance: this purpose is refused");
  }
  const token = GRANT_TOKEN_PREFIX + principal + SCOPE_SEPARATOR + scopes.join(",");
  // `grantId` omitted rather than empty, so the wire shows the default.
  if (purpose === MINIMAL_PURPOSE) return { token, expiresAt: GRANT_EXPIRES_AT };
  return { token, expiresAt: GRANT_EXPIRES_AT, grantId: GRANT_ID };
}

// ---------------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------------

const args = process.argv.slice(2);
const modeArg = args.indexOf("--identity");
const mode = modeArg >= 0 ? (args[modeArg + 1] ?? "") : "both";
if (mode !== "both" && mode !== "introspect-only" && mode !== "off") {
  throw new Error(`--identity must be one of both, introspect-only, off (got ${JSON.stringify(mode)})`);
}

// A `VgiRpcServer` rather than a bare `Protocol`: identity is a *secondary*
// protocol, and only a host can carry one. The constructor registers
// reflection, so identity lands after it and appears in its own server's
// listing — which is how a client discovers it instead of calling to find out.
const server = new VgiRpcServer(protocol, { serverId: `conformance-http-identity-${mode}` });

if (mode !== "off") {
  server.registerIdentity(
    new IdentityImpl({
      resolveToken: conformanceResolveToken,
      // The narrowing fixture. Leaving the hook out must drop one *method*,
      // not the protocol, and must shrink the protocol_hash with it.
      ...(mode === "both" ? { mintGrant: conformanceMintGrant } : {}),
      introspectPrincipals: [INTROSPECTOR_PRINCIPAL],
      maxAuthAge: MAX_AUTH_AGE,
    }),
  );
}

const handler = createHttpHandler(server, {
  serverId: `conformance-http-identity-${mode}`,
  protocolName: "ConformanceService",
  authenticate: conformanceAuthenticate,
});

const listener = Bun.serve({ port: 0, fetch: handler });
console.log(`PORT:${listener.port}`);
