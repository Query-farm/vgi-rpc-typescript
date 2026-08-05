// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * The standardized shape of an HTTP 401, per `docs/unauthorized-spec.md` in
 * the vgi-rpc reference repository.
 *
 * A rejection carries a machine-readable reason code so a client can switch on
 * it — refresh on `expired_credential`, give up on `insufficient_scope` —
 * instead of matching on message text, which misclassifies the moment someone
 * rewords a string.
 *
 * The code is deliberately coarse: it names the *stage* that refused the
 * request, never the verifier's internal diagnosis. Whether a signature was
 * malformed or simply wrong is an operator's business, not a caller's, and
 * echoing that back is how a rejection turns into an oracle.
 *
 * The proxy note is derived from **server configuration**, not from what
 * failed on this request, so every 401 a proxy-dependent service emits says
 * the same thing — which is exactly what makes it safe and what makes it right
 * in the case operators actually hit, where the proxy is simply not forwarding
 * the header and the credential was never the problem.
 */

/** Header carrying one {@link AuthReason} on every 401. */
export const AUTH_REASON_HEADER = "VGI-Auth-Reason";

/** Header set to `"true"` on 401s from a service whose authentication depends
 *  on a reverse proxy. Omitted otherwise — never `"false"`. */
export const AUTH_PROXY_REQUIRED_HEADER = "VGI-Auth-Proxy-Required";

/**
 * The closed set of reason codes a 401 may carry.
 *
 * Readers must treat an unrecognised code as {@link AuthReason.Unauthorized} —
 * that means the server is newer, not broken.
 */
export const AuthReason = {
  /** No credential was presented at all. */
  MissingCredential: "missing_credential",
  /** A credential was presented and rejected. */
  InvalidCredential: "invalid_credential",
  /** Well-formed, but outside its validity window. */
  ExpiredCredential: "expired_credential",
  /** Identified, but not permitted. Deliberately a 401 rather than a 403: the
   *  authenticate callback runs before any method is resolved, so there is no
   *  route yet whose permissions could be evaluated. A service wanting a true
   *  403 raises it from the method body. */
  InsufficientScope: "insufficient_scope",
  /** The request carried no evidence of arriving through the trusted proxy.
   *  Derived from server configuration, never from the request. */
  ProxyRequired: "proxy_required",
  /** Refused, unclassified. The fallback. */
  Unauthorized: "unauthorized",
} as const;

/** One code from the closed set of {@link AuthReason}. */
export type AuthReason = (typeof AuthReason)[keyof typeof AuthReason];

const AUTH_REASONS: ReadonlySet<string> = new Set(Object.values(AuthReason));

/**
 * Property an error sets to steer the reason code of the 401 it produces.
 *
 * Duck-typed rather than checked with `instanceof` so an authenticator defined
 * outside this module can participate. The name is deliberately not `reason`:
 * {@link ProofError.reason} already means the verifier's internal code, which
 * must never reach a caller.
 */
export const AUTH_REASON_PROPERTY = "vgiAuthReason";

/**
 * A rejected credential, carrying the reason code the 401 will report.
 *
 * Throw one from an {@link AuthenticateFn} to classify the refusal; any other
 * error lands on {@link AuthReason.Unauthorized}. Guessing a finer code from an
 * unclassified failure would mean matching on message text.
 */
export class AuthFailure extends Error {
  /** The code reported on the wire. */
  readonly reason: AuthReason;
  /** Human-readable, may be empty. Free text — never a verifier's per-attempt
   *  state, which would leak which stage rejected. */
  readonly detail: string;
  /** Duck-typed marker read by {@link classifyAuthFailure}. */
  readonly vgiAuthReason: AuthReason;

  constructor(reason: AuthReason, detail = "") {
    super(detail || reason);
    // `chainAuthenticate` distinguishes credential rejections from bugs by
    // constructor identity, and knows this class by name — see `isCredentialError`.
    this.name = "AuthFailure";
    this.reason = reason;
    this.detail = detail;
    this.vgiAuthReason = reason;
  }
}

/**
 * An authenticator could not answer. **Not a rejection.**
 *
 * "The credential is bad" and "I could not find out whether the credential is
 * bad" are different answers, and collapsing them is expensive in both
 * directions. A sidecar restart that surfaces as 401 makes every caller
 * re-authenticate at once; the DuckDB extension treats a second 401 after a
 * refresh as fatal, so a thirty-second blip becomes a fleet-wide re-login
 * storm. And a caller that negative-caches a rejection will cache an outage.
 *
 * Deliberately **not** an {@link AuthFailure}, which is the whole point:
 * `chainAuthenticate` advances to the next authenticator on a credential
 * rejection, so an outage raised as one is read as "this credential isn't mine,
 * try the next" and ends up as a 401 from the end of the chain. Raised as this
 * type it propagates, and the handler renders `503` with `Retry-After` instead.
 *
 * Throw it for transport failures, timeouts, and 5xx from a remote authority.
 * Do not throw it for a credential the authority answered about.
 */
export class AuthUnavailableError extends Error {
  /** Seconds to advertise in `Retry-After`. A hint to retry, not a backoff
   *  schedule — keep it short. */
  readonly retryAfter: number;
  /** Operator-facing text. Must not contain the credential. */
  readonly detail: string;

  constructor(detail = "", retryAfter = 5) {
    super(detail || "authentication service unavailable");
    // `chainAuthenticate` classifies by constructor identity and knows this
    // class by name — see `isCredentialError`.
    this.name = "AuthUnavailableError";
    this.detail = detail;
    this.retryAfter = retryAfter;
  }
}

/**
 * Map an authenticate-callback error onto a reason code and detail.
 *
 * Anything that does not declare a reason is unclassified. A `PermissionError`
 * is the one exception: the name says the caller got as far as being
 * identified, so it maps onto {@link AuthReason.InsufficientScope} — a guess
 * from the error's *type*, not from its wording.
 */
export function classifyAuthFailure(err: unknown): { reason: AuthReason; detail: string } {
  const declared = (err as Record<string, unknown> | null)?.[AUTH_REASON_PROPERTY];
  const message = err instanceof Error ? err.message : "";
  if (typeof declared === "string" && AUTH_REASONS.has(declared)) {
    return { reason: declared as AuthReason, detail: err instanceof AuthFailure ? err.detail : message };
  }
  if (err instanceof Error && err.name === "PermissionError") {
    return { reason: AuthReason.InsufficientScope, detail: message };
  }
  return { reason: AuthReason.Unauthorized, detail: message };
}

/**
 * Compose the operator-facing note about proxy-dependent authentication.
 *
 * The wording is not normative — it is prose for a human. It must convey that
 * the service is only reachable through its proxy, which header names the
 * proxy must set, and that a rejection here is at least as likely to be a
 * proxy misconfiguration as a bad credential.
 *
 * Returns `""` when the service depends on no proxy-injected header, which is
 * what suppresses the note everywhere else.
 */
export function buildProxyHint(headers: readonly string[]): string {
  const names = [...new Set(headers)];
  if (names.length === 0) return "";
  return (
    `This service only accepts requests that arrive through its configured reverse proxy, ` +
    `which must set the ${names.join(", ")} header(s). A rejection here is at least as likely ` +
    `to be a proxy misconfiguration as a bad credential — check that the proxy is forwarding ` +
    `them before re-issuing credentials.`
  );
}

/**
 * Render the JSON envelope of spec §4.3.
 *
 * `proxy_hint` is absent, not empty, when it does not apply, so its presence
 * alone is a usable signal.
 */
export function unauthorizedEnvelope(reason: AuthReason, detail: string, proxyHint = ""): string {
  const payload: Record<string, string> = { error: "unauthorized", reason, detail };
  if (proxyHint) payload.proxy_hint = proxyHint;
  return JSON.stringify(payload);
}
