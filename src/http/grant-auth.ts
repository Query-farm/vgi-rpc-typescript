// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Bearer authenticators that close the `vgi_rpc.Identity.v1` loop
 * (WIRE_PROTOCOL.md §16, "Accepting identity credentials"):
 *
 * - {@link grantAuthenticate} accepts the framework's own sealed grants.
 * - {@link resolveTokenAuthenticate} asks the worker's `resolveToken`.
 *
 * {@link composeIdentityAuthenticate} puts them after the deployment's own
 * authenticator in the normative order -- the deployment's (JWT, static) first,
 * then sealed grants, then `resolveToken` -- and `createHttpHandler` calls it
 * automatically for a host serving `vgi_rpc.Identity.v1`.
 *
 * Routing is by prefix and strict in both directions. A token without the
 * `vgig1.` prefix never reaches the grant verifier. A token *with* it that
 * does not verify is refused outright (401) and never reaches `resolveToken`:
 * a forged or stale grant must not get a second chance from a resolver that
 * might answer for it.
 */

import { AuthContext } from "../auth.js";
import { GRANT_TOKEN_PREFIX, GrantInvalidError, type GrantKeys, verifyGrantToken } from "../grants.js";
import {
  IdentityUnavailableError,
  isJwsShaped,
  MAX_TOKEN_BYTES,
  type TokenResolver,
  trimForShapeTest,
  utf8Length,
} from "../token-identity.js";
import type { AuthenticateFn } from "./auth.js";
import { chainAuthenticate } from "./bearer.js";
import { AUTH_REASON_PROPERTY, AuthFailure, AuthReason, AuthUnavailableError } from "./unauthorized.js";

/** `AuthContext.domain` of a grant-authenticated request. */
export const GRANT_AUTH_DOMAIN = "grant";
/** `AuthContext.domain` of a request authenticated through `resolveToken`. */
export const TOKEN_AUTH_DOMAIN = "token";

const BEARER = "Bearer ";

/**
 * A `vgig1.` credential that did not verify.
 *
 * Not an `AuthFailure` and not a plain `Error`, on purpose: `chainAuthenticate`
 * advances past both, and a grant-shaped token must stop here rather than fall
 * through to `resolveToken`. The handler still answers 401, with the reason
 * this error declares.
 */
export class GrantRejectedError extends Error {
  /** The 401 reason code (`invalid_credential` or `expired_credential`). */
  readonly vgiAuthReason: AuthReason;
  constructor(detail: string, reason: AuthReason) {
    super(detail);
    this.name = "GrantRejectedError";
    this.vgiAuthReason = reason;
    (this as Record<string, unknown>)[AUTH_REASON_PROPERTY] = reason;
  }
}

/** The bearer credential, or an `AuthFailure` (the chain moves on). */
function bearerOf(request: Request): string {
  const header = request.headers.get("Authorization") ?? "";
  if (!header) throw new AuthFailure(AuthReason.MissingCredential, "Missing Authorization header");
  if (!header.startsWith(BEARER)) {
    throw new AuthFailure(AuthReason.InvalidCredential, "Authorization header is not a Bearer credential");
  }
  return header.slice(BEARER.length);
}

/**
 * Accept the framework's own sealed grants as bearer credentials.
 *
 * The resulting `AuthContext` has domain `"grant"`, the grant's principal, and
 * claims `{grant_id, scopes, purpose}` -- and **no `auth_time`**, so a
 * grant-authenticated caller cannot `issue_grant`: grants never mint grants.
 */
export function grantAuthenticate(keys: GrantKeys): AuthenticateFn {
  return async (request: Request): Promise<AuthContext> => {
    const token = bearerOf(request);
    if (!token.startsWith(GRANT_TOKEN_PREFIX)) {
      throw new AuthFailure(AuthReason.InvalidCredential, "not a sealed grant");
    }
    let claims: Awaited<ReturnType<typeof verifyGrantToken>>;
    try {
      claims = await verifyGrantToken(keys, token);
    } catch (e) {
      if (e instanceof GrantInvalidError) {
        throw new GrantRejectedError(
          `sealed grant rejected: ${e.message}`,
          e.expired ? AuthReason.ExpiredCredential : AuthReason.InvalidCredential,
        );
      }
      throw e;
    }
    return new AuthContext(GRANT_AUTH_DOMAIN, true, claims.principal, {
      grant_id: claims.grantId,
      scopes: [...claims.scopes],
      purpose: claims.purpose,
    });
  };
}

/**
 * Accept bearer credentials the worker's `resolveToken` resolves.
 *
 * `null` from the hook falls through (401 if nothing else accepts). An outage
 * -- `AuthUnavailableError`, or `IdentityUnavailableError` from the same hook
 * -- propagates as `AuthUnavailableError`: 503 with `Retry-After`, never 401.
 * The hook never sees a `vgig1.` token, a JWS-shaped token, a blank one, or
 * one over 4096 UTF-8 bytes.
 */
export function resolveTokenAuthenticate(resolveToken: TokenResolver): AuthenticateFn {
  return async (request: Request): Promise<AuthContext> => {
    const token = bearerOf(request);
    if (token.startsWith(GRANT_TOKEN_PREFIX)) {
      throw new AuthFailure(AuthReason.InvalidCredential, "sealed grants are not resolved by resolveToken");
    }
    if (utf8Length(token) > MAX_TOKEN_BYTES || !trimForShapeTest(token)) {
      throw new AuthFailure(AuthReason.InvalidCredential, "bearer credential rejected");
    }
    if (isJwsShaped(token)) {
      throw new AuthFailure(AuthReason.InvalidCredential, "a JWS is not resolved by resolveToken");
    }
    let identity: Awaited<ReturnType<TokenResolver>>;
    try {
      identity = await resolveToken(token);
    } catch (e) {
      if (e instanceof IdentityUnavailableError) {
        throw new AuthUnavailableError(e.detail || "identity lookup unavailable", e.retryAfter);
      }
      throw e;
    }
    if (identity == null) {
      throw new AuthFailure(AuthReason.InvalidCredential, "bearer credential did not resolve");
    }
    return new AuthContext(TOKEN_AUTH_DOMAIN, true, identity.principal, { token_name: identity.tokenName ?? "" });
  };
}

/** Keep an unauthenticated server unauthenticated for requests with no credential. */
function anonymousWithoutCredentials(request: Request): AuthContext {
  if (request.headers.get("Authorization")) {
    throw new AuthFailure(AuthReason.InvalidCredential, "bearer credential not accepted");
  }
  return AuthContext.anonymous();
}

/** The identity sources {@link composeIdentityAuthenticate} appends. */
export interface IdentityBearerSources {
  /** Sealed-grant configuration, when grants are on. */
  grantKeys?: GrantKeys;
  /** The worker's `resolveToken`, when it has one. */
  resolveToken?: TokenResolver;
}

/**
 * Append the identity bearer authenticators after the deployment's own.
 *
 * Order: `authenticate` (JWT, static, ...), then sealed grants, then
 * `resolveToken`. With neither identity source, `authenticate` is returned
 * unchanged. With no `authenticate`, a request carrying no `Authorization`
 * header stays anonymous exactly as before; one carrying a bearer nothing
 * accepts is 401.
 *
 * `authenticate` must throw a plain `Error` or `AuthFailure` for a credential
 * it does not recognise -- one that answers anonymous for everything ends the
 * chain first.
 */
export function composeIdentityAuthenticate(
  authenticate: AuthenticateFn | undefined,
  sources: IdentityBearerSources,
): AuthenticateFn | undefined {
  const members: AuthenticateFn[] = [];
  if (sources.grantKeys) members.push(grantAuthenticate(sources.grantKeys));
  if (sources.resolveToken) members.push(resolveTokenAuthenticate(sources.resolveToken));
  if (members.length === 0) return authenticate;
  if (!authenticate) return chainAuthenticate(...members, anonymousWithoutCredentials);
  return chainAuthenticate(authenticate, ...members);
}
