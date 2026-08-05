// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import type { AuthContext } from "../auth.js";
import { constantTimeEqual } from "../util/web-crypto.js";
import type { AuthenticateFn } from "./auth.js";
import { AuthFailure, AuthReason, AuthUnavailableError } from "./unauthorized.js";

/** Receives the raw bearer token string, returns an AuthContext on success. Must throw on failure. */
export type BearerValidateFn = (token: string) => AuthContext | Promise<AuthContext>;

/**
 * Create a bearer-token authenticate callback.
 *
 * Extracts the `Authorization: Bearer <token>` header and delegates
 * validation to the user-supplied `validate` callback.
 */
export function bearerAuthenticate(options: { validate: BearerValidateFn }): AuthenticateFn {
  const { validate } = options;

  return async function authenticate(request: Request): Promise<AuthContext> {
    const authHeader = request.headers.get("Authorization") ?? "";
    // "send a credential" and "the credential you sent is not a Bearer one"
    // are different instructions to the caller, so they get different codes.
    if (!authHeader) {
      throw new AuthFailure(AuthReason.MissingCredential, "Missing Authorization header");
    }
    if (!authHeader.startsWith("Bearer ")) {
      throw new AuthFailure(AuthReason.InvalidCredential, "Authorization header is not a Bearer credential");
    }
    const token = authHeader.slice(7);
    return validate(token);
  };
}

/** Constant-time string comparison to prevent timing attacks on token lookup. */
function safeEqual(a: string, b: string): boolean {
  const enc = new TextEncoder();
  return constantTimeEqual(enc.encode(a), enc.encode(b));
}

/**
 * Create a bearer-token authenticate callback from a static token map.
 *
 * Convenience wrapper around `bearerAuthenticate` that looks up the
 * token in a pre-built mapping using constant-time comparison.
 */
export function bearerAuthenticateStatic(options: {
  tokens: ReadonlyMap<string, AuthContext> | Record<string, AuthContext>;
}): AuthenticateFn {
  const entries: [string, AuthContext][] =
    options.tokens instanceof Map ? [...options.tokens.entries()] : Object.entries(options.tokens);

  function validate(token: string): AuthContext {
    for (const [key, ctx] of entries) {
      if (safeEqual(token, key)) return ctx;
    }
    throw new AuthFailure(AuthReason.InvalidCredential, "Unknown bearer token");
  }

  return bearerAuthenticate({ validate });
}

/**
 * Check whether an error represents a credential rejection (should be
 * caught by the chain) vs a bug or authorization failure (should propagate).
 *
 * Mirrors Python's semantics where only `ValueError` is caught:
 * - Plain `Error` (constructor === Error) without `PermissionError` name → credential rejection
 * - `TypeError`, `RangeError`, etc. (Error subclasses) → bug, propagate
 * - `PermissionError` name → authorization failure, propagate
 * - Non-Error throws → propagate
 */
function isCredentialError(err: unknown): err is Error {
  // "I could not find out whether this credential is good" is not a rejection,
  // so it must not advance the chain: swallowed here it would emerge as a 401
  // from the end of the chain and turn an authority outage into a fleet-wide
  // re-login storm. Checked before AuthFailure so the two cannot be confused.
  if (err instanceof AuthUnavailableError) return false;
  // An AuthFailure is a credential rejection that merely names its reason, so
  // it has to keep composing — otherwise classifying a refusal would silently
  // turn the first alternative into the only one that is tried.
  if (err instanceof AuthFailure) return true;
  return err instanceof Error && err.constructor === Error && err.name !== "PermissionError";
}

/**
 * Reduce a chain's per-authenticator reason codes to the one the 401 carries.
 *
 * `missing_credential` only when *every* alternative agreed the caller
 * presented nothing — that is the case where telling them to send a credential
 * is actionable. As soon as one alternative saw something and rejected it,
 * that first substantive code wins, since "you sent nothing" would then be
 * actively misleading advice.
 */
function combineReasons(codes: readonly AuthReason[]): AuthReason {
  if (codes.length === 0) return AuthReason.Unauthorized;
  const substantive = codes.find((code) => code !== AuthReason.MissingCredential);
  return substantive ?? AuthReason.MissingCredential;
}

/**
 * Chain multiple authenticate callbacks, trying each in order.
 *
 * Each authenticator is called in sequence. Plain `Error` (credential
 * rejection) causes the next authenticator to be tried. Error subclasses
 * (`TypeError`, `RangeError`, etc.), `PermissionError`-named errors, and
 * non-Error throws propagate immediately.
 *
 * @throws Error if no authenticators are provided.
 */
export function chainAuthenticate(...authenticators: AuthenticateFn[]): AuthenticateFn {
  if (authenticators.length === 0) {
    throw new Error("chainAuthenticate requires at least one authenticator");
  }

  return async function authenticate(request: Request): Promise<AuthContext> {
    let lastError: Error | null = null;
    const codes: AuthReason[] = [];
    for (const authFn of authenticators) {
      try {
        return await authFn(request);
      } catch (err) {
        if (isCredentialError(err)) {
          lastError = err;
          codes.push(err instanceof AuthFailure ? err.reason : AuthReason.Unauthorized);
          continue;
        }
        throw err;
      }
    }
    const error = new AuthFailure(combineReasons(codes), "No authenticator accepted the request");
    if (lastError) error.cause = lastError;
    throw error;
  };
}
