// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { timingSafeEqual } from "node:crypto";
import type { AuthContext } from "../auth.js";
import type { AuthenticateFn } from "./auth.js";

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
    if (!authHeader.startsWith("Bearer ")) {
      throw new Error("Missing or invalid Authorization header");
    }
    const token = authHeader.slice(7);
    return validate(token);
  };
}

/** Constant-time string comparison to prevent timing attacks on token lookup. */
function safeEqual(a: string, b: string): boolean {
  const enc = new TextEncoder();
  const bufA = enc.encode(a);
  const bufB = enc.encode(b);
  if (bufA.byteLength !== bufB.byteLength) return false;
  return timingSafeEqual(bufA, bufB);
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
    throw new Error("Unknown bearer token");
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
  return err instanceof Error && err.constructor === Error && err.name !== "PermissionError";
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
    for (const authFn of authenticators) {
      try {
        return await authFn(request);
      } catch (err) {
        if (isCredentialError(err)) {
          lastError = err;
          continue;
        }
        throw err;
      }
    }
    const error = new Error("No authenticator accepted the request");
    if (lastError) error.cause = lastError;
    throw error;
  };
}
