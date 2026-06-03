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
export declare function bearerAuthenticate(options: {
    validate: BearerValidateFn;
}): AuthenticateFn;
/**
 * Create a bearer-token authenticate callback from a static token map.
 *
 * Convenience wrapper around `bearerAuthenticate` that looks up the
 * token in a pre-built mapping using constant-time comparison.
 */
export declare function bearerAuthenticateStatic(options: {
    tokens: ReadonlyMap<string, AuthContext> | Record<string, AuthContext>;
}): AuthenticateFn;
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
export declare function chainAuthenticate(...authenticators: AuthenticateFn[]): AuthenticateFn;
//# sourceMappingURL=bearer.d.ts.map