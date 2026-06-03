import type { AuthenticateFn } from "./auth.js";
/** Options for {@link jwtAuthenticate}, configuring JWT Bearer-token validation. */
export interface JwtAuthenticateOptions {
    /** The expected `iss` claim (also used to discover AS metadata). */
    issuer: string;
    /** The expected `aud` claim. If an array, tries each audience in order. */
    audience: string | string[];
    /** Explicit JWKS URI. If omitted, discovered from issuer metadata. */
    jwksUri?: string;
    /** JWT claim to use as the principal. Default: "sub". */
    principalClaim?: string;
    /** AuthContext domain. Default: "jwt". */
    domain?: string;
}
/**
 * Create an AuthenticateFn that validates JWT Bearer tokens using oauth4webapi.
 *
 * On first call, discovers the Authorization Server metadata from the issuer
 * to obtain the JWKS URI (unless `jwksUri` is provided directly).
 */
export declare function jwtAuthenticate(options: JwtAuthenticateOptions): AuthenticateFn;
//# sourceMappingURL=jwt.d.ts.map