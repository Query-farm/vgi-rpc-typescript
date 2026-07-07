import type { AuthenticateFn } from "./auth.js";
/** Generate a 43-character URL-safe random code verifier (RFC 7636 S4.1). */
export declare function generateCodeVerifier(): string;
/** Compute S256 code challenge from a code verifier (RFC 7636 S4.2). */
export declare function generateCodeChallenge(verifier: string): string;
/** Generate a random state nonce for CSRF protection. */
export declare function generateStateNonce(): string;
/** Derive a separate HMAC key for OAuth session cookies. */
export declare function deriveSessionKey(signingKey: Uint8Array): Uint8Array;
/**
 * Pack PKCE session data into a signed, base64-encoded cookie value.
 *
 * Wire format v4:
 *   [1B version=4] [8B created_at uint64 LE]
 *   [2B cv_len uint16 LE] [cv_len bytes code_verifier]
 *   [2B state_len uint16 LE] [state_len bytes state_nonce]
 *   [2B url_len uint16 LE] [url_len bytes original_url]
 *   [2B rt_len uint16 LE] [rt_len bytes return_to]
 *   [32B HMAC-SHA256(session_key, all above)]
 */
export declare function packOAuthCookie(codeVerifier: string, stateNonce: string, originalUrl: string, sessionKey: Uint8Array, createdAt?: number, returnTo?: string): string;
export interface UnpackedOAuthCookie {
    codeVerifier: string;
    stateNonce: string;
    originalUrl: string;
    returnTo: string;
}
/**
 * Unpack and verify a signed OAuth session cookie.
 *
 * @throws Error on tampered, expired, or malformed cookies.
 */
export declare function unpackOAuthCookie(cookieValue: string, sessionKey: Uint8Array, maxAge?: number): UnpackedOAuthCookie;
export interface OidcEndpoints {
    authorizationEndpoint: string;
    tokenEndpoint: string;
}
/**
 * Create a lazy-cached OIDC discovery function.
 *
 * Caches the Promise; resets on rejection so a transient failure is retried.
 */
export declare function createOidcDiscovery(issuer: string): () => Promise<OidcEndpoints | null>;
export interface TokenExchangeResult {
    token: string;
    maxAge: number;
    refreshToken: string | null;
    /** The OIDC id_token from the token response, if present. Used to derive the
     *  JS-readable `_vgi_identity` display cookie regardless of whether the bearer
     *  is the id_token or an opaque access_token. */
    idToken: string | null;
}
/** Exchange an authorization code for a token via the token endpoint. */
export declare function exchangeCodeForToken(tokenEndpoint: string, code: string, redirectUri: string, codeVerifier: string, clientId: string, clientSecret?: string, useIdToken?: boolean): Promise<TokenExchangeResult>;
/** Validate the original URL is relative and within the expected prefix. */
export declare function validateOriginalUrl(url: string, prefix: string): string;
/** Validate an external return-to URL against an origin allowlist. */
export declare function validateReturnTo(url: string, allowedOrigins?: ReadonlySet<string>): string;
/** Parse the Cookie header from a Request into a Map. */
export declare function parseCookies(request: Request): Map<string, string>;
interface SetCookieOptions {
    maxAge?: number;
    path?: string;
    secure?: boolean;
    httpOnly?: boolean;
    sameSite?: "Strict" | "Lax" | "None";
}
/** Best-effort decode of a JWT payload (no signature verification). */
export declare function decodeJwtPayload(token: string | null | undefined): Record<string, unknown> | null;
/**
 * base64url(JSON) of the display-identity claims from an id_token, or null.
 *
 * Encodes only `{sub, email, preferred_username, name, picture}` (present ones)
 * so the shared landing page can render the identity pill without decoding a
 * bearer token itself. Padding is stripped to match the Python reference.
 */
export declare function identityCookieValue(idToken: string | null | undefined): string | null;
/** Build a Set-Cookie header string. */
export declare function buildSetCookieHeader(name: string, value: string, options: SetCookieOptions): string;
/** Render a user-friendly OAuth error page. */
export declare function buildOAuthErrorPage(message: string, detail: string | null, retryUrl: string): string;
/**
 * Create an authenticate callback that reads a bearer token from a cookie.
 *
 * Extracts the token from the named cookie and delegates validation to the
 * `innerAuth` authenticator by creating a new Request with an Authorization header.
 */
export declare function cookieAuthenticate(innerAuth: AuthenticateFn, cookieName?: string): AuthenticateFn;
/** Configuration object produced by configureOAuthPkce. */
export interface OAuthPkceConfig {
    sessionKey: Uint8Array;
    oidcDiscovery: () => Promise<OidcEndpoints | null>;
    clientId: string;
    clientSecret: string | undefined;
    useIdToken: boolean;
    prefix: string;
    secureCookie: boolean;
    redirectUri: string;
    scope: string;
    allowedReturnOrigins: ReadonlySet<string>;
    cookieAuthenticate: AuthenticateFn;
}
/** Options for configureOAuthPkce. */
export interface OAuthPkceOptions {
    signingKey: Uint8Array;
    issuer: string;
    clientId: string;
    clientSecret?: string;
    useIdToken?: boolean;
    prefix: string;
    secureCookie: boolean;
    redirectUri: string;
    scope?: string;
    allowedReturnOrigins?: ReadonlySet<string>;
}
/**
 * Resolve the OAuth PKCE `scope` string from available sources.
 *
 * Precedence:
 *   1. `scopesSupported` from OAuth resource metadata (space-joined), when non-empty.
 *   2. Explicit `optionsScope` override (e.g. `HttpHandlerOptions.oauthPkceScope`).
 *   3. `undefined`, which lets `configureOAuthPkce` apply its built-in default of
 *      `"openid email"`.
 *
 * Mirrors the Python reference behavior introduced in vgi-rpc v0.6.12: authorization
 * requests should use the scopes the server publishes in its protected resource
 * metadata, so clients ask for exactly what the resource advertises.
 */
export declare function resolvePkceScope(scopesSupported: readonly string[] | undefined, optionsScope: string | undefined): string | undefined;
/** Factory function wiring all PKCE components. */
export declare function configureOAuthPkce(opts: OAuthPkceOptions, innerAuth: AuthenticateFn): OAuthPkceConfig;
/**
 * Handle POST/OPTIONS {prefix}/_oauth/token — the PKCE token-exchange proxy.
 *
 * SPA PKCE clients cannot safely hold a client_secret, but some IdPs
 * (notably Google) reject token-endpoint requests from "Web application"
 * clients without one. This handler accepts authorization_code/refresh_token
 * exchanges from a browser, injects the configured server-side
 * client_secret, and forwards the request to the IdP's real token_endpoint.
 * The IdP response is returned verbatim (status code + body).
 */
export declare function handleOAuthTokenProxy(request: Request, config: OAuthPkceConfig): Promise<Response>;
/** Handle GET {prefix}/_oauth/callback — the redirect from the authorization server. */
export declare function handleOAuthCallback(request: Request, config: OAuthPkceConfig): Promise<Response>;
/** Handle GET {prefix}/_oauth/logout — clear auth cookie and redirect. */
export declare function handleOAuthLogout(_request: Request, config: OAuthPkceConfig): Response;
/** Redirect an unauthenticated browser GET to the OAuth authorization endpoint. Returns null if unable. */
export declare function handleBrowserGetRedirect(request: Request, config: OAuthPkceConfig): Promise<Response | null>;
/** If user is already authenticated and has _vgi_return_to, redirect immediately. Returns null otherwise. */
export declare function handleEarlyReturnTo(request: Request, config: OAuthPkceConfig): Response | null;
export {};
//# sourceMappingURL=oauth-pkce.d.ts.map