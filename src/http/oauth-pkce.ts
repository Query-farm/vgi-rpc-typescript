// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Server-side OAuth PKCE authorization code flow for vgi-rpc HTTP browse pages.
 *
 * When both `authenticate` and `oauthResourceMetadata` (with a `clientId`)
 * are configured, this module enables browser-based authentication:
 *
 * 1. A browser GET that would return 401 is instead redirected to the
 *    authorization server's login page (with PKCE code challenge).
 * 2. After the user authenticates, the authorization server redirects back
 *    to `{prefix}/_oauth/callback` with an authorization code.
 * 3. The callback exchanges the code for a token, stores it in a JS-readable
 *    cookie, and redirects back to the original page.
 */

import type { AuthContext } from "../auth.js";
import type { AuthenticateFn } from "./auth.js";
import { ERROR_PAGE_STYLE, FONTS, LOGO_URL } from "./pages.js";

// Indirect-string require keeps node:crypto out of the static bundle for
// workerd. OAuth PKCE is opt-in (configureOAuthPkce); callers on workerd
// should not enable it.
const _NODE_CRYPTO_MOD = "node:crypto";
function _crypto(): {
  createHash: any;
  createHmac: any;
  randomBytes: (n: number) => any;
  timingSafeEqual: (a: any, b: any) => boolean;
} {
  const req: any = (globalThis as any).require ?? null;
  if (!req) {
    throw new Error("OAuth PKCE requires Node.js or Bun (node:crypto).");
  }
  return req(_NODE_CRYPTO_MOD);
}
const createHash = (algo: string) => _crypto().createHash(algo);
const createHmac = (algo: string, key: any) => _crypto().createHmac(algo, key);
const randomBytes = (n: number) => _crypto().randomBytes(n);
const timingSafeEqual = (a: any, b: any) => _crypto().timingSafeEqual(a, b);

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SESSION_COOKIE_NAME = "_vgi_oauth_session";
const AUTH_COOKIE_NAME = "_vgi_auth";
const SESSION_COOKIE_VERSION = 4;
const SESSION_MAX_AGE = 600; // 10 minutes
const AUTH_COOKIE_DEFAULT_MAX_AGE = 3600; // 1 hour fallback
const MAX_ORIGINAL_URL_LEN = 2048;
const HMAC_LEN = 32;

/** Default origins allowed for _vgi_return_to redirects. */
const DEFAULT_ALLOWED_RETURN_ORIGINS: ReadonlySet<string> = new Set(["https://cupola.query-farm.services"]);

// ---------------------------------------------------------------------------
// PKCE helpers (RFC 7636)
// ---------------------------------------------------------------------------

/** Generate a 43-character URL-safe random code verifier (RFC 7636 S4.1). */
export function generateCodeVerifier(): string {
  // Match Python secrets.token_urlsafe(32) — 32 random bytes → base64url (no padding) = 43 chars
  return randomBytes(32).toString("base64url");
}

/** Compute S256 code challenge from a code verifier (RFC 7636 S4.2). */
export function generateCodeChallenge(verifier: string): string {
  const digest = createHash("sha256").update(verifier, "ascii").digest();
  return digest.toString("base64url");
}

/** Generate a random state nonce for CSRF protection. */
export function generateStateNonce(): string {
  return randomBytes(24).toString("base64url");
}

// ---------------------------------------------------------------------------
// Derived HMAC key
// ---------------------------------------------------------------------------

/** Derive a separate HMAC key for OAuth session cookies. */
export function deriveSessionKey(signingKey: Uint8Array): Uint8Array {
  return createHmac("sha256", signingKey).update("oauth-pkce-session").digest();
}

// ---------------------------------------------------------------------------
// Base64url encoding matching Python's base64.urlsafe_b64encode (WITH padding)
// ---------------------------------------------------------------------------

/** Encode bytes as base64url WITH `=` padding (matches Python `base64.urlsafe_b64encode`). */
function b64urlEncode(buf: Buffer): string {
  return buf.toString("base64").replace(/\+/g, "-").replace(/\//g, "_");
}

/** Decode base64url string (with or without padding) to Buffer. */
function b64urlDecode(s: string): Buffer {
  // base64url → standard base64
  const standard = s.replace(/-/g, "+").replace(/_/g, "/");
  return Buffer.from(standard, "base64");
}

// ---------------------------------------------------------------------------
// Signed session cookie (stores code_verifier + state + original URL)
// ---------------------------------------------------------------------------

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
export function packOAuthCookie(
  codeVerifier: string,
  stateNonce: string,
  originalUrl: string,
  sessionKey: Uint8Array,
  createdAt?: number,
  returnTo?: string,
): string {
  const now = createdAt ?? Math.floor(Date.now() / 1000);
  const cvBytes = Buffer.from(codeVerifier, "utf-8");
  const stateBytes = Buffer.from(stateNonce, "utf-8");
  const urlBytes = Buffer.from(originalUrl, "utf-8");
  const rtBytes = Buffer.from(returnTo ?? "", "utf-8");

  const payloadLen = 1 + 8 + 2 + cvBytes.length + 2 + stateBytes.length + 2 + urlBytes.length + 2 + rtBytes.length;
  const payload = Buffer.alloc(payloadLen);
  let offset = 0;

  payload.writeUInt8(SESSION_COOKIE_VERSION, offset);
  offset += 1;

  payload.writeBigUInt64LE(BigInt(now), offset);
  offset += 8;

  payload.writeUInt16LE(cvBytes.length, offset);
  offset += 2;
  cvBytes.copy(payload, offset);
  offset += cvBytes.length;

  payload.writeUInt16LE(stateBytes.length, offset);
  offset += 2;
  stateBytes.copy(payload, offset);
  offset += stateBytes.length;

  payload.writeUInt16LE(urlBytes.length, offset);
  offset += 2;
  urlBytes.copy(payload, offset);
  offset += urlBytes.length;

  payload.writeUInt16LE(rtBytes.length, offset);
  offset += 2;
  rtBytes.copy(payload, offset);

  const mac = createHmac("sha256", sessionKey).update(payload).digest();
  return b64urlEncode(Buffer.concat([payload, mac]));
}

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
export function unpackOAuthCookie(
  cookieValue: string,
  sessionKey: Uint8Array,
  maxAge: number = SESSION_MAX_AGE,
): UnpackedOAuthCookie {
  let raw: Buffer;
  try {
    raw = b64urlDecode(cookieValue);
  } catch {
    throw new Error("Malformed session cookie");
  }

  // Minimum: version(1) + timestamp(8) + 4 x length(2) + HMAC(32) = 49
  if (raw.length < 49) {
    throw new Error("Session cookie too short");
  }

  // Verify HMAC before inspecting payload
  const payload = raw.subarray(0, raw.length - HMAC_LEN);
  const receivedMac = raw.subarray(raw.length - HMAC_LEN);
  const expectedMac = createHmac("sha256", sessionKey).update(payload).digest();
  if (!timingSafeEqual(receivedMac, expectedMac)) {
    throw new Error("Session cookie signature mismatch");
  }

  // Parse payload
  const version = payload.readUInt8(0);
  if (version !== SESSION_COOKIE_VERSION) {
    throw new Error(`Unexpected session cookie version: ${version}`);
  }

  const createdAt = Number(payload.readBigUInt64LE(1));
  if (maxAge > 0) {
    const age = Math.floor(Date.now() / 1000) - createdAt;
    if (age < 0 || age > maxAge) {
      throw new Error(`Session cookie expired (age=${age}s, max=${maxAge}s)`);
    }
  }

  let pos = 9;
  const cvLen = payload.readUInt16LE(pos);
  pos += 2;
  const codeVerifier = payload.subarray(pos, pos + cvLen).toString("utf-8");
  pos += cvLen;

  const stateLen = payload.readUInt16LE(pos);
  pos += 2;
  const stateNonce = payload.subarray(pos, pos + stateLen).toString("utf-8");
  pos += stateLen;

  const urlLen = payload.readUInt16LE(pos);
  pos += 2;
  const originalUrl = payload.subarray(pos, pos + urlLen).toString("utf-8");
  pos += urlLen;

  const rtLen = payload.readUInt16LE(pos);
  pos += 2;
  const returnTo = payload.subarray(pos, pos + rtLen).toString("utf-8");

  return { codeVerifier, stateNonce, originalUrl, returnTo };
}

// ---------------------------------------------------------------------------
// OIDC discovery cache
// ---------------------------------------------------------------------------

export interface OidcEndpoints {
  authorizationEndpoint: string;
  tokenEndpoint: string;
}

/**
 * Create a lazy-cached OIDC discovery function.
 *
 * Caches the Promise; resets on rejection so a transient failure is retried.
 */
export function createOidcDiscovery(issuer: string): () => Promise<OidcEndpoints | null> {
  let cached: Promise<OidcEndpoints | null> | null = null;

  return function discover(): Promise<OidcEndpoints | null> {
    if (cached) return cached;
    const url = `${issuer.replace(/\/+$/, "")}/.well-known/openid-configuration`;
    cached = fetch(url, { signal: AbortSignal.timeout(10000) })
      .then(async (resp) => {
        if (!resp.ok) throw new Error(`OIDC discovery HTTP ${resp.status}`);
        const data = await resp.json();
        return {
          authorizationEndpoint: data.authorization_endpoint as string,
          tokenEndpoint: data.token_endpoint as string,
        };
      })
      .catch(() => {
        cached = null; // reset on failure for retry
        return null;
      });
    return cached;
  };
}

// ---------------------------------------------------------------------------
// Token exchange
// ---------------------------------------------------------------------------

export interface TokenExchangeResult {
  token: string;
  maxAge: number;
  refreshToken: string | null;
}

/** Exchange an authorization code for a token via the token endpoint. */
export async function exchangeCodeForToken(
  tokenEndpoint: string,
  code: string,
  redirectUri: string,
  codeVerifier: string,
  clientId: string,
  clientSecret?: string,
  useIdToken?: boolean,
): Promise<TokenExchangeResult> {
  const params = new URLSearchParams({
    grant_type: "authorization_code",
    code,
    redirect_uri: redirectUri,
    code_verifier: codeVerifier,
    client_id: clientId,
  });
  if (clientSecret) {
    params.set("client_secret", clientSecret);
  }

  let body: any;
  try {
    const resp = await fetch(tokenEndpoint, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: params.toString(),
      signal: AbortSignal.timeout(15000),
    });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(`HTTP ${resp.status}: ${text}`);
    }
    body = await resp.json();
  } catch (err: any) {
    throw new Error(`Token exchange failed: ${err.message ?? err}`);
  }

  const refreshToken: string | null = body.refresh_token ?? null;

  if (useIdToken) {
    const token = body.id_token;
    if (!token) throw new Error("Token response missing id_token");
    // Derive maxAge from the id_token's exp claim
    try {
      const parts = (token as string).split(".");
      if (parts.length >= 2) {
        const padding = 4 - (parts[1].length % 4);
        const payloadJson = Buffer.from(parts[1] + "=".repeat(padding % 4), "base64").toString("utf-8");
        const claims = JSON.parse(payloadJson);
        if (claims.exp != null) {
          const maxAge = Math.max(Number(claims.exp) - Math.floor(Date.now() / 1000), 60);
          return { token, maxAge, refreshToken };
        }
      }
    } catch {
      // Fall through to default
    }
    return { token, maxAge: AUTH_COOKIE_DEFAULT_MAX_AGE, refreshToken };
  }

  const token = body.access_token;
  if (!token) throw new Error("Token response missing access_token");
  const expiresIn = body.expires_in ?? AUTH_COOKIE_DEFAULT_MAX_AGE;
  return { token, maxAge: Number(expiresIn), refreshToken };
}

// ---------------------------------------------------------------------------
// Original URL validation
// ---------------------------------------------------------------------------

/** Validate the original URL is relative and within the expected prefix. */
export function validateOriginalUrl(url: string, prefix: string): string {
  let u = url;
  if (u.length > MAX_ORIGINAL_URL_LEN) {
    u = u.slice(0, MAX_ORIGINAL_URL_LEN);
  }
  try {
    const parsed = new URL(u, "http://dummy");
    // If the URL has a different origin than dummy, it's absolute
    if (u.startsWith("http://") || u.startsWith("https://") || u.startsWith("//")) {
      return prefix || "/";
    }
    // Check hostname is "dummy" (relative URL) — otherwise it's absolute
    if (parsed.hostname !== "dummy") {
      return prefix || "/";
    }
  } catch {
    return prefix || "/";
  }
  if (prefix && !u.startsWith(prefix)) {
    return prefix || "/";
  }
  return u;
}

function isLocalhost(hostname: string): boolean {
  return hostname === "localhost" || hostname === "127.0.0.1" || hostname === "[::1]";
}

/** Validate an external return-to URL against an origin allowlist. */
export function validateReturnTo(url: string, allowedOrigins?: ReadonlySet<string>): string {
  const origins = allowedOrigins ?? DEFAULT_ALLOWED_RETURN_ORIGINS;
  if (!url || url.length > 2048) return "";
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return "";
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return "";
  if (!parsed.hostname) return "";
  // localhost with any port is always allowed
  if (isLocalhost(parsed.hostname) && parsed.protocol === "http:") return url;
  // Check against allowlist (scheme + host, ignoring path)
  const origin = `${parsed.protocol}//${parsed.hostname}`;
  if (origins.has(origin)) return url;
  // Also try with explicit port
  if (parsed.port) {
    const originWithPort = `${parsed.protocol}//${parsed.hostname}:${parsed.port}`;
    if (origins.has(originWithPort)) return url;
  }
  return "";
}

// ---------------------------------------------------------------------------
// Cookie helpers
// ---------------------------------------------------------------------------

/** Parse the Cookie header from a Request into a Map. */
export function parseCookies(request: Request): Map<string, string> {
  const header = request.headers.get("Cookie");
  const map = new Map<string, string>();
  if (!header) return map;
  for (const pair of header.split(";")) {
    const eq = pair.indexOf("=");
    if (eq < 0) continue;
    const name = pair.slice(0, eq).trim();
    const value = pair.slice(eq + 1).trim();
    map.set(name, value);
  }
  return map;
}

interface SetCookieOptions {
  maxAge?: number;
  path?: string;
  secure?: boolean;
  httpOnly?: boolean;
  sameSite?: "Strict" | "Lax" | "None";
}

/** Build a Set-Cookie header string. */
export function buildSetCookieHeader(name: string, value: string, options: SetCookieOptions): string {
  let cookie = `${name}=${value}`;
  if (options.maxAge !== undefined) cookie += `; Max-Age=${options.maxAge}`;
  if (options.path) cookie += `; Path=${options.path}`;
  if (options.secure) cookie += "; Secure";
  if (options.httpOnly) cookie += "; HttpOnly";
  if (options.sameSite) cookie += `; SameSite=${options.sameSite}`;
  return cookie;
}

// ---------------------------------------------------------------------------
// Error HTML page
// ---------------------------------------------------------------------------

function escapeHtml(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

/** Render a user-friendly OAuth error page. */
export function buildOAuthErrorPage(message: string, detail: string | null, retryUrl: string): string {
  const detailHtml = detail ? `<div class="detail">${escapeHtml(detail)}</div>` : "";
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Authentication Error \u2014 vgi-rpc</title>
${FONTS}
${ERROR_PAGE_STYLE}
</head>
<body>
<div class="logo">
  <img src="${LOGO_URL}" alt="vgi-rpc logo">
</div>
<h1>Authentication Error</h1>
<p>${escapeHtml(message)}</p>
${detailHtml}
<p><a href="${escapeHtml(retryUrl)}">Try again</a></p>
<footer>
  Powered by <a href="https://vgi-rpc.query.farm"><code>vgi-rpc</code></a>
</footer>
</body>
</html>`;
}

// ---------------------------------------------------------------------------
// User-info JS snippet for landing/describe pages
// ---------------------------------------------------------------------------

const USER_INFO_STYLE = `#vgi-user-info {
  position: fixed; top: 12px; right: 16px; z-index: 1000;
  font-family: 'Inter', system-ui, sans-serif; font-size: 0.85em;
  display: flex; align-items: center; gap: 8px;
  background: #fff; border: 1px solid #e0ddd0; border-radius: 20px;
  padding: 4px 14px 4px 6px; box-shadow: 0 2px 8px rgba(0,0,0,0.06);
}
#vgi-user-info img {
  width: 26px; height: 26px; border-radius: 50%;
}
#vgi-user-info .email { color: #2c2c1e; font-weight: 500; }
#vgi-user-info a {
  color: #6b6b5a; text-decoration: none; margin-left: 4px;
  font-size: 0.9em;
}
#vgi-user-info a:hover { color: #8b0000; }`;

function buildUserInfoScript(cookieName: string, logoutUrl: string): string {
  return `(function() {
  var c = document.cookie.match('(^|;)\\\\s*${cookieName}=([^;]+)');
  if (!c) return;
  try {
    var parts = c[2].split('.');
    var payload = JSON.parse(atob(parts[1].replace(/-/g,'+').replace(/_/g,'/')));
    var el = document.getElementById('vgi-user-info');
    if (!el) return;
    var html = '';
    if (payload.picture) html += '<img src="' + payload.picture + '" alt="">';
    html += '<span class="email">' + (payload.email || payload.sub || '') + '</span>';
    html += '<a href="${logoutUrl}">Sign out</a>';
    el.innerHTML = html;
  } catch(e) {}
})();`;
}

/** Return HTML snippet (style + div + script) for user info display. */
export function buildUserInfoHtml(prefix: string): string {
  const logoutUrl = `${prefix}/_oauth/logout`;
  return (
    `<style>${USER_INFO_STYLE}</style>\n` +
    `<div id="vgi-user-info"></div>\n` +
    `<script>${buildUserInfoScript(AUTH_COOKIE_NAME, logoutUrl)}</script>`
  );
}

// ---------------------------------------------------------------------------
// Cookie authenticate function
// ---------------------------------------------------------------------------

/**
 * Create an authenticate callback that reads a bearer token from a cookie.
 *
 * Extracts the token from the named cookie and delegates validation to the
 * `innerAuth` authenticator by creating a new Request with an Authorization header.
 */
export function cookieAuthenticate(innerAuth: AuthenticateFn, cookieName: string = AUTH_COOKIE_NAME): AuthenticateFn {
  return async function authenticate(request: Request): Promise<AuthContext> {
    const cookies = parseCookies(request);
    const token = cookies.get(cookieName);
    if (!token) {
      throw new Error("No auth cookie");
    }
    // Create a new Request with the cookie token as an Authorization header
    const newHeaders = new Headers(request.headers);
    newHeaders.set("Authorization", `Bearer ${token}`);
    const newRequest = new Request(request.url, {
      method: request.method,
      headers: newHeaders,
    });
    return innerAuth(newRequest);
  };
}

// ---------------------------------------------------------------------------
// OAuth PKCE configuration
// ---------------------------------------------------------------------------

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
  userInfoHtml: string;
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
export function resolvePkceScope(
  scopesSupported: readonly string[] | undefined,
  optionsScope: string | undefined,
): string | undefined {
  if (scopesSupported && scopesSupported.length > 0) {
    return scopesSupported.join(" ");
  }
  return optionsScope;
}

/** Factory function wiring all PKCE components. */
export function configureOAuthPkce(opts: OAuthPkceOptions, innerAuth: AuthenticateFn): OAuthPkceConfig {
  const sessionKey = deriveSessionKey(opts.signingKey);
  const oidcDiscovery = createOidcDiscovery(opts.issuer);
  return {
    sessionKey,
    oidcDiscovery,
    clientId: opts.clientId,
    clientSecret: opts.clientSecret,
    useIdToken: opts.useIdToken ?? false,
    prefix: opts.prefix,
    secureCookie: opts.secureCookie,
    redirectUri: opts.redirectUri,
    scope: opts.scope ?? "openid email",
    allowedReturnOrigins: opts.allowedReturnOrigins ?? DEFAULT_ALLOWED_RETURN_ORIGINS,
    cookieAuthenticate: cookieAuthenticate(innerAuth),
    userInfoHtml: buildUserInfoHtml(opts.prefix),
  };
}

// ---------------------------------------------------------------------------
// OAuth token-exchange proxy
// ---------------------------------------------------------------------------

const ALLOWED_TOKEN_GRANT_TYPES: ReadonlySet<string> = new Set(["authorization_code", "refresh_token"]);

function isLocalhostHost(hostname: string): boolean {
  return hostname === "localhost" || hostname === "127.0.0.1" || hostname === "[::1]";
}

/** Set Access-Control-Allow-Origin when the request's Origin is in the allowlist (or is localhost). */
function setProxyCors(headers: Headers, request: Request, config: OAuthPkceConfig): void {
  headers.append("Vary", "Origin");
  const origin = request.headers.get("Origin");
  if (!origin) return;
  let parsed: URL;
  try {
    parsed = new URL(origin);
  } catch {
    return;
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return;
  if (!parsed.hostname) return;
  if (isLocalhostHost(parsed.hostname) && parsed.protocol === "http:") {
    headers.set("Access-Control-Allow-Origin", origin);
    return;
  }
  if (config.allowedReturnOrigins.has(origin)) {
    headers.set("Access-Control-Allow-Origin", origin);
  }
}

function jsonErrorResponse(headers: Headers, status: number, error: string, description: string): Response {
  headers.set("Content-Type", "application/json");
  return new Response(JSON.stringify({ error, error_description: description }), { status, headers });
}

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
export async function handleOAuthTokenProxy(request: Request, config: OAuthPkceConfig): Promise<Response> {
  const headers = new Headers();
  setProxyCors(headers, request, config);

  if (request.method === "OPTIONS") {
    headers.set("Access-Control-Allow-Methods", "POST, OPTIONS");
    headers.set("Access-Control-Allow-Headers", "Content-Type");
    headers.set("Access-Control-Max-Age", "7200");
    return new Response(null, { status: 204, headers });
  }

  if (request.method !== "POST") {
    return new Response(null, { status: 405, headers });
  }

  const ctype = (request.headers.get("Content-Type") ?? "").split(";")[0].trim().toLowerCase();
  if (ctype !== "application/x-www-form-urlencoded") {
    return jsonErrorResponse(headers, 415, "invalid_request",
      "Content-Type must be application/x-www-form-urlencoded");
  }

  let raw: string;
  try {
    raw = await request.text();
  } catch {
    return jsonErrorResponse(headers, 400, "invalid_request", "Could not read request body");
  }

  let form: URLSearchParams;
  try {
    form = new URLSearchParams(raw);
  } catch {
    return jsonErrorResponse(headers, 400, "invalid_request", "Could not parse form body");
  }

  const grantType = form.get("grant_type") ?? "";
  if (!ALLOWED_TOKEN_GRANT_TYPES.has(grantType)) {
    return jsonErrorResponse(headers, 400, "unsupported_grant_type",
      "grant_type must be authorization_code or refresh_token");
  }

  const submittedClientId = form.get("client_id");
  if (submittedClientId && submittedClientId !== config.clientId) {
    return jsonErrorResponse(headers, 400, "invalid_client",
      "client_id does not match the configured client");
  }

  const endpoints = await config.oidcDiscovery();
  if (!endpoints) {
    return jsonErrorResponse(headers, 502, "server_error", "Authorization server discovery failed");
  }

  const upstream = new URLSearchParams();
  upstream.set("grant_type", grantType);
  upstream.set("client_id", config.clientId);
  if (config.clientSecret) {
    upstream.set("client_secret", config.clientSecret);
  }
  for (const key of ["code", "code_verifier", "redirect_uri", "refresh_token", "scope"]) {
    const value = form.get(key);
    if (value !== null && value !== "") {
      upstream.set(key, value);
    }
  }

  let upstreamResp: Response;
  try {
    upstreamResp = await fetch(endpoints.tokenEndpoint, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: upstream.toString(),
      signal: AbortSignal.timeout(15000),
    });
  } catch (err: any) {
    return jsonErrorResponse(headers, 502, "server_error",
      `Upstream token endpoint failed: ${err?.message ?? err}`);
  }

  const body = new Uint8Array(await upstreamResp.arrayBuffer());
  const ct = upstreamResp.headers.get("content-type") ?? "application/json";
  headers.set("Content-Type", ct);
  return new Response(body, { status: upstreamResp.status, headers });
}

// ---------------------------------------------------------------------------
// OAuth callback handler
// ---------------------------------------------------------------------------

/** Handle GET {prefix}/_oauth/callback — the redirect from the authorization server. */
export async function handleOAuthCallback(request: Request, config: OAuthPkceConfig): Promise<Response> {
  const url = new URL(request.url);
  const retryUrl = config.prefix || "/";

  function errorResponse(status: number, message: string, detail: string | null): Response {
    return new Response(buildOAuthErrorPage(message, detail, retryUrl), {
      status,
      headers: { "Content-Type": "text/html; charset=utf-8" },
    });
  }

  // Check for authorization server error
  const error = url.searchParams.get("error");
  if (error) {
    const errorDesc = url.searchParams.get("error_description") ?? error;
    return errorResponse(400, "The authorization server returned an error.", errorDesc);
  }

  // Extract code and state from query
  const code = url.searchParams.get("code");
  const state = url.searchParams.get("state");
  if (!code || !state) {
    return errorResponse(400, "Missing authorization code or state parameter.", null);
  }

  // Read and validate session cookie
  const cookies = parseCookies(request);
  const sessionCookie = cookies.get(SESSION_COOKIE_NAME);
  if (!sessionCookie) {
    return errorResponse(400, "Session cookie missing or expired. Please try again.", null);
  }

  let unpacked: UnpackedOAuthCookie;
  try {
    unpacked = unpackOAuthCookie(sessionCookie, config.sessionKey);
  } catch {
    return errorResponse(400, "Session expired or invalid. Please try again.", null);
  }

  // CSRF: validate state matches (constant-time)
  const stateA = Buffer.from(state, "utf-8");
  const stateB = Buffer.from(unpacked.stateNonce, "utf-8");
  if (stateA.length !== stateB.length || !timingSafeEqual(stateA, stateB)) {
    return errorResponse(400, "State mismatch \u2014 possible CSRF. Please try again.", null);
  }

  // Discover token endpoint
  const endpoints = await config.oidcDiscovery();
  if (!endpoints) {
    return errorResponse(502, "Could not reach the authorization server.", "OIDC discovery failed.");
  }

  // Exchange code for token
  let result: TokenExchangeResult;
  try {
    result = await exchangeCodeForToken(
      endpoints.tokenEndpoint,
      code,
      config.redirectUri,
      unpacked.codeVerifier,
      config.clientId,
      config.clientSecret,
      config.useIdToken,
    );
  } catch (err: any) {
    return errorResponse(502, "Token exchange with the authorization server failed.", String(err.message ?? err));
  }

  const clearSessionCookie = buildSetCookieHeader(SESSION_COOKIE_NAME, "", {
    maxAge: 0,
    path: `${config.prefix}/_oauth/`,
    secure: config.secureCookie,
    httpOnly: true,
    sameSite: "Lax",
  });

  // External frontend: redirect with token + OAuth metadata in URL fragment
  if (unpacked.returnTo) {
    const separator = unpacked.returnTo.includes("#") ? "&" : "#";
    const fragmentParts = [`token=${result.token}`];
    if (result.refreshToken) {
      fragmentParts.push(`refresh_token=${encodeURIComponent(result.refreshToken)}`);
    }
    fragmentParts.push(`token_endpoint=${encodeURIComponent(endpoints.tokenEndpoint)}`);
    fragmentParts.push(`client_id=${encodeURIComponent(config.clientId)}`);
    if (config.clientSecret) {
      fragmentParts.push(`client_secret=${encodeURIComponent(config.clientSecret)}`);
    }
    if (config.useIdToken) {
      fragmentParts.push("use_id_token=true");
    }
    const redirectUrl = `${unpacked.returnTo}${separator}${fragmentParts.join("&")}`;

    return new Response(null, {
      status: 302,
      headers: {
        Location: redirectUrl,
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Content-Type": "text/html; charset=utf-8",
        "Set-Cookie": clearSessionCookie,
      },
    });
  }

  // Same-origin: redirect to original page with cookies
  const originalUrl = validateOriginalUrl(unpacked.originalUrl, config.prefix);
  const cookiePath = config.prefix || "/";

  const authCookie = buildSetCookieHeader(AUTH_COOKIE_NAME, result.token, {
    maxAge: result.maxAge,
    path: cookiePath,
    secure: config.secureCookie,
    httpOnly: false,
    sameSite: "Lax",
  });

  // Response with two Set-Cookie headers
  const headers = new Headers();
  headers.set("Location", originalUrl);
  headers.set("Cache-Control", "no-cache, no-store, must-revalidate");
  headers.set("Content-Type", "text/html; charset=utf-8");
  headers.append("Set-Cookie", authCookie);
  headers.append("Set-Cookie", clearSessionCookie);

  return new Response(null, { status: 302, headers });
}

// ---------------------------------------------------------------------------
// OAuth logout handler
// ---------------------------------------------------------------------------

/** Handle GET {prefix}/_oauth/logout — clear auth cookie and redirect. */
export function handleOAuthLogout(_request: Request, config: OAuthPkceConfig): Response {
  const cookiePath = config.prefix || "/";
  const clearAuthCookie = buildSetCookieHeader(AUTH_COOKIE_NAME, "", {
    maxAge: 0,
    path: cookiePath,
    secure: config.secureCookie,
    httpOnly: false,
  });
  return new Response(null, {
    status: 302,
    headers: {
      Location: config.prefix || "/",
      "Set-Cookie": clearAuthCookie,
    },
  });
}

// ---------------------------------------------------------------------------
// Browser GET redirect (replaces 401 with OAuth redirect for browsers)
// ---------------------------------------------------------------------------

/** Redirect an unauthenticated browser GET to the OAuth authorization endpoint. Returns null if unable. */
export async function handleBrowserGetRedirect(request: Request, config: OAuthPkceConfig): Promise<Response | null> {
  // Only redirect browsers (Accept: text/html)
  const accept = request.headers.get("Accept") ?? "";
  if (!accept.includes("text/html")) return null;

  // Discover authorization endpoint
  const endpoints = await config.oidcDiscovery();
  if (!endpoints) return null;

  const url = new URL(request.url);

  // Generate PKCE parameters
  const codeVerifier = generateCodeVerifier();
  const codeChallenge = generateCodeChallenge(codeVerifier);
  const stateNonce = generateStateNonce();

  // Capture original URL
  let originalUrl = url.pathname;
  if (url.search) {
    originalUrl = `${originalUrl}${url.search}`;
  }
  originalUrl = validateOriginalUrl(originalUrl, config.prefix);

  // Check for external frontend return URL
  const returnTo = validateReturnTo(url.searchParams.get("_vgi_return_to") ?? "", config.allowedReturnOrigins);

  // Pack session cookie
  const cookieValue = packOAuthCookie(codeVerifier, stateNonce, originalUrl, config.sessionKey, undefined, returnTo);

  // Build authorization URL
  const authParams = new URLSearchParams({
    response_type: "code",
    client_id: config.clientId,
    redirect_uri: config.redirectUri,
    code_challenge: codeChallenge,
    code_challenge_method: "S256",
    state: stateNonce,
    scope: config.scope,
  });
  // When redirecting to an external frontend, request offline access
  if (returnTo) {
    authParams.set("access_type", "offline");
    authParams.set("prompt", "consent");
  }
  const authUrl = `${endpoints.authorizationEndpoint}?${authParams.toString()}`;

  const sessionCookie = buildSetCookieHeader(SESSION_COOKIE_NAME, cookieValue, {
    maxAge: SESSION_MAX_AGE,
    path: `${config.prefix}/_oauth/`,
    secure: config.secureCookie,
    httpOnly: true,
    sameSite: "Lax",
  });

  return new Response(null, {
    status: 302,
    headers: {
      Location: authUrl,
      "Cache-Control": "no-cache, no-store, must-revalidate",
      "Content-Type": "text/html; charset=utf-8",
      "Set-Cookie": sessionCookie,
    },
  });
}

// ---------------------------------------------------------------------------
// Early return-to redirect (authenticated user with _vgi_return_to)
// ---------------------------------------------------------------------------

/** If user is already authenticated and has _vgi_return_to, redirect immediately. Returns null otherwise. */
export function handleEarlyReturnTo(request: Request, config: OAuthPkceConfig): Response | null {
  const url = new URL(request.url);
  const returnTo = validateReturnTo(url.searchParams.get("_vgi_return_to") ?? "", config.allowedReturnOrigins);
  if (!returnTo) return null;

  // Check for existing auth token in cookie
  const cookies = parseCookies(request);
  const token = cookies.get(AUTH_COOKIE_NAME);
  if (!token) return null;

  // Don't redirect with an expired token — let the OAuth flow run again
  try {
    const parts = token.split(".");
    if (parts.length >= 2) {
      const payload = JSON.parse(Buffer.from(parts[1], "base64url").toString("utf-8"));
      if (typeof payload.exp === "number" && payload.exp <= Math.floor(Date.now() / 1000)) {
        return null;
      }
    }
  } catch {
    /* not a JWT or can't decode — proceed with redirect */
  }

  // Already authenticated with a return_to — redirect back with the token
  const separator = returnTo.includes("#") ? "&" : "#";
  const fragmentParams = [`token=${token}`];
  const redirectUrl = `${returnTo}${separator}${fragmentParams.join("&")}`;

  return new Response(null, {
    status: 302,
    headers: {
      Location: redirectUrl,
      "Cache-Control": "no-cache, no-store, must-revalidate",
    },
  });
}
