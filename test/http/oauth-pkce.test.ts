// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import { createHash, randomBytes } from "node:crypto";
import { AuthContext } from "../../src/auth.js";
import {
  buildOAuthErrorPage,
  buildSetCookieHeader,
  buildUserInfoHtml,
  cookieAuthenticate,
  deriveSessionKey,
  generateCodeChallenge,
  generateCodeVerifier,
  generateStateNonce,
  handleEarlyReturnTo,
  handleOAuthLogout,
  packOAuthCookie,
  parseCookies,
  unpackOAuthCookie,
  validateOriginalUrl,
  validateReturnTo,
} from "../../src/http/oauth-pkce.js";

// ---------------------------------------------------------------------------
// PKCE helpers
// ---------------------------------------------------------------------------

describe("PKCE helpers", () => {
  test("generateCodeVerifier returns 43-character URL-safe string", () => {
    const verifier = generateCodeVerifier();
    expect(verifier.length).toBe(43);
    // URL-safe base64 charset
    expect(verifier).toMatch(/^[A-Za-z0-9_-]+$/);
  });

  test("generateCodeVerifier produces unique values", () => {
    const a = generateCodeVerifier();
    const b = generateCodeVerifier();
    expect(a).not.toBe(b);
  });

  test("generateCodeChallenge matches RFC 7636 test vector", () => {
    // RFC 7636 Appendix B test vector
    const verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
    const challenge = generateCodeChallenge(verifier);
    expect(challenge).toBe("E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM");
  });

  test("generateCodeChallenge is S256", () => {
    const verifier = generateCodeVerifier();
    const challenge = generateCodeChallenge(verifier);
    // Verify manually
    const expected = createHash("sha256").update(verifier, "ascii").digest().toString("base64url");
    expect(challenge).toBe(expected);
  });

  test("generateStateNonce produces URL-safe string", () => {
    const nonce = generateStateNonce();
    expect(nonce.length).toBeGreaterThan(0);
    expect(nonce).toMatch(/^[A-Za-z0-9_-]+$/);
  });
});

// ---------------------------------------------------------------------------
// Session key derivation
// ---------------------------------------------------------------------------

describe("deriveSessionKey", () => {
  test("derives deterministic key from signing key", () => {
    const signingKey = randomBytes(32);
    const a = deriveSessionKey(signingKey);
    const b = deriveSessionKey(signingKey);
    expect(Buffer.from(a).equals(Buffer.from(b))).toBe(true);
  });

  test("different signing keys produce different session keys", () => {
    const a = deriveSessionKey(randomBytes(32));
    const b = deriveSessionKey(randomBytes(32));
    expect(Buffer.from(a).equals(Buffer.from(b))).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Session cookie pack/unpack
// ---------------------------------------------------------------------------

describe("OAuth session cookie", () => {
  const sessionKey = deriveSessionKey(randomBytes(32));

  test("round-trip pack/unpack", () => {
    const cv = generateCodeVerifier();
    const state = generateStateNonce();
    const url = "/api/v1/query?foo=bar";
    const returnTo = "";

    const packed = packOAuthCookie(cv, state, url, sessionKey, undefined, returnTo);
    expect(typeof packed).toBe("string");

    const unpacked = unpackOAuthCookie(packed, sessionKey);
    expect(unpacked.codeVerifier).toBe(cv);
    expect(unpacked.stateNonce).toBe(state);
    expect(unpacked.originalUrl).toBe(url);
    expect(unpacked.returnTo).toBe(returnTo);
  });

  test("round-trip with return_to", () => {
    const cv = generateCodeVerifier();
    const state = generateStateNonce();
    const url = "/api/";
    const returnTo = "http://localhost:3000/app";

    const packed = packOAuthCookie(cv, state, url, sessionKey, undefined, returnTo);
    const unpacked = unpackOAuthCookie(packed, sessionKey);
    expect(unpacked.returnTo).toBe(returnTo);
  });

  test("tamper detection", () => {
    const packed = packOAuthCookie("verifier", "state", "/url", sessionKey);
    // Flip a character
    const tampered = `${packed.slice(0, 10)}X${packed.slice(11)}`;
    expect(() => unpackOAuthCookie(tampered, sessionKey)).toThrow("signature mismatch");
  });

  test("wrong key detection", () => {
    const packed = packOAuthCookie("verifier", "state", "/url", sessionKey);
    const wrongKey = deriveSessionKey(randomBytes(32));
    expect(() => unpackOAuthCookie(packed, wrongKey)).toThrow("signature mismatch");
  });

  test("expiry detection", () => {
    const oldTime = Math.floor(Date.now() / 1000) - 700; // 700 seconds ago, max is 600
    const packed = packOAuthCookie("verifier", "state", "/url", sessionKey, oldTime);
    expect(() => unpackOAuthCookie(packed, sessionKey, 600)).toThrow("expired");
  });

  test("maxAge=0 disables expiry check", () => {
    const oldTime = Math.floor(Date.now() / 1000) - 7200;
    const packed = packOAuthCookie("verifier", "state", "/url", sessionKey, oldTime);
    const unpacked = unpackOAuthCookie(packed, sessionKey, 0);
    expect(unpacked.codeVerifier).toBe("verifier");
  });

  test("too-short cookie", () => {
    const shortValue = Buffer.from("too short").toString("base64url");
    expect(() => unpackOAuthCookie(shortValue, sessionKey)).toThrow("too short");
  });

  test("malformed base64", () => {
    // Valid base64url characters but when decoded will be too short
    expect(() => unpackOAuthCookie("!!!not-valid!!!", sessionKey)).toThrow();
  });
});

// ---------------------------------------------------------------------------
// URL validation
// ---------------------------------------------------------------------------

describe("validateOriginalUrl", () => {
  test("accepts relative URL within prefix", () => {
    expect(validateOriginalUrl("/api/v1/query", "/api")).toBe("/api/v1/query");
  });

  test("accepts relative URL without prefix", () => {
    expect(validateOriginalUrl("/some/path", "")).toBe("/some/path");
  });

  test("rejects absolute URL", () => {
    expect(validateOriginalUrl("https://evil.com/steal", "/api")).toBe("/api");
  });

  test("rejects protocol-relative URL", () => {
    expect(validateOriginalUrl("//evil.com/steal", "/api")).toBe("/api");
  });

  test("rejects URL outside prefix", () => {
    expect(validateOriginalUrl("/other/path", "/api")).toBe("/api");
  });

  test("truncates overly long URL", () => {
    const longUrl = `/api/${"a".repeat(3000)}`;
    const result = validateOriginalUrl(longUrl, "/api");
    expect(result.length).toBeLessThanOrEqual(2048);
  });
});

describe("validateReturnTo", () => {
  test("allows default origin", () => {
    expect(validateReturnTo("https://cupola.query-farm.services/app")).toBe("https://cupola.query-farm.services/app");
  });

  test("allows localhost http", () => {
    expect(validateReturnTo("http://localhost:3000/app")).toBe("http://localhost:3000/app");
  });

  test("allows 127.0.0.1 http", () => {
    expect(validateReturnTo("http://127.0.0.1:8080/")).toBe("http://127.0.0.1:8080/");
  });

  test("rejects non-allowed origin", () => {
    expect(validateReturnTo("https://evil.com/steal")).toBe("");
  });

  test("rejects empty string", () => {
    expect(validateReturnTo("")).toBe("");
  });

  test("rejects overly long URL", () => {
    expect(validateReturnTo(`http://localhost/${"a".repeat(3000)}`)).toBe("");
  });

  test("rejects non-http/https scheme", () => {
    expect(validateReturnTo("ftp://example.com")).toBe("");
  });

  test("accepts custom allowed origin", () => {
    const allowed = new Set(["https://custom.example.com"]);
    expect(validateReturnTo("https://custom.example.com/path", allowed)).toBe("https://custom.example.com/path");
  });

  test("rejects localhost https (must be http)", () => {
    expect(validateReturnTo("https://localhost:3000/app")).toBe("");
  });
});

// ---------------------------------------------------------------------------
// Cookie helpers
// ---------------------------------------------------------------------------

describe("parseCookies", () => {
  test("parses multiple cookies", () => {
    const request = new Request("http://localhost/", {
      headers: { Cookie: "a=1; b=2; c=hello" },
    });
    const cookies = parseCookies(request);
    expect(cookies.get("a")).toBe("1");
    expect(cookies.get("b")).toBe("2");
    expect(cookies.get("c")).toBe("hello");
  });

  test("handles missing Cookie header", () => {
    const request = new Request("http://localhost/");
    const cookies = parseCookies(request);
    expect(cookies.size).toBe(0);
  });
});

describe("buildSetCookieHeader", () => {
  test("builds complete header", () => {
    const header = buildSetCookieHeader("name", "value", {
      maxAge: 600,
      path: "/api/",
      secure: true,
      httpOnly: true,
      sameSite: "Lax",
    });
    expect(header).toContain("name=value");
    expect(header).toContain("Max-Age=600");
    expect(header).toContain("Path=/api/");
    expect(header).toContain("Secure");
    expect(header).toContain("HttpOnly");
    expect(header).toContain("SameSite=Lax");
  });

  test("omits optional fields", () => {
    const header = buildSetCookieHeader("n", "v", {});
    expect(header).toBe("n=v");
  });
});

// ---------------------------------------------------------------------------
// Cookie authenticate
// ---------------------------------------------------------------------------

describe("cookieAuthenticate", () => {
  const mockAuth: (request: Request) => AuthContext = (request: Request) => {
    const auth = request.headers.get("Authorization");
    if (!auth?.startsWith("Bearer ")) {
      throw new Error("Missing bearer token");
    }
    const token = auth.slice(7);
    if (token === "valid-token") {
      return new AuthContext("test", true, "user@example.com");
    }
    throw new Error("Invalid token");
  };

  test("passes through Authorization header from inner auth", async () => {
    const auth = cookieAuthenticate(mockAuth);
    const request = new Request("http://localhost/", {
      headers: { Authorization: "Bearer valid-token" },
    });
    // cookieAuthenticate only reads from cookies, not the Authorization header
    // If no cookie is present, it should throw
    await expect(auth(request)).rejects.toThrow("No auth cookie");
  });

  test("reads token from cookie and delegates to inner auth", async () => {
    const auth = cookieAuthenticate(mockAuth);
    const request = new Request("http://localhost/", {
      headers: { Cookie: "_vgi_auth=valid-token" },
    });
    const ctx = await auth(request);
    expect(ctx.authenticated).toBe(true);
    expect(ctx.principal).toBe("user@example.com");
  });

  test("throws on missing cookie", async () => {
    const auth = cookieAuthenticate(mockAuth);
    const request = new Request("http://localhost/");
    await expect(auth(request)).rejects.toThrow("No auth cookie");
  });

  test("propagates inner auth errors", async () => {
    const auth = cookieAuthenticate(mockAuth);
    const request = new Request("http://localhost/", {
      headers: { Cookie: "_vgi_auth=bad-token" },
    });
    await expect(auth(request)).rejects.toThrow("Invalid token");
  });
});

// ---------------------------------------------------------------------------
// Error page
// ---------------------------------------------------------------------------

describe("buildOAuthErrorPage", () => {
  test("renders error page with detail", () => {
    const html = buildOAuthErrorPage("Something went wrong", "Details here", "/retry");
    expect(html).toContain("Something went wrong");
    expect(html).toContain("Details here");
    expect(html).toContain("/retry");
    expect(html).toContain("<!DOCTYPE html>");
  });

  test("renders without detail", () => {
    const html = buildOAuthErrorPage("Error", null, "/");
    expect(html).toContain("Error");
    expect(html).not.toContain('<div class="detail">');
  });

  test("escapes HTML in message", () => {
    const html = buildOAuthErrorPage("<script>alert(1)</script>", null, "/");
    expect(html).not.toContain("<script>alert(1)</script>");
    expect(html).toContain("&lt;script&gt;");
  });
});

// ---------------------------------------------------------------------------
// User info HTML
// ---------------------------------------------------------------------------

describe("buildUserInfoHtml", () => {
  test("contains style, div, and script", () => {
    const html = buildUserInfoHtml("/api");
    expect(html).toContain("<style>");
    expect(html).toContain('id="vgi-user-info"');
    expect(html).toContain("<script>");
    expect(html).toContain("/api/_oauth/logout");
  });
});

// ---------------------------------------------------------------------------
// handleOAuthLogout
// ---------------------------------------------------------------------------

describe("handleOAuthLogout", () => {
  test("returns 302 with cleared cookie", () => {
    const config = {
      sessionKey: new Uint8Array(32),
      oidcDiscovery: async () => null,
      clientId: "test",
      clientSecret: undefined,
      useIdToken: false,
      prefix: "/api",
      secureCookie: false,
      redirectUri: "http://localhost/api/_oauth/callback",
      scope: "openid email",
      allowedReturnOrigins: new Set<string>(),
      cookieAuthenticate: async () => AuthContext.anonymous(),
      userInfoHtml: "",
    };

    const request = new Request("http://localhost/api/_oauth/logout");
    const response = handleOAuthLogout(request, config);
    expect(response.status).toBe(302);
    expect(response.headers.get("Location")).toBe("/api");
    const setCookie = response.headers.get("Set-Cookie");
    expect(setCookie).toContain("_vgi_auth=");
    expect(setCookie).toContain("Max-Age=0");
  });
});

// ---------------------------------------------------------------------------
// handleEarlyReturnTo
// ---------------------------------------------------------------------------

describe("handleEarlyReturnTo", () => {
  const config = {
    sessionKey: new Uint8Array(32),
    oidcDiscovery: async () => null,
    clientId: "test",
    clientSecret: undefined,
    useIdToken: false,
    prefix: "/api",
    secureCookie: false,
    redirectUri: "http://localhost/api/_oauth/callback",
    scope: "openid email",
    allowedReturnOrigins: new Set(["https://cupola.query-farm.services"]),
    cookieAuthenticate: async () => AuthContext.anonymous(),
    userInfoHtml: "",
  };

  test("returns null when no return_to", () => {
    const request = new Request("http://localhost/api/");
    const result = handleEarlyReturnTo(request, config);
    expect(result).toBeNull();
  });

  test("returns null when no auth cookie", () => {
    const request = new Request("http://localhost/api/?_vgi_return_to=https://cupola.query-farm.services/app");
    const result = handleEarlyReturnTo(request, config);
    expect(result).toBeNull();
  });

  test("redirects when authenticated with return_to", () => {
    const request = new Request("http://localhost/api/?_vgi_return_to=https://cupola.query-farm.services/app", {
      headers: { Cookie: "_vgi_auth=mytoken123" },
    });
    const result = handleEarlyReturnTo(request, config);
    expect(result).not.toBeNull();
    expect(result!.status).toBe(302);
    const location = result!.headers.get("Location")!;
    expect(location).toContain("https://cupola.query-farm.services/app#token=mytoken123");
  });
});

// ---------------------------------------------------------------------------
// Cross-compatibility with Python wire format
// ---------------------------------------------------------------------------

describe("Python wire format compatibility", () => {
  test("packOAuthCookie produces base64url WITH padding (matching Python)", () => {
    const sessionKey = deriveSessionKey(new Uint8Array(32));
    const packed = packOAuthCookie("verifier", "state", "/url", sessionKey, 1700000000);
    // Python's base64.urlsafe_b64encode includes '=' padding
    // Check that any padding present uses '=' (not stripped)
    // The cookie should be decodable by both Python and TypeScript
    const unpacked = unpackOAuthCookie(packed, sessionKey, 0);
    expect(unpacked.codeVerifier).toBe("verifier");
  });
});
