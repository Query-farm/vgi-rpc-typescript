// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Guards on the token-introspection endpoint that the cross-language
 * conformance group structurally cannot reach.
 *
 * The shared suite drives a live worker over HTTP, so it can only see what a
 * caller sees: it can prove the credential never appears in a *response*, but
 * not that it never reaches a log record, and it can prove a rejection is
 * definitive but not that an *outage* is distinguishable from one. Both are
 * asserted here, along with the construction-time refusals a running worker
 * never gets to exhibit.
 */

import { afterEach, describe, expect, test } from "bun:test";
import { AuthContext } from "../../src/auth.js";
import { chainAuthenticate } from "../../src/http/bearer.js";
import { createHttpHandler } from "../../src/http/index.js";
import {
  createIntrospector,
  INTROSPECT_ENABLED_HEADER,
  INTROSPECT_ENDPOINT,
  type TokenIdentity,
  tokenDigest,
} from "../../src/http/introspect.js";
import { AuthFailure, AuthReason, AuthUnavailableError } from "../../src/http/unauthorized.js";
import { float, Protocol } from "../../src/index.js";

const BASE = "http://localhost";
const INTROSPECTOR = "proxy@example";
const SUBJECT_TOKEN = "opaque-subject-credential-do-not-log";
const SUBJECT_PRINCIPAL = "alice@example.com";

function makeProtocol(): Protocol {
  return new Protocol("TestIntrospect").unary("add", {
    params: { a: float, b: float },
    result: { result: float },
    handler: async ({ a, b }) => ({ result: a + b }),
  });
}

/** Resolve exactly one credential; everything else is unknown. */
function resolver(credential: string): TokenIdentity | null {
  return credential === SUBJECT_TOKEN ? { principal: SUBJECT_PRINCIPAL, tokenName: "laptop" } : null;
}

/** Name yourself in a header — enough to give the allowlist something to check. */
function principalAuth(request: Request): AuthContext {
  const principal = request.headers.get("X-Principal");
  return principal ? new AuthContext("test", true, principal) : AuthContext.anonymous();
}

function introspectRequest(token: string, caller: string | null = INTROSPECTOR): Request {
  return new Request(`${BASE}/vgi${INTROSPECT_ENDPOINT}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(caller ? { "X-Principal": caller } : {}),
    },
    body: JSON.stringify({ token }),
  });
}

function enabledHandler() {
  return createHttpHandler(makeProtocol(), {
    prefix: "/vgi",
    authenticate: principalAuth,
    introspectResolver: resolver,
    introspectPrincipals: [INTROSPECTOR],
  });
}

// ---------------------------------------------------------------------------
// The credential never reaches a log record
// ---------------------------------------------------------------------------

const CONSOLE_METHODS = ["log", "info", "warn", "error", "debug"] as const;
type CapturedConsole = { lines: string[]; restore(): void };

/** Capture everything the code under test writes to `console`, flattened to text. */
function captureConsole(): CapturedConsole {
  const lines: string[] = [];
  const originals = CONSOLE_METHODS.map((name) => [name, console[name]] as const);
  for (const [name] of originals) {
    console[name] = (...args: unknown[]) => {
      lines.push(args.map((a) => (typeof a === "string" ? a : JSON.stringify(a))).join(" "));
    };
  }
  return {
    lines,
    restore() {
      for (const [name, fn] of originals) console[name] = fn;
    },
  };
}

let captured: CapturedConsole | null = null;
afterEach(() => {
  captured?.restore();
  captured = null;
});

describe("introspection never logs the credential", () => {
  test("neither a resolution, an unknown credential, nor a JWS refusal echoes it", async () => {
    const handler = enabledHandler();
    const jws = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbGljZSJ9.c2lnbmF0dXJl";
    captured = captureConsole();
    for (const token of [SUBJECT_TOKEN, "guessed-credential", jws]) {
      await handler(introspectRequest(token));
    }
    const log = captured.lines.join("\n");
    captured.restore();

    // The whole point of the guard: a credential in a log record is a
    // credential in whatever ships those records.
    expect(log).not.toContain(SUBJECT_TOKEN);
    expect(log).not.toContain("guessed-credential");
    expect(log).not.toContain(jws);
    // A digest is what makes the record still useful — one credential's
    // failures correlate across records without the record being the credential.
    expect(log).toContain(await tokenDigest(SUBJECT_TOKEN));
    expect(log).toContain(await tokenDigest(jws));
  });

  test("a refused caller's log names the caller, not the credential", async () => {
    const handler = enabledHandler();
    captured = captureConsole();
    await handler(introspectRequest(SUBJECT_TOKEN, "someone-else"));
    const log = captured.lines.join("\n");
    captured.restore();

    expect(log).toContain("someone-else");
    expect(log).not.toContain(SUBJECT_TOKEN);
    // Refused before the credential is read at all, so there is not even a
    // digest to correlate.
    expect(log).not.toContain(await tokenDigest(SUBJECT_TOKEN));
  });
});

// ---------------------------------------------------------------------------
// Definitive vs transient
// ---------------------------------------------------------------------------

describe("AuthUnavailableError is not a credential rejection", () => {
  test("chainAuthenticate propagates it instead of advancing to the next authenticator", async () => {
    let secondRan = false;
    const chained = chainAuthenticate(
      () => {
        throw new AuthUnavailableError("token sidecar unreachable");
      },
      () => {
        secondRan = true;
        return AuthContext.anonymous();
      },
    );

    await expect(chained(new Request(`${BASE}/vgi/add`, { method: "POST" }))).rejects.toThrow(AuthUnavailableError);
    // Advancing would swallow the outage and emerge as a 401 from the end of
    // the chain — a sidecar restart turning into a fleet-wide re-login storm.
    expect(secondRan).toBe(false);
  });

  test("an AuthFailure still advances, so classifying a refusal keeps composing", async () => {
    let secondRan = false;
    const chained = chainAuthenticate(
      () => {
        throw new AuthFailure(AuthReason.InvalidCredential, "not mine");
      },
      () => {
        secondRan = true;
        return new AuthContext("test", true, "bob");
      },
    );

    const ctx = await chained(new Request(`${BASE}/vgi/add`, { method: "POST" }));
    expect(secondRan).toBe(true);
    expect(ctx.principal).toBe("bob");
  });

  test("the handler renders it as 503 + Retry-After, never 401", async () => {
    const handler = createHttpHandler(makeProtocol(), {
      prefix: "/vgi",
      authenticate: () => {
        throw new AuthUnavailableError("token sidecar unreachable", 7);
      },
    });

    const res = await handler(
      new Request(`${BASE}/vgi/add`, { method: "POST", headers: { "Content-Type": "application/json" } }),
    );

    // 401 would tell every caller to re-authenticate against a service that is
    // simply down, and invite them to negative-cache an outage.
    expect(res.status).toBe(503);
    expect(res.headers.get("Retry-After")).toBe("7");
    expect(res.headers.get("Cache-Control")).toBe("no-store");
    expect(await res.json()).toEqual({
      error: "authentication_unavailable",
      detail: "authentication authority unavailable",
    });
  });

  test("a RESOLVER outage is 503 too, not the endpoint's definitive 404", async () => {
    // The case above covers the `authenticate` path. This one covers the
    // resolver's, which is where it is easier to get wrong and more expensive:
    // the endpoint's own "did not resolve" is 404, and 404 is exactly the answer
    // a caller may negative-cache — so borrowing it for an unreachable backing
    // store has the caller remember a live credential as bad for the cache's
    // lifetime. Caught by the cross-language conformance group, which this
    // implementation failed with a bare 500 (the documented
    // `AuthUnavailableError` was never caught).
    const handler = createHttpHandler(makeProtocol(), {
      prefix: "/vgi",
      authenticate: principalAuth,
      introspectResolver: () => {
        throw new AuthUnavailableError("mapping store unreachable", 9);
      },
      introspectPrincipals: [INTROSPECTOR],
    });

    captured = captureConsole();
    const res = await handler(introspectRequest(SUBJECT_TOKEN));
    const log = captured.lines.join("\n");
    captured.restore();

    expect(res.status).toBe(503);
    expect(res.headers.get("Retry-After")).toBe("9");
    expect(await res.text()).not.toContain(SUBJECT_TOKEN);
    expect(log).not.toContain("mapping store unreachable");
  });

  test("a resolver returning null is still the definitive 404", async () => {
    // The distinction only pays if the ordinary refusal is unchanged.
    const res = await enabledHandler()(introspectRequest("no-such-credential"));
    expect(res.status).toBe(404);
    expect(await res.json()).toEqual({ error: "unresolved" });
    expect(res.headers.get("Cache-Control")).toBe("no-store");
  });
});

// ---------------------------------------------------------------------------
// Configuration refusals — a running worker never gets to exhibit these
// ---------------------------------------------------------------------------

describe("the introspector allowlist has no permissive default", () => {
  test("an omitted allowlist is refused at construction", () => {
    expect(() => createIntrospector({ resolver, principals: undefined })).toThrow(/at least one principal/);
  });

  test("an empty allowlist is refused at construction", () => {
    expect(() => createIntrospector({ resolver, principals: ["", "  ".trim()] })).toThrow(/at least one principal/);
  });

  test("a resolver without an allowlist cannot reach the handler", () => {
    expect(() =>
      createHttpHandler(makeProtocol(), { prefix: "/vgi", authenticate: principalAuth, introspectResolver: resolver }),
    ).toThrow(/at least one principal/);
  });

  test("an allowlist without a resolver is refused rather than silently ignored", () => {
    expect(() => createHttpHandler(makeProtocol(), { prefix: "/vgi", introspectPrincipals: [INTROSPECTOR] })).toThrow(
      /without introspectResolver/,
    );
  });

  test("a non-finite TTL is refused: it silently disables the caller's cache", () => {
    expect(() => createIntrospector({ resolver, principals: [INTROSPECTOR], ttlSeconds: Number.NaN })).toThrow(
      /finite, positive/,
    );
    expect(() => createIntrospector({ resolver, principals: [INTROSPECTOR], ttlSeconds: 0 })).toThrow(
      /finite, positive/,
    );
  });
});

// ---------------------------------------------------------------------------
// Advertisement, and the CORS exposure that makes it readable
// ---------------------------------------------------------------------------

describe("capability advertisement", () => {
  test("absent — never 'false' — when introspection is off", async () => {
    const handler = createHttpHandler(makeProtocol(), { prefix: "/vgi", corsOrigins: "*" });
    const res = await handler(new Request(`${BASE}/vgi/health`, { method: "OPTIONS" }));
    expect(res.headers.get(INTROSPECT_ENABLED_HEADER)).toBeNull();
    expect(res.headers.get("Access-Control-Expose-Headers")).not.toContain(INTROSPECT_ENABLED_HEADER);
  });

  test("advertised and exposed when it is on", async () => {
    const handler = createHttpHandler(makeProtocol(), {
      prefix: "/vgi",
      corsOrigins: "*",
      authenticate: principalAuth,
      introspectResolver: resolver,
      introspectPrincipals: [INTROSPECTOR],
    });
    const res = await handler(new Request(`${BASE}/vgi/health`, { method: "OPTIONS" }));

    expect(res.headers.get(INTROSPECT_ENABLED_HEADER)).toBe("true");
    // A header a browser client cannot read is a capability it cannot preflight.
    expect(res.headers.get("Access-Control-Expose-Headers")).toContain(INTROSPECT_ENABLED_HEADER);
  });
});

// ---------------------------------------------------------------------------
// Response shape and the off-mode contract
// ---------------------------------------------------------------------------

describe("response shape", () => {
  test("a resolution carries exactly the closed set of three keys", async () => {
    // Captured only to keep the resolution's audit line out of the test output.
    captured = captureConsole();
    const res = await enabledHandler()(introspectRequest(SUBJECT_TOKEN));
    captured.restore();
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    // Asserted as a set so a future addition — a claims passthrough above all —
    // has to come through this test.
    expect(Object.keys(body).sort()).toEqual(["principal", "token_name", "ttl_seconds"]);
    expect(body.principal).toBe(SUBJECT_PRINCIPAL);
    expect(body.ttl_seconds).toBe(300);
    expect(res.headers.get("Cache-Control")).toBe("no-store");
  });

  test("an oversized body is refused without being parsed", async () => {
    const res = await enabledHandler()(introspectRequest("x".repeat(9000)));
    expect(res.status).toBe(404);
    expect(await res.json()).toEqual({ error: "unresolved" });
  });

  test("a disabled worker answers a definitive 404, not the generic 415", async () => {
    const handler = createHttpHandler(makeProtocol(), { prefix: "/vgi" });
    const res = await handler(introspectRequest(SUBJECT_TOKEN, null));
    // 415 reads as transient to a caller classifying 401/403/404 as definitive,
    // so pointing a proxy at a worker without the feature would retry forever.
    expect(res.status).toBe(404);
    expect(await res.json()).toEqual({ error: "not_enabled" });
  });

  test("a disabled worker answers before authentication, so no credential is needed", async () => {
    const handler = createHttpHandler(makeProtocol(), {
      prefix: "/vgi",
      authenticate: () => {
        throw new AuthFailure(AuthReason.MissingCredential, "reject-all");
      },
    });
    const res = await handler(introspectRequest(SUBJECT_TOKEN, null));
    expect(res.status).toBe(404);
    expect(await res.json()).toEqual({ error: "not_enabled" });
  });

  test("an unrouted JSON POST is a 404 rather than a 415", async () => {
    // The same classification failure one path over: a caller that treats
    // anything outside 401/403/404 as transient must not be handed a 415 for a
    // path this worker simply does not serve.
    const handler = createHttpHandler(makeProtocol(), { prefix: "" });
    const res = await handler(
      new Request(`${BASE}/vgi${INTROSPECT_ENDPOINT}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ token: "anything" }),
      }),
    );
    expect(res.status).toBe(404);
  });
});
