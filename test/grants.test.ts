// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Sealed grants (IDENTITY_V1_SPEC.md §9), driven by the reference's
 * `vgi_rpc/conformance/grant_token_vectors.json` -- the file every port mints
 * and verifies against byte for byte -- plus the authenticator chain of §9.3
 * and the end-to-end loop over the HTTP handler.
 */

import { describe, expect, test } from "bun:test";
import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { AuthContext } from "../src/auth.js";
import {
  buildSecondaryProtocol,
  buildWhoamiProtocol,
  SECONDARY_PROTOCOL_HASH,
  WHOAMI_PROTOCOL_HASH,
} from "../src/conformance/index.js";
import {
  GRANT_KEYS_ENV,
  GRANT_TOKEN_PREFIX,
  GrantInvalidError,
  GrantKeys,
  grantKeyId,
  mintGrantToken,
  verifyGrantToken,
} from "../src/grants.js";
import { composeIdentityAuthenticate, GrantRejectedError, grantAuthenticate } from "../src/http/grant-auth.js";
import { AuthUnavailableError } from "../src/http/unauthorized.js";
import { protocolHashFor } from "../src/reflection.js";
import { buildIdentityProtocol, IdentityImpl, IdentityUnavailableError } from "../src/token-identity.js";
import { PYTHON_BIN, REFERENCE_HOME } from "./reference.js";

// ---------------------------------------------------------------------------
// The vectors: from the reference checkout, else from the installed package.
// ---------------------------------------------------------------------------

function vectorsPath(): string {
  const local = join(REFERENCE_HOME, "vgi_rpc", "conformance", "grant_token_vectors.json");
  if (existsSync(local)) return local;
  const located = execFileSync(
    PYTHON_BIN,
    [
      "-c",
      "import pathlib, vgi_rpc.conformance as c; print(pathlib.Path(c.__file__).parent / 'grant_token_vectors.json')",
    ],
    { encoding: "utf8", timeout: 20_000 },
  ).trim();
  if (!existsSync(located)) throw new Error(`grant_token_vectors.json not found (looked at ${local} and ${located})`);
  return located;
}

const V = JSON.parse(readFileSync(vectorsPath(), "utf8"));

const b64 = (s: string) => Uint8Array.from(atob(s), (c) => c.charCodeAt(0));
const hex = (s: string) => Uint8Array.from(s.match(/../g) ?? [], (h) => Number.parseInt(h, 16));
const toHex = (b: Uint8Array) => [...b].map((x) => x.toString(16).padStart(2, "0")).join("");

function verifierFor(c: Record<string, any>): { keys: GrantKeys; now: number } {
  const d = V.defaults;
  return {
    keys: new GrantKeys((c.verify_keys_b64 ?? d.verify_keys_b64).map(b64), {
      audience: c.audience ?? d.audience,
      maxTtlSeconds: c.max_ttl_seconds ?? d.max_ttl_seconds,
      clockSkewSeconds: c.clock_skew_seconds ?? d.clock_skew_seconds,
    }),
    now: c.now ?? d.now,
  };
}

describe("grant_token_vectors.json", () => {
  test("the file has every section", () => {
    expect(V.mint.length).toBeGreaterThan(0);
    expect(V.accept.length).toBeGreaterThan(0);
    expect(V.reject.length).toBeGreaterThan(0);
  });

  for (const c of V.mint) {
    test(`mint: ${c.name} reproduces the exact token`, async () => {
      const keys = new GrantKeys([b64(c.minting_key_b64)], {
        audience: c.audience,
        maxTtlSeconds: c.max_ttl_seconds,
      });
      expect(toHex(await grantKeyId(keys.keys[0]))).toBe(c.kid_hex);
      expect(toHex(keys.aad(hex(c.kid_hex)))).toBe(c.aad_hex);
      const { token, claims } = await mintGrantToken(keys, {
        principal: c.request.principal,
        scopes: c.request.scopes,
        purpose: c.request.purpose,
        ttlSeconds: c.request.ttl_seconds,
        grantId: c.request.grant_id,
        now: c.now,
        nonce: hex(c.nonce_hex),
      });
      expect(token).toBe(c.token);
      expect({
        principal: claims.principal,
        scopes: claims.scopes,
        purpose: claims.purpose,
        grant_id: claims.grantId,
        issued_at: claims.issuedAt,
        expires_at: claims.expiresAt,
      }).toEqual(c.claims);
      // And it verifies under the same configuration.
      const opened = await verifyGrantToken(keys, token, c.now + 1);
      expect(opened.principal).toBe(c.claims.principal);
    });
  }

  for (const c of V.accept) {
    test(`accept: ${c.name}`, async () => {
      const { keys, now } = verifierFor(c);
      const claims = await verifyGrantToken(keys, c.token, now);
      expect(claims.principal.length).toBeGreaterThan(0);
    });
  }

  for (const c of V.reject) {
    test(`reject: ${c.name} (expired=${c.expired})`, async () => {
      const { keys, now } = verifierFor(c);
      let error: unknown;
      try {
        await verifyGrantToken(keys, c.token, now);
      } catch (e) {
        error = e;
      }
      expect(error).toBeInstanceOf(GrantInvalidError);
      expect((error as GrantInvalidError).expired).toBe(c.expired);
    });
  }
});

describe("configuration", () => {
  const key = new Uint8Array(32).fill(7);
  const keyB64 = btoa(String.fromCharCode(...key));

  test("env: unset is off; a key turns grants on; padding optional", () => {
    expect(GrantKeys.fromEnv({})).toBeNull();
    const keys = GrantKeys.fromEnv({ [GRANT_KEYS_ENV]: keyB64.replace(/=+$/, ""), VGI_RPC_GRANT_AUDIENCE: "aud" });
    expect(keys?.audience).toBe("aud");
    expect(keys?.maxTtlSeconds).toBe(604800);
  });

  test("a malformed key, duplicate keys or a bad lifetime refuse to start", () => {
    expect(() => GrantKeys.fromEnv({ [GRANT_KEYS_ENV]: "not*base64" })).toThrow(/base64/);
    expect(() => GrantKeys.fromEnv({ [GRANT_KEYS_ENV]: btoa("short") })).toThrow(/32 are required/);
    expect(() => GrantKeys.fromEnv({ [GRANT_KEYS_ENV]: `${keyB64},${keyB64}` })).toThrow(/distinct/);
    expect(() => GrantKeys.fromEnv({ [GRANT_KEYS_ENV]: keyB64, VGI_RPC_GRANT_MAX_TTL_SECONDS: "0" })).toThrow();
    expect(() => GrantKeys.fromEnv({ [GRANT_KEYS_ENV]: keyB64, VGI_RPC_GRANT_MAX_TTL_SECONDS: "1d" })).toThrow();
  });
});

// ---------------------------------------------------------------------------
// The chain (§9.3)
// ---------------------------------------------------------------------------

const KEYS = new GrantKeys([new Uint8Array(32).fill(1)], { audience: "test", maxTtlSeconds: 3600 });
const bearer = (token: string) => new Request("http://x/", { headers: { Authorization: `Bearer ${token}` } });

describe("the authenticator chain", () => {
  test("a minted grant authenticates as its owner, domain grant, no auth_time", async () => {
    const { token } = await mintGrantToken(KEYS, {
      principal: "alice",
      scopes: ["r"],
      purpose: "p",
      ttlSeconds: 60,
    });
    const auth = await grantAuthenticate(KEYS)(bearer(token));
    expect([auth.domain, auth.principal, auth.authenticated]).toEqual(["grant", "alice", true]);
    expect(Object.keys(auth.claims).sort()).toEqual(["grant_id", "purpose", "scopes"]);
  });

  test("a bad vgig1. token stops the chain -- resolveToken is never asked", async () => {
    let asked = 0;
    const chain = composeIdentityAuthenticate(undefined, {
      grantKeys: KEYS,
      resolveToken: () => {
        asked++;
        return { principal: "anyone" };
      },
    })!;
    await expect(chain(bearer(`${GRANT_TOKEN_PREFIX}AAAA`))).rejects.toBeInstanceOf(GrantRejectedError);
    expect(asked).toBe(0);
    // Anything without the exact prefix never reaches the grant verifier.
    expect((await chain(bearer("vgig2.AAAA"))).domain).toBe("token");
    expect(asked).toBe(1);
  });

  test("expired authentic grants report expired_credential", async () => {
    const { token } = await mintGrantToken(KEYS, {
      principal: "alice",
      scopes: [],
      purpose: "",
      ttlSeconds: 60,
      now: Math.trunc(Date.now() / 1000) - 3600,
    });
    let error: unknown;
    try {
      await grantAuthenticate(KEYS)(bearer(token));
    } catch (e) {
      error = e;
    }
    expect((error as GrantRejectedError).vgiAuthReason).toBe("expired_credential");
  });

  test("resolveToken: resolved, unknown, unavailable, JWS-shaped, no credential", async () => {
    const chain = composeIdentityAuthenticate(undefined, {
      resolveToken: (t) => {
        if (t === "down") throw new IdentityUnavailableError("down", 9);
        if (t === "unknown") return null;
        return { principal: "subject", tokenName: "n" };
      },
    })!;
    const ok = await chain(bearer("opaque"));
    expect([ok.domain, ok.principal, ok.claims.token_name, "auth_time" in ok.claims]).toEqual([
      "token",
      "subject",
      "n",
      false,
    ]);
    await expect(chain(bearer("unknown"))).rejects.toThrow();
    const down = await chain(bearer("down")).catch((e) => e);
    expect(down).toBeInstanceOf(AuthUnavailableError);
    expect((down as AuthUnavailableError).retryAfter).toBe(9);
    await expect(chain(bearer("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbGljZSJ9.c2lnbmF0dXJl"))).rejects.toThrow();
    // No Authorization header: anonymous, as before.
    expect((await chain(new Request("http://x/"))).authenticated).toBe(false);
  });

  test("the deployment's own authenticator runs first", async () => {
    const own = (r: Request) => {
      if (r.headers.get("Authorization") === "Bearer mine") return new AuthContext("jwt", true, "me");
      throw new Error("not mine");
    };
    const chain = composeIdentityAuthenticate(own, { resolveToken: () => ({ principal: "resolved" }) })!;
    expect((await chain(bearer("mine"))).principal).toBe("me");
    expect((await chain(bearer("other"))).principal).toBe("resolved");
  });
});

describe("IdentityImpl with grant keys", () => {
  test("mints sealed grants when no hook is given, and grants never mint grants", async () => {
    const identity = new IdentityImpl({ grantKeys: KEYS });
    expect([...identity.offeredMethods()]).toEqual(["issue_grant"]);
    expect(buildIdentityProtocol(identity)).not.toBeNull();
    const fresh = new AuthContext("jwt", true, "alice", { auth_time: Date.now() / 1000 });
    const grant = await identity.issueGrant("p", ["a"], 600, fresh);
    expect(grant.token.startsWith(GRANT_TOKEN_PREFIX)).toBe(true);
    const asGrant = await grantAuthenticate(KEYS)(bearer(grant.token));
    expect(asGrant.principal).toBe("alice");
    await expect(identity.issueGrant("p", [], 600, asGrant)).rejects.toMatchObject({ errorKind: "stale_auth" });
  });
});

describe("fixture protocol hashes", () => {
  test("conformance.Whoami.v1 and conformance.Secondary.v1 hash to their pinned digests", async () => {
    for (const [p, pinned] of [
      [buildWhoamiProtocol(), WHOAMI_PROTOCOL_HASH],
      [buildSecondaryProtocol(), SECONDARY_PROTOCOL_HASH],
    ] as const) {
      expect(await protocolHashFor({ name: p.name, protocol: p, versionExempt: false })).toBe(pinned);
    }
  });
});

describe("the loop over HTTP", () => {
  test("issue_grant with a fresh caller, then Bearer <grant> authenticates as the owner", async () => {
    const { createHttpHandler } = await import("../src/http/index.js");
    const { VgiRpcServer } = await import("../src/server.js");
    const { Protocol } = await import("../src/protocol.js");
    const { httpConnect } = await import("../src/client/connect.js");
    const { conformanceAuthenticate } = await import("../src/conformance/index.js");
    const app = new Protocol("app.Test.v1").unary("noop", { params: {}, result: {}, handler: () => ({}) });
    const server = new VgiRpcServer(app, { protocols: [buildWhoamiProtocol()], grantKeys: KEYS });
    expect([...server.bindings().keys()]).toContain("vgi_rpc.Identity.v1");
    const handler = createHttpHandler(server, { serverId: "grant-e2e", authenticate: conformanceAuthenticate });
    const client = (protocol: string, headers: Record<string, string>) =>
      httpConnect("http://test", {
        protocol,
        fetch: (async (input: RequestInfo | URL, init?: RequestInit) => {
          const merged = new Headers(init?.headers);
          for (const [k, v] of Object.entries(headers)) merged.set(k, v);
          return handler(new Request(input, { ...init, headers: merged }));
        }) as typeof globalThis.fetch,
      });
    const minter = client("vgi_rpc.Identity.v1", {
      "X-Conformance-Principal": "owner@example",
      "X-Conformance-Auth-Time": String(Math.trunc(Date.now() / 1000)),
    });
    const issued = (await minter.call("issue_grant", { purpose: "e2e", scopes: ["x"], ttl_seconds: 300 })) as {
      result: Uint8Array;
    };
    minter.close();
    const { deserializeBatch } = await import("../src/arrow/index.js");
    const grantToken = String((deserializeBatch(issued.result) as any).getChild("token").get(0));
    expect(grantToken.startsWith(GRANT_TOKEN_PREFIX)).toBe(true);
    const whoami = client("conformance.Whoami.v1", { Authorization: `Bearer ${grantToken}` });
    const seen = JSON.parse(String(((await whoami.call("whoami", {})) as { result: string }).result));
    whoami.close();
    expect([seen.authenticated, seen.domain, seen.principal]).toEqual([true, "grant", "owner@example"]);
  });
});
