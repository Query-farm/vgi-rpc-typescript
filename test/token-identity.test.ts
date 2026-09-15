// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// `vgi_rpc.Identity.v1` — resolving a credential, and minting a grant.
//
// The two methods are guarded very differently and the difference is the point,
// so most of what is tested here is the *asymmetry*: introspection answers a
// question about somebody else's credential and is therefore an oracle that has
// to be locked down; issuance is always about the caller and therefore is not.

import { describe, expect, test } from "bun:test";
import { deserializeBatch } from "../src/arrow/index.js";
import { isList } from "../src/arrow/predicates.js";
import { AuthContext } from "../src/auth.js";
import { chainAuthenticate } from "../src/http/bearer.js";
import { AuthUnavailableError } from "../src/http/unauthorized.js";
import { Protocol } from "../src/protocol.js";
import { canonicalDescription, type HashMethod } from "../src/protocol-hash.js";
import { bindingHash, unaryHasReturn } from "../src/reflection.js";
import { VgiRpcServer } from "../src/server.js";
import {
  buildIdentityProtocol,
  GrantRefusedError,
  IDENTITY_PROTOCOL_NAME,
  IdentityImpl,
  IdentityUnavailableError,
  IntrospectionRefusedError,
  type IssuedGrant,
  MAX_TOKEN_CHARS,
  RateLimiter,
  rejectJwsShaped,
  StaleAuthError,
  type TokenIdentity,
  TokenUnresolvedError,
  tokenDigest,
} from "../src/token-identity.js";
import type { CallContext, MethodDefinition } from "../src/types.js";

const NOW = () => Date.now() / 1000;

function auth(
  principal: string | null = "alice",
  options: { authenticated?: boolean; authTime?: number | string | null } = {},
): AuthContext {
  const claims: Record<string, unknown> = {};
  if (options.authTime !== undefined) claims.auth_time = options.authTime;
  return new AuthContext("test", options.authenticated ?? true, principal, claims);
}

/** The context a dispatcher hands a unary handler. Only `auth` is read. */
function ctx(a: AuthContext): CallContext {
  return { auth: a, clientLog: () => {} } as unknown as CallContext;
}

const resolver = (token: string): TokenIdentity | null =>
  token === "good" ? { principal: "bob", tokenName: "ci-key" } : null;

const minter = (principal: string, _purpose: string, _scopes: readonly string[], ttlSeconds: number): IssuedGrant => ({
  token: `grant-for-${principal}`,
  expiresAt: NOW() + ttlSeconds,
  grantId: "g1",
});

function introspecting(options: Record<string, unknown> = {}): IdentityImpl {
  return new IdentityImpl({ resolveToken: resolver, introspectPrincipals: ["proxy"], ...options });
}

/** The hash inputs for a protocol's methods, the way the server derives them. */
function hashMethods(methods: ReadonlyMap<string, MethodDefinition>): HashMethod[] {
  return [...methods.values()].map((m) => ({
    name: m.name,
    methodType: "unary",
    hasReturn: unaryHasReturn(m),
    hasHeader: m.headerSchema !== undefined,
    paramsFields: m.paramsSchema?.fields ?? [],
    resultFields: m.resultSchema?.fields ?? [],
  }));
}

/** Decode a single-row payload stream into a plain object. */
function decodePayload(bytes: Uint8Array): Record<string, unknown> {
  const batch = deserializeBatch(bytes);
  const out: Record<string, unknown> = {};
  batch.schema.fields.forEach((f, i) => {
    out[f.name] = batch.getChildAt(i)?.get(0);
  });
  return out;
}

// ---------------------------------------------------------------------------

describe("the wire shape", () => {
  // Computed by the Python reference. A port that disagrees on any field name,
  // type or nullability flag disagrees on whether it speaks this protocol, and
  // the digest is the only place that shows up before a call fails.
  test("hosting both methods matches the reference digest", async () => {
    const p = buildIdentityProtocol(
      new IdentityImpl({ resolveToken: resolver, mintGrant: minter, introspectPrincipals: ["proxy"] }),
    )!;
    expect(await bindingHash(IDENTITY_PROTOCOL_NAME, p.getMethods())).toBe(
      "8317f2ad8e2476bb99e8b94800ab79b19a8cf0c6bdd6d66c2d82bd62ffbe69d5",
    );
  });

  test("hosting introspect_token alone matches the reference digest", async () => {
    // Not decoration: this proves method-level narrowing actually narrows the
    // hash rather than hosting a method that refuses.
    const p = buildIdentityProtocol(introspecting())!;
    expect(await bindingHash(IDENTITY_PROTOCOL_NAME, p.getMethods())).toBe(
      "27b75bef22e4c70baab92a5188a473506b89055d2cb2b58cc187f6fe7a436385",
    );
  });

  test("hosting issue_grant alone matches the reference digest", async () => {
    const p = buildIdentityProtocol(new IdentityImpl({ mintGrant: minter }))!;
    expect(await bindingHash(IDENTITY_PROTOCOL_NAME, p.getMethods())).toBe(
      "c71b12f453310139b6b6a445378064661c52711d03ae1e4fba29b8f7976ef4d8",
    );
  });

  test("narrowing the method set narrows the hash", async () => {
    // A server offering half the methods is not offering the same surface.
    const both = buildIdentityProtocol(
      new IdentityImpl({ resolveToken: resolver, mintGrant: minter, introspectPrincipals: ["proxy"] }),
    )!;
    const one = buildIdentityProtocol(introspecting())!;
    expect(await bindingHash(IDENTITY_PROTOCOL_NAME, both.getMethods())).not.toBe(
      await bindingHash(IDENTITY_PROTOCOL_NAME, one.getMethods()),
    );
  });

  test("the canonical preimage matches the reference byte for byte", () => {
    // With the preimage in hand a failing port diffs two JSON documents rather
    // than staring at one bit of disagreement.
    const p = buildIdentityProtocol(
      new IdentityImpl({ resolveToken: resolver, mintGrant: minter, introspectPrincipals: ["proxy"] }),
    )!;
    expect(canonicalDescription(IDENTITY_PROTOCOL_NAME, hashMethods(p.getMethods()))).toBe(
      '{"methods":[{"has_header":false,"has_return":true,"name":"introspect_token","params":[{"name":"token","nullable":false,"type":"utf8"}],"result":[{"name":"result","nullable":false,"type":"binary"}],"type":"unary"},{"has_header":false,"has_return":true,"name":"issue_grant","params":[{"name":"purpose","nullable":false,"type":"utf8"},{"name":"scopes","nullable":false,"type":"list<item?:utf8>"},{"name":"ttl_seconds","nullable":false,"type":"int64"}],"result":[{"name":"result","nullable":false,"type":"binary"}],"type":"unary"}],"protocol":"vgi_rpc.Identity.v1"}',
    );
  });

  test("the scopes list item is nullable, and the column is not", () => {
    // Arrow's own convention for a list child, and part of the type — so it is
    // part of the hash. This port once declared every list item non-nullable;
    // the digest above would be wrong and nothing else would say why.
    const p = buildIdentityProtocol(new IdentityImpl({ mintGrant: minter }))!;
    const scopes = p.getMethod("issue_grant")!.paramsSchema!.fields.find((f) => f.name === "scopes")!;
    expect(scopes.nullable).toBe(false);
    expect(isList(scopes.type)).toBe(true);
    expect((scopes.type as unknown as { children: { nullable: boolean }[] }).children[0].nullable).toBe(true);
  });

  test("a payload rides as serialized Arrow IPC in a single binary column", () => {
    const p = buildIdentityProtocol(introspecting())!;
    const result = p.getMethod("introspect_token")!.resultSchema!;
    expect(result.fields.map((f) => f.name)).toEqual(["result"]);
    expect(result.fields[0].nullable).toBe(false);
  });
});

describe("registration", () => {
  // Absent beats routed-and-refusing.
  const app = () => new Protocol("demo.App.v1");

  test("identity is absent by default", () => {
    // A dependency upgrade must not grow an oracle on every worker.
    const server = new VgiRpcServer(app());
    expect(server.bindings().has(IDENTITY_PROTOCOL_NAME)).toBe(false);
  });

  test("only methods with hooks are offered", () => {
    // What the server hosts describes what it actually does. A worker that
    // resolves credentials but does not mint grants offers one method, and a
    // client learns that from reflection rather than by calling and reading an
    // error.
    expect([...introspecting().offeredMethods()]).toEqual(["introspect_token"]);
    expect([...new IdentityImpl({ mintGrant: minter }).offeredMethods()]).toEqual(["issue_grant"]);
    expect(
      [
        ...new IdentityImpl({
          resolveToken: resolver,
          mintGrant: minter,
          introspectPrincipals: ["proxy"],
        }).offeredMethods(),
      ].sort(),
    ).toEqual(["introspect_token", "issue_grant"]);
  });

  test("the hosted binding carries only the offered methods", () => {
    const server = new VgiRpcServer(app());
    server.registerIdentity(new IdentityImpl({ mintGrant: minter }));
    expect(server.bindings().get(IDENTITY_PROTOCOL_NAME)!.protocol.methodNames()).toEqual(["issue_grant"]);
  });

  test("nothing is registered when neither hook is configured", () => {
    // If neither hook exists the protocol is not registered at all.
    const server = new VgiRpcServer(app());
    server.registerIdentity(new IdentityImpl());
    expect(server.bindings().has(IDENTITY_PROTOCOL_NAME)).toBe(false);
  });

  test("identity claims the reserved prefix", () => {
    // Framework-owned, so an application cannot impersonate it.
    expect(IDENTITY_PROTOCOL_NAME.startsWith("vgi_rpc.")).toBe(true);
    const server = new VgiRpcServer(app());
    const identity = buildIdentityProtocol(new IdentityImpl({ mintGrant: minter }))!;
    expect(() =>
      server.addProtocol({ name: IDENTITY_PROTOCOL_NAME, protocol: identity, protocolHash: "", versionExempt: false }),
    ).toThrow(/reserved/);
  });

  test("identity appears in reflection's listing", () => {
    // Registered after reflection so a client discovers it the ordinary way.
    const server = new VgiRpcServer(app());
    server.registerReflection();
    server.registerIdentity(introspecting());
    expect([...server.bindings().keys()]).toEqual(["demo.App.v1", "vgi_rpc.Reflection.v1", IDENTITY_PROTOCOL_NAME]);
    // …and it routes: the method resolves on (protocol, method).
    expect(server.resolve(IDENTITY_PROTOCOL_NAME, "introspect_token").method.name).toBe("introspect_token");
  });
});

describe("introspection is locked down", () => {
  // The answer is an identity assertion the asker acts on with its own
  // credentials. "Trust it as much as you trust the worker" is the wrong frame:
  // the asker trusts it *more*, because it authorizes with credentials the
  // worker does not hold.

  test("resolves for an allowlisted caller", async () => {
    // The happy path, for the reverse proxy the method exists for.
    const got = await introspecting().introspectToken("good", auth("proxy"));
    expect(got.principal).toBe("bob");
    expect(got.tokenName).toBe("ci-key");
  });

  test("a caller off the allowlist is refused", async () => {
    // Authentication is not the same capability as introspection. A deployment
    // where any valid credential may introspect lets any user test guesses of
    // any other user's credential at unlimited rate, and resolve a stolen one
    // to its owner.
    for (const caller of ["alice", "", null]) {
      await expect(introspecting().introspectToken("good", auth(caller))).rejects.toThrow(IntrospectionRefusedError);
    }
  });

  test("an unauthenticated caller is refused", async () => {
    // Subprocess and unix transports carry no authenticated principal.
    await expect(introspecting().introspectToken("good", auth("proxy", { authenticated: false }))).rejects.toThrow(
      IntrospectionRefusedError,
    );
  });

  test("refusal precedes the resolver", async () => {
    // An unauthorized caller learns nothing, including how long it took.
    const seen: string[] = [];
    const impl = new IdentityImpl({
      resolveToken: (token) => {
        seen.push(token);
        return null;
      },
      introspectPrincipals: ["proxy"],
    });
    await expect(impl.introspectToken("secret", auth("mallory"))).rejects.toThrow(IntrospectionRefusedError);
    expect(seen).toEqual([]);
  });

  test("the authorization guard runs before the credential is even measured", async () => {
    // Steps 2–3 come before step 4 on purpose, and the order is load-bearing:
    // an unauthorized caller presenting an over-long or JWS-shaped subject must
    // still get `introspection_refused`, never `token_unresolved`. Reordering
    // for tidiness would leak that the subject was malformed to someone with no
    // standing to ask.
    const impl = introspecting();
    for (const token of ["", "x".repeat(MAX_TOKEN_CHARS + 1), "aaa.bbb.ccc"]) {
      const err = await impl.introspectToken(token, auth("mallory")).catch((e) => e);
      expect(err).toBeInstanceOf(IntrospectionRefusedError);
      expect(err.errorKind).toBe("introspection_refused");
    }
  });

  test("the rate limit is checked before the credential too", async () => {
    // Same reason: an over-budget caller must not be told anything about the
    // subject, including that it was the wrong shape.
    const impl = introspecting({ introspectRateLimit: 1 });
    await impl.introspectToken("good", auth("proxy"));
    const err = await impl.introspectToken("aaa.bbb.ccc", auth("proxy")).catch((e) => e);
    expect(err).toBeInstanceOf(IntrospectionRefusedError);
    expect(err.message).toMatch(/rate limit/);
  });

  test("rejections are uniform", async () => {
    // Unknown, malformed and over-long are one answer. Distinguishing them
    // would confirm that a guessed credential exists.
    for (const token of ["", "unknown", "x".repeat(MAX_TOKEN_CHARS + 1)]) {
      const err = await introspecting()
        .introspectToken(token, auth("proxy"))
        .catch((e) => e);
      expect(err).toBeInstanceOf(TokenUnresolvedError);
      expect(err.message).toBe("unresolved");
    }
  });

  test("a JWS never reaches the resolver", async () => {
    // Routing one onward hands a third party a token the asker may have
    // rejected. A JWS is validated locally against a key set; forwarding one
    // the asker already refused — expired, wrong audience — to something that
    // might accept it turns this method into a laundering step.
    const seen: string[] = [];
    const impl = new IdentityImpl({
      resolveToken: (token) => {
        seen.push(token);
        return { principal: "bob" };
      },
      introspectPrincipals: ["proxy"],
    });
    await expect(impl.introspectToken("aaa.bbb.ccc", auth("proxy"))).rejects.toThrow(TokenUnresolvedError);
    expect(seen).toEqual([]);
  });

  test("unavailable is transient, not definitive", async () => {
    // A caller that negative-caches "unknown" must not cache this. Cache an
    // outage and a worker restart takes the fleet down for the cache's
    // lifetime; retry a rejection and the worker is hammered.
    const impl = new IdentityImpl({
      resolveToken: () => {
        throw new IdentityUnavailableError("store is down");
      },
      introspectPrincipals: ["proxy"],
    });
    const err = await impl.introspectToken("good", auth("proxy")).catch((e) => e);
    expect(err).toBeInstanceOf(IdentityUnavailableError);
    expect(err.retryAfter).toBeGreaterThan(0);
    expect(err.errorKind).toBe("identity_unavailable");
    // Not a definitive rejection wearing a different name.
    expect(err).not.toBeInstanceOf(TokenUnresolvedError);
  });

  test("the HTTP route's unavailable error keeps its transient classification", async () => {
    // One resolver type serves both this protocol and the older
    // `__introspect_token__` route, whose "not knowable" is spelled
    // `AuthUnavailableError`. Propagating it untranslated would put an outage
    // on the wire with no `error_kind` at all — and `error_kind` is the only
    // definitive-versus-transient signal a caller gets.
    const impl = new IdentityImpl({
      resolveToken: () => {
        throw new AuthUnavailableError("sidecar down", 11);
      },
      introspectPrincipals: ["proxy"],
    });
    const err = await impl.introspectToken("good", auth("proxy")).catch((e) => e);
    expect(err).toBeInstanceOf(IdentityUnavailableError);
    expect(err.errorKind).toBe("identity_unavailable");
    expect(err.retryAfter).toBe(11);
  });

  test("an authenticate chain does not swallow an unavailable identity", async () => {
    // This port's equivalent of Python's "must not be a ValueError":
    // `chainAuthenticate` advances on a *plain* Error — "not my credential, try
    // the next" — so a sidecar outage raised as one emerges as a 401 from the
    // end of the chain and restarts every session in the fleet over a
    // thirty-second blip. Being an Error *subclass* is what makes it propagate.
    const ok = async () => auth("fallback");
    const chained = chainAuthenticate(async () => {
      throw new IdentityUnavailableError("store is down");
    }, ok);
    await expect(chained(new Request("http://worker.test/"))).rejects.toThrow(IdentityUnavailableError);

    // Contrast: a plain Error *is* "try the next one", which is exactly the
    // classification IdentityUnavailableError must not fall into.
    const advanced = chainAuthenticate(async () => {
      throw new Error("not my credential");
    }, ok);
    expect((await advanced(new Request("http://worker.test/"))).principal).toBe("fallback");
  });

  test("rate limited", async () => {
    // Bounds, rather than closes, the oracle an allowlisted caller still has.
    const impl = introspecting({ introspectRateLimit: 2 });
    expect((await impl.introspectToken("good", auth("proxy"))).principal).toBe("bob");
    expect((await impl.introspectToken("good", auth("proxy"))).principal).toBe("bob");
    await expect(impl.introspectToken("good", auth("proxy"))).rejects.toThrow(/rate limit/);
  });

  test("an allowlist is mandatory", () => {
    // There is no permissive default, so it cannot be reached by omission — and
    // it is validated at construction, so a worker that would refuse every
    // introspection fails to start rather than serving traffic until someone
    // tries.
    expect(() => new IdentityImpl({ resolveToken: resolver })).toThrow(/at least one principal/);
    expect(() => new IdentityImpl({ resolveToken: resolver, introspectPrincipals: [] })).toThrow(
      /at least one principal/,
    );
    expect(() => new IdentityImpl({ resolveToken: resolver, introspectPrincipals: [""] })).toThrow(
      /at least one principal/,
    );
  });
});

describe("issuance is not an oracle", () => {
  // Issuance is always about the caller, so it needs neither allowlist nor
  // limit.

  test("mints for the caller", async () => {
    // The happy path: a present user minting their own standing grant.
    const impl = new IdentityImpl({ mintGrant: minter });
    const grant = await impl.issueGrant("reports", ["read"], 3600, auth("alice", { authTime: NOW() }));
    expect(grant.token).toBe("grant-for-alice");
    expect(grant.expiresAt).toBeGreaterThan(NOW());
  });

  test("the subject is the caller and is not a parameter", () => {
    // Cross-subject minting is closed by construction, not by a check. A check
    // is something one of seven ports can forget; a missing parameter is not.
    const p = buildIdentityProtocol(new IdentityImpl({ mintGrant: minter }))!;
    const params = p.getMethod("issue_grant")!.paramsSchema!.fields.map((f) => f.name);
    expect(params).toEqual(["purpose", "scopes", "ttl_seconds"]);
    expect(params).not.toContain("subject");
    expect(params).not.toContain("principal");
  });

  test("needs no allowlist", () => {
    // Unlike introspection — and the asymmetry is the whole design.
    expect([...new IdentityImpl({ mintGrant: minter }).offeredMethods()]).toEqual(["issue_grant"]);
  });
});

describe("freshness", () => {
  // A credential with no verifiable auth_time cannot mint.
  const impl = () => new IdentityImpl({ mintGrant: minter, maxAuthAge: 900 });

  test("absent auth_time is refused", async () => {
    // A static bearer proves a machine holds a secret, never that a human just
    // logged in.
    await expect(impl().issueGrant("p", [], 60, auth("alice"))).rejects.toThrow(/no auth_time/);
  });

  test("an unusable auth_time is refused", async () => {
    // Present but not a number is not a verified login either.
    await expect(impl().issueGrant("p", [], 60, auth("alice", { authTime: "yesterday" }))).rejects.toThrow(
      StaleAuthError,
    );
  });

  test("stale auth_time is refused actionably", async () => {
    // Naming the reason leaks nothing here: it is always about the caller. A
    // console that cannot tell "your login is too old" from "no" cannot know to
    // re-prompt.
    await expect(impl().issueGrant("p", [], 60, auth("alice", { authTime: NOW() - 5000 }))).rejects.toThrow(
      /re-authenticate/,
    );
  });

  test("fresh auth_time is accepted", async () => {
    // The ceiling is a ceiling, not an equality.
    const grant = await impl().issueGrant("p", [], 60, auth("alice", { authTime: NOW() - 10 }));
    expect(grant.token).toBe("grant-for-alice");
  });

  test("a grant cannot mint another grant", async () => {
    // The lineage cannot escape the identity provider. A grant is not an
    // IdP-issued token, so it carries no auth_time, so presenting one here
    // fails the freshness check. That single rule is what stops indefinite
    // self-renewal.
    const grantBearer = auth("alice"); // no auth_time: this is what a grant looks like
    await expect(impl().issueGrant("p", [], 60, grantBearer)).rejects.toThrow(StaleAuthError);
  });

  test("an unauthenticated transport fails closed", async () => {
    // Subprocess and unix have no authenticated principal at all.
    await expect(impl().issueGrant("p", [], 60, auth(null, { authenticated: false }))).rejects.toThrow(
      /not authenticated/,
    );
  });
});

describe("absent hooks", () => {
  // Calling a method the deployment did not configure. The per-method guard is
  // the belt to method-level narrowing's braces — both exist.

  test("introspection without a resolver", async () => {
    // Refused rather than crashing, for a caller that reached it anyway.
    await expect(new IdentityImpl({ mintGrant: minter }).introspectToken("good", auth("proxy"))).rejects.toThrow(
      /does not resolve/,
    );
  });

  test("issuance without a minter", async () => {
    // Same, on the other side.
    await expect(introspecting().issueGrant("p", [], 60, auth("alice", { authTime: NOW() }))).rejects.toThrow(
      GrantRefusedError,
    );
  });
});

describe("diagnostics", () => {
  // The credential must never reach a log, a span, or an error message.

  test("the digest is not the token", async () => {
    // Stable enough to correlate one credential's failures; not the credential.
    expect(await tokenDigest("secret")).not.toBe("secret");
    expect(await tokenDigest("secret")).toBe(await tokenDigest("secret"));
    expect(await tokenDigest("secret")).not.toBe(await tokenDigest("other"));
    expect((await tokenDigest("secret")).length).toBe(64);
  });

  test("error kinds are stable", () => {
    // They are the only definitive/transient signal a caller has. These were an
    // HTTP route whose callers classified on the status code (404 vs 503). As
    // protocol methods every handler exception surfaces the same way, so
    // `error_kind` carries the whole distinction.
    expect(IntrospectionRefusedError.errorKind).toBe("introspection_refused");
    expect(TokenUnresolvedError.errorKind).toBe("token_unresolved");
    expect(StaleAuthError.errorKind).toBe("stale_auth");
    expect(GrantRefusedError.errorKind).toBe("grant_refused");
    expect(IdentityUnavailableError.errorKind).toBe("identity_unavailable");
    // Hoisted onto the error batch as `vgi_rpc.error_kind`, which reads the
    // instance property — a static alone would never reach the wire.
    expect(new TokenUnresolvedError("unresolved").errorKind).toBe("token_unresolved");
  });
});

describe("the rate limiter", () => {
  // Fixed-window, because the state is two integers rather than an aged float.

  test("admits up to the limit", () => {
    const limiter = new RateLimiter(3);
    expect([0, 1, 2, 3].map(() => limiter.allow("a", 100_000))).toEqual([true, true, true, false]);
  });

  test("the window rolls", () => {
    const limiter = new RateLimiter(1);
    expect(limiter.allow("a", 100_000)).toBe(true);
    expect(limiter.allow("a", 100_500)).toBe(false);
    expect(limiter.allow("a", 101_500)).toBe(true);
  });

  test("callers are independent", () => {
    // One caller exhausting its budget must not refuse another.
    const limiter = new RateLimiter(1);
    expect(limiter.allow("a", 100_000)).toBe(true);
    expect(limiter.allow("b", 100_000)).toBe(true);
    expect(limiter.allow("a", 100_000)).toBe(false);
  });

  test("cycling keys cannot grow the map", () => {
    // Whole-map reset rather than per-key ageing, which would let a caller
    // cycling keys grow the map without bound between sweeps.
    const limiter = new RateLimiter(1);
    for (let i = 0; i < 1000; i++) limiter.allow(`k${i}`, 100_000);
    limiter.allow("fresh", 200_000);
    expect(limiter.size).toBe(1);
  });
});

describe("the JWS shape test survives translation", () => {
  // Whitespace must not be a way to walk a JWS past the guard.
  //
  // This exists because the ports diverged here and the reference was the
  // accident: Python's `$` matches before a single trailing newline, so
  // "a.b.c\n" was refused there while this port's unflagged `$` matched
  // strictly and routed the same credential straight to the resolver — the one
  // outcome the guard exists to prevent. Python was not even self-consistent,
  // refusing one trailing newline and admitting two. Testing the trimmed form
  // is the rule that means the same thing in seven regex dialects, because it
  // depends on none of their anchor semantics.

  test("padding does not smuggle a JWS past the guard", () => {
    // No amount of surrounding whitespace makes a JWS resolvable.
    for (const token of ["aaa.bbb.ccc", "aaa.bbb.ccc\n", "aaa.bbb.ccc\n\n", "  aaa.bbb.ccc  ", "\taaa.bbb.ccc\r\n"]) {
      expect(() => rejectJwsShaped(token)).toThrow(TokenUnresolvedError);
    }
  });

  test("a blank credential is not a credential", () => {
    // Whitespace-only never reaches a resolver either.
    for (const token of ["", "   ", "\n", "\t\r\n"]) {
      expect(() => rejectJwsShaped(token)).toThrow(TokenUnresolvedError);
    }
  });

  test("an opaque credential still reaches the resolver", () => {
    // Trimming tightens the JWS test; it must not refuse ordinary tokens.
    for (const token of ["opaque-token", "a.b.c.d", "two.segments", "sk_live_abc123"]) {
      expect(() => rejectJwsShaped(token)).not.toThrow();
    }
  });

  test("the length cap is measured against the original", () => {
    // Trimming is not a way to shrink an over-long credential under the cap.
    expect(() => rejectJwsShaped(`${" ".repeat(MAX_TOKEN_CHARS)}opaque`)).toThrow(TokenUnresolvedError);
  });

  test("the resolver receives the credential unmodified", async () => {
    // Trimming is for the shape test only — never for what is resolved.
    // Rewriting a credential before resolving it would make the worker answer
    // about a string the caller never sent.
    const seen: string[] = [];
    const impl = new IdentityImpl({
      resolveToken: (token) => {
        seen.push(token);
        return { principal: "p" };
      },
      introspectPrincipals: ["proxy"],
    });
    await impl.introspectToken("  padded-opaque-token  ", auth("proxy"));
    expect(seen).toEqual(["  padded-opaque-token  "]);
  });
});

describe("the dispatched methods", () => {
  // End to end through the registered handlers, which is where the payload
  // encoding and the defaults live.

  test("introspect_token returns a TokenIdentity payload with defaults filled", async () => {
    const p = buildIdentityProtocol(introspecting())!;
    const out = await p.getMethod("introspect_token")!.handler!({ token: "good" }, ctx(auth("proxy")));
    expect(decodePayload(out.result)).toEqual({ principal: "bob", token_name: "ci-key", ttl_seconds: 300n });
  });

  test("issue_grant returns an IssuedGrant payload", async () => {
    const p = buildIdentityProtocol(new IdentityImpl({ mintGrant: minter }))!;
    const out = await p.getMethod("issue_grant")!.handler!(
      { purpose: "reports", scopes: ["read", "write"], ttl_seconds: 60n },
      ctx(auth("alice", { authTime: NOW() })),
    );
    const decoded = decodePayload(out.result);
    expect(decoded.token).toBe("grant-for-alice");
    expect(decoded.grant_id).toBe("g1");
    expect(Number(decoded.expires_at)).toBeGreaterThan(NOW());
  });

  test("a null scope arrives as an empty string rather than vanishing", async () => {
    // The list item is nullable on the wire. Dropping a null silently would
    // change the request the worker was asked to judge.
    let seen: readonly string[] = [];
    const p = buildIdentityProtocol(
      new IdentityImpl({
        mintGrant: (principal, purpose, scopes, ttl) => {
          seen = scopes;
          return minter(principal, purpose, scopes, ttl);
        },
      }),
    )!;
    await p.getMethod("issue_grant")!.handler!(
      { purpose: "p", scopes: ["read", null], ttl_seconds: 60n },
      ctx(auth("alice", { authTime: NOW() })),
    );
    expect(seen).toEqual(["read", ""]);
  });

  test("a guard failure propagates out of the handler", async () => {
    // The handler is a thin delegation: the guards are the implementation's,
    // and the dispatcher turns the throw into a typed error batch.
    const p = buildIdentityProtocol(introspecting())!;
    await expect(p.getMethod("introspect_token")!.handler!({ token: "good" }, ctx(auth("mallory")))).rejects.toThrow(
      IntrospectionRefusedError,
    );
  });
});
