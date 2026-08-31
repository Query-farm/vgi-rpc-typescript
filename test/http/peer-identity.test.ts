// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import { RecordBatch, RecordBatchStreamWriter, recordBatchFromArrays } from "@query-farm/apache-arrow";
import { AuthContext } from "../../src/auth.js";
import { REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY } from "../../src/constants.js";
import { ARROW_CONTENT_TYPE } from "../../src/http/common.js";
import { createHttpHandler } from "../../src/http/handler.js";
import { AUTH_REASON_HEADER, AuthFailure, AuthReason } from "../../src/http/unauthorized.js";
import {
  anyOfPeerIdentities,
  IdentityAssurance,
  observePeerIdentity,
  PeerIdentity,
  PeerIdentityRejectedError,
  PeerIdentityResult,
  PeerIdentityStatus,
  PeerIdentityUnavailableError,
  type PeerResolutionContext,
  PeerSubjectKind,
  peerIdentityPrimary,
  SubjectStability,
} from "../../src/identity.js";
import { Protocol } from "../../src/protocol.js";
import { str, toSchema } from "../../src/schema.js";
import type { CallContext } from "../../src/types.js";

function spiffeIdentity(): PeerIdentity {
  return new PeerIdentity({
    provider: "spiffe",
    evidenceSource: "mtls",
    assurance: IdentityAssurance.CRYPTOGRAPHIC_PEER,
    issuer: "spiffe://example.org",
    transport: "http",
    subjectKind: PeerSubjectKind.WORKLOAD,
    subjectKey: "spiffe://example.org/workload",
    subjectStability: SubjectStability.STABLE,
    subjectVerified: true,
  });
}

function makeBody(): Uint8Array {
  const schema = toSchema({ message: str });
  const batch = recordBatchFromArrays({ message: ["hi"] }, schema);
  const metadata = new Map([
    [RPC_METHOD_KEY, "echo"],
    [REQUEST_VERSION_KEY, REQUEST_VERSION],
  ]);
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, schema);
  writer.write(new RecordBatch(schema, batch.data, metadata));
  writer.close();
  return writer.toUint8Array(true);
}

function post(handler: (request: Request) => Promise<Response>): Promise<Response> {
  return handler(
    new Request("http://worker.example/echo", {
      method: "POST",
      headers: { "Content-Type": ARROW_CONTENT_TYPE },
      body: makeBody(),
    }),
  );
}

function postWithHeaders(
  handler: (request: Request) => Promise<Response>,
  headers: Record<string, string>,
): Promise<Response> {
  return handler(
    new Request("http://worker.example/echo", {
      method: "POST",
      headers: { "Content-Type": ARROW_CONTENT_TYPE, ...headers },
      body: makeBody(),
    }),
  );
}

function protocol(observe?: (context: CallContext) => void): Protocol {
  const value = new Protocol("peer-identity-test");
  value.unary("echo", {
    params: { message: str },
    result: { message: str },
    handler: (params, context) => {
      observe?.(context);
      return { message: params.message };
    },
  });
  return value;
}

describe("HTTP peer identity pipeline", () => {
  test("peer-primary authentication exposes evidence to the worker", async () => {
    let resolution: PeerResolutionContext | undefined;
    let callContext: CallContext | undefined;
    const handler = createHttpHandler(
      protocol((context) => (callContext = context)),
      {
        peerServiceName: "svc:vgi-worker",
        peerIdentityProviders: [
          {
            provider: "spiffe",
            resolve: (context) => {
              resolution = context;
              return PeerIdentityResult.available(spiffeIdentity());
            },
          },
        ],
        peerAuthenticationPolicy: peerIdentityPrimary("spiffe"),
      },
    );

    const response = await post(handler);
    expect(response.status).toBe(200);
    expect(resolution?.authority).toBe("worker.example");
    expect(resolution?.destinationAddress).toBeUndefined();
    expect(resolution?.serviceName).toBe("svc:vgi-worker");
    expect(callContext?.auth.domain).toBe("spiffe");
    expect(callContext?.peerEvidence.uniqueVerifiedSubject("spiffe").subjectKey).toBe("spiffe://example.org/workload");
  });

  test("observation cannot erase an application missing-credential failure", async () => {
    const handler = createHttpHandler(protocol(), {
      authenticate: () => {
        throw new AuthFailure(AuthReason.MissingCredential, "bearer token required");
      },
      peerIdentityProviders: [{ provider: "spiffe", resolve: () => PeerIdentityResult.available(spiffeIdentity()) }],
      peerAuthenticationPolicy: observePeerIdentity,
    });
    const response = await post(handler);
    expect(response.status).toBe(401);
    expect(response.headers.get(AUTH_REASON_HEADER)).toBe(AuthReason.MissingCredential);
  });

  test("invalid application credentials never fall back to peer-primary", async () => {
    let providerCalled = false;
    const handler = createHttpHandler(protocol(), {
      authenticate: () => {
        throw new AuthFailure(AuthReason.InvalidCredential, "bad bearer token");
      },
      peerIdentityProviders: [
        {
          provider: "spiffe",
          resolve: () => {
            providerCalled = true;
            return PeerIdentityResult.available(spiffeIdentity());
          },
        },
      ],
      peerAuthenticationPolicy: peerIdentityPrimary("spiffe"),
    });
    const response = await post(handler);
    expect(response.status).toBe(401);
    expect(response.headers.get(AUTH_REASON_HEADER)).toBe(AuthReason.InvalidCredential);
    expect(providerCalled).toBe(false);
  });

  test("provider deadline failures are a retryable 503", async () => {
    const handler = createHttpHandler(protocol(), {
      peerResolutionTimeoutMs: 5,
      peerIdentityProviders: [{ provider: "spiffe", resolve: () => new Promise(() => {}) }],
      peerAuthenticationPolicy: peerIdentityPrimary("spiffe"),
    });
    const response = await post(handler);
    expect(response.status).toBe(503);
    expect(response.headers.get("Retry-After")).toBe("5");
  });

  test("observation survives a non-cooperative unavailable provider", async () => {
    const handler = createHttpHandler(protocol(), {
      peerResolutionTimeoutMs: 5,
      peerIdentityProviders: [{ provider: "spiffe", resolve: () => new Promise(() => {}) }],
      peerAuthenticationPolicy: observePeerIdentity,
    });
    expect((await post(handler)).status).toBe(200);
  });

  test("valid application any-of authentication survives unavailable peer evidence", async () => {
    const handler = createHttpHandler(protocol(), {
      authenticate: () => new AuthContext("bearer", true, "alice"),
      peerResolutionTimeoutMs: 5,
      peerIdentityProviders: [
        {
          provider: "spiffe",
          resolve: () => {
            throw new PeerIdentityUnavailableError("provider-secret-unavailable");
          },
        },
      ],
      peerAuthenticationPolicy: anyOfPeerIdentities("spiffe"),
    });
    expect((await post(handler)).status).toBe(200);
  });

  test("a completed invalid provider is not downgraded behind a slow provider", async () => {
    const handler = createHttpHandler(protocol(), {
      authenticate: () => new AuthContext("bearer", true, "alice"),
      peerResolutionTimeoutMs: 5,
      peerIdentityProviders: [
        { provider: "slow", resolve: () => new Promise(() => {}) },
        {
          provider: "invalid",
          resolve: () => new PeerIdentityResult("invalid", PeerIdentityStatus.INVALID),
        },
      ],
      peerAuthenticationPolicy: anyOfPeerIdentities("slow", "invalid"),
    });
    const response = await post(handler);
    expect(response.status).toBe(401);
    expect(response.headers.get(AUTH_REASON_HEADER)).toBe(AuthReason.InvalidCredential);
  });

  test("typed rejection and provider-result mismatch are definitive invalid evidence", async () => {
    for (const provider of [
      {
        provider: "spiffe",
        resolve: () => {
          throw new PeerIdentityRejectedError("provider-secret-rejection");
        },
      },
      {
        provider: "spiffe",
        resolve: () => new PeerIdentityResult("other", PeerIdentityStatus.NO_MATCH),
      },
    ]) {
      const handler = createHttpHandler(protocol(), {
        authenticate: () => new AuthContext("bearer", true, "alice"),
        peerIdentityProviders: [provider],
        peerAuthenticationPolicy: anyOfPeerIdentities("spiffe"),
      });
      const response = await post(handler);
      expect(response.status).toBe(401);
      expect(await response.text()).not.toContain("provider-secret");
    }
  });

  test("provider concurrency must admit one complete configured fanout", () => {
    expect(() =>
      createHttpHandler(protocol(), {
        peerProviderConcurrency: 1,
        peerIdentityProviders: [
          { provider: "first", resolve: () => new PeerIdentityResult("first", PeerIdentityStatus.NO_MATCH) },
          { provider: "second", resolve: () => new PeerIdentityResult("second", PeerIdentityStatus.NO_MATCH) },
        ],
      }),
    ).toThrow("at least the configured provider fanout");
  });

  test("authentication callback and peer policy exception details are never returned", async () => {
    const authenticationHandler = createHttpHandler(protocol(), {
      authenticate: () => {
        throw new Error("application-callback-secret");
      },
    });
    const authenticationResponse = await post(authenticationHandler);
    expect(authenticationResponse.status).toBe(401);
    expect(await authenticationResponse.text()).not.toContain("application-callback-secret");

    const policyHandler = createHttpHandler(protocol(), {
      peerIdentityProviders: [
        { provider: "spiffe", resolve: () => new PeerIdentityResult("spiffe", PeerIdentityStatus.NO_MATCH) },
      ],
      peerAuthenticationPolicy: () => {
        throw new Error("peer-policy-secret");
      },
    });
    const policyResponse = await post(policyHandler);
    expect(policyResponse.status).toBe(401);
    expect(await policyResponse.text()).not.toContain("peer-policy-secret");
  });

  test("the total deadline includes runtime resolution context", async () => {
    let providerCalled = false;
    const handler = createHttpHandler(protocol(), {
      peerResolutionTimeoutMs: 5,
      peerResolutionContext: () => new Promise(() => {}),
      peerIdentityProviders: [
        {
          provider: "spiffe",
          resolve: () => {
            providerCalled = true;
            return PeerIdentityResult.available(spiffeIdentity());
          },
        },
      ],
      peerAuthenticationPolicy: peerIdentityPrimary("spiffe"),
    });
    expect((await post(handler)).status).toBe(503);
    expect(providerCalled).toBe(false);
  });

  test("providers receive only the remaining total budget", async () => {
    let receivedBudget = Number.POSITIVE_INFINITY;
    const handler = createHttpHandler(protocol(), {
      peerResolutionTimeoutMs: 100,
      peerResolutionContext: async () => {
        await new Promise((resolve) => setTimeout(resolve, 10));
        return {};
      },
      peerIdentityProviders: [
        {
          provider: "spiffe",
          resolve: (context) => {
            receivedBudget = context.remainingBudgetMs()!;
            return PeerIdentityResult.available(spiffeIdentity());
          },
        },
      ],
      peerAuthenticationPolicy: peerIdentityPrimary("spiffe"),
    });
    expect((await post(handler)).status).toBe(200);
    expect(receivedBudget).toBeLessThan(100);
    expect(receivedBudget).toBeGreaterThan(0);
  });

  test("timed-out providers retain capacity until they actually exit", async () => {
    let calls = 0;
    const handler = createHttpHandler(protocol(), {
      peerResolutionTimeoutMs: 5,
      peerProviderConcurrency: 1,
      peerIdentityProviders: [
        {
          provider: "spiffe",
          resolve: () => {
            calls++;
            return new Promise(() => {});
          },
        },
      ],
      peerAuthenticationPolicy: observePeerIdentity,
    });
    expect((await post(handler)).status).toBe(200);
    expect((await post(handler)).status).toBe(200);
    expect(calls).toBe(1);
  });

  test("identity headers require raw multiplicity-preserving runtime input", async () => {
    const handler = createHttpHandler(protocol(), {
      peerResolutionContext: () => ({ headers: new Map([["X-Peer", ["alice", "mallory"]]]) }),
      peerIdentityProviders: [
        {
          provider: "proxy",
          resolve: (context) => {
            context.header("x-peer");
            return PeerIdentityResult.available(spiffeIdentity());
          },
        },
      ],
      peerAuthenticationPolicy: peerIdentityPrimary("proxy"),
    });
    expect((await post(handler)).status).toBe(401);
  });

  test("merged Fetch headers are invisible to identity providers", async () => {
    let observed: string | undefined = "not-called";
    const handler = createHttpHandler(protocol(), {
      peerIdentityProviders: [
        {
          provider: "spiffe",
          resolve: (context) => {
            observed = context.header("x-peer");
            return PeerIdentityResult.available(spiffeIdentity());
          },
        },
      ],
      peerAuthenticationPolicy: peerIdentityPrimary("spiffe"),
    });
    expect((await postWithHeaders(handler, { "X-Peer": "spoofed" })).status).toBe(200);
    expect(observed).toBeUndefined();
  });

  test("string-valued raw header adapters fail closed", async () => {
    let providerCalled = false;
    const handler = createHttpHandler(protocol(), {
      peerResolutionContext: () => ({ headers: { "X-Peer": "alice" } as unknown as Record<string, readonly string[]> }),
      peerIdentityProviders: [
        {
          provider: "spiffe",
          resolve: () => {
            providerCalled = true;
            return PeerIdentityResult.available(spiffeIdentity());
          },
        },
      ],
      peerAuthenticationPolicy: peerIdentityPrimary("spiffe"),
    });
    expect((await post(handler)).status).toBe(401);
    expect(providerCalled).toBe(false);
  });
});
