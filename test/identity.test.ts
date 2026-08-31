// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import { AuthContext } from "../src/auth.js";
import {
  allOfPeerIdentities,
  anyOfPeerIdentities,
  IdentityAssurance,
  PeerEvidenceSet,
  PeerIdentity,
  PeerIdentityResult,
  PeerIdentityStatus,
  PeerResolutionContext,
  PeerSubjectKind,
  peerIdentityPrimary,
  requirePeerIdentity,
  SubjectStability,
} from "../src/identity.js";

function identity(provider = "spiffe", subject = "spiffe://example.org/workload"): PeerIdentity {
  return new PeerIdentity({
    provider,
    evidenceSource: "test",
    assurance: IdentityAssurance.CRYPTOGRAPHIC_PEER,
    issuer: "spiffe://example.org",
    transport: "tcp",
    subjectKind: PeerSubjectKind.WORKLOAD,
    subjectKey: subject,
    subjectStability: SubjectStability.STABLE,
    subjectVerified: true,
  });
}

describe("transport peer identity", () => {
  test("matches the shared principal and binding vector", async () => {
    const peer = identity();
    const evidence = new PeerEvidenceSet([PeerIdentityResult.available(peer)]);
    expect(peer.canonicalPrincipal).toBe(
      "peer/spiffe/spiffe%3A%2F%2Fexample.org/spiffe%3A%2F%2Fexample.org%2Fworkload",
    );
    expect(await evidence.bindingDigest(["spiffe"])).toBe(
      "948ce118ddd5f212e7bfd62e13ffdba0675397c56a43060e98656965389e5367",
    );
  });

  test("binding ignores routing topology but not capabilities", async () => {
    const withTopology = (sourceAddress: string, proxyAddress: string, capabilities = {}) =>
      new PeerIdentity({
        provider: "spiffe",
        evidenceSource: "test",
        assurance: IdentityAssurance.CRYPTOGRAPHIC_PEER,
        issuer: "spiffe://example.org",
        transport: "tcp",
        subjectKind: PeerSubjectKind.WORKLOAD,
        subjectKey: "spiffe://example.org/workload",
        subjectStability: SubjectStability.STABLE,
        subjectVerified: true,
        capabilities,
        sourceAddress,
        proxyAddress,
      });
    const first = new PeerEvidenceSet([PeerIdentityResult.available(withTopology("100.64.0.1:40001", "10.0.0.10"))]);
    const second = new PeerEvidenceSet([PeerIdentityResult.available(withTopology("100.64.0.1:49999", "10.0.0.11"))]);
    expect(await first.bindingDigest(["spiffe"])).toBe(await second.bindingDigest(["spiffe"]));
    const changed = new PeerEvidenceSet([
      PeerIdentityResult.available(withTopology("100.64.0.1:49999", "10.0.0.11", { "query.farm/run": [] })),
    ]);
    expect(await first.bindingDigest(["spiffe"])).not.toBe(await changed.bindingDigest(["spiffe"]));
  });

  test("deeply snapshots structured evidence", () => {
    const attributes = { roles: ["reader"] };
    const peer = new PeerIdentity({
      provider: "test",
      evidenceSource: "test",
      assurance: IdentityAssurance.LOCAL_DAEMON,
      issuer: "test://issuer",
      transport: "tcp",
      attributes,
    });
    attributes.roles[0] = "writer";
    expect(peer.attributes.roles).toEqual(["reader"]);
    expect(Object.isFrozen(peer.attributes.roles)).toBe(true);
  });

  test("rejects unknown statuses instead of treating them as absent evidence", () => {
    expect(() => new PeerIdentityResult("future", "future_status" as PeerIdentityStatus)).toThrow(
      "invalid peer identity status",
    );
    expect(
      () =>
        new PeerEvidenceSet([
          { provider: "future", status: "future_status", identities: [] } as unknown as PeerIdentityResult,
        ]),
    ).toThrow("invalid peer identity status");
  });

  test("rejects malformed UTF-16 before canonicalization", () => {
    expect(
      () =>
        new PeerIdentity({
          provider: "bad\ud800",
          evidenceSource: "test",
          assurance: IdentityAssurance.LOCAL_DAEMON,
          issuer: "test://issuer",
          transport: "tcp",
        }),
    ).toThrow("unpaired surrogate");
    expect(() => new PeerResolutionContext("http", { authority: "bad\udc00" })).toThrow("unpaired surrogate");
  });

  test("bounds structured evidence size, depth, and value count", () => {
    const base = {
      provider: "test",
      evidenceSource: "test",
      assurance: IdentityAssurance.LOCAL_DAEMON,
      issuer: "test://issuer",
      transport: "tcp",
    } as const;
    expect(() => new PeerIdentity({ ...base, attributes: { value: "x".repeat(65_537) } })).toThrow("byte size");

    let nested: Record<string, unknown> = {};
    for (let i = 0; i < 17; i++) nested = { child: nested };
    expect(() => new PeerIdentity({ ...base, attributes: nested })).toThrow("depth");

    expect(
      () => new PeerIdentity({ ...base, attributes: { values: Array.from({ length: 4_096 }, () => null) } }),
    ).toThrow("value count");
  });

  test("remaining provider budget decreases on a monotonic clock", async () => {
    const context = new PeerResolutionContext("http", { budgetMs: 100 });
    const before = context.remainingBudgetMs()!;
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(context.remainingBudgetMs()!).toBeLessThan(before);
  });

  test("any-of skips unavailable and subjectless available factors", async () => {
    const subjectless = new PeerIdentity({
      provider: "capabilities",
      evidenceSource: "test",
      assurance: IdentityAssurance.LOCAL_DAEMON,
      issuer: "test://issuer",
      transport: "tcp",
    });
    const evidence = new PeerEvidenceSet([
      new PeerIdentityResult("first", PeerIdentityStatus.UNAVAILABLE),
      PeerIdentityResult.available(subjectless),
      PeerIdentityResult.available(identity("second")),
    ]);
    const auth = await anyOfPeerIdentities("first", "capabilities", "second")(evidence, AuthContext.anonymous());
    expect(auth.domain).toBe("second");
  });

  test("any-of rejects ambiguity before application fallback", async () => {
    const evidence = new PeerEvidenceSet([
      new PeerIdentityResult("spiffe", PeerIdentityStatus.AVAILABLE, [
        identity("spiffe", "spiffe://example.org/one"),
        identity("spiffe", "spiffe://example.org/two"),
      ]),
    ]);
    expect(anyOfPeerIdentities("spiffe")(evidence, new AuthContext("bearer", true, "alice"))).rejects.toThrow(
      "ambiguous",
    );
  });

  test("all-of binds application identity", async () => {
    const evidence = new PeerEvidenceSet([PeerIdentityResult.available(identity())]);
    const policy = allOfPeerIdentities(["spiffe"], () => {});
    const alice = await policy(evidence, new AuthContext("bearer", true, "alice"));
    const bob = await policy(evidence, new AuthContext("bearer", true, "bob"));
    expect(alice.claims.peer_evidence_binding).not.toBe(bob.claims.peer_evidence_binding);
  });

  test("require accepts capability-only evidence but primary rejects it", async () => {
    const capabilityOnly = new PeerIdentity({
      provider: "tailscale",
      evidenceSource: "serve",
      assurance: IdentityAssurance.CONFIGURED_PROXY,
      issuer: "tailnet:test",
      transport: "http",
      capabilities: { "query.farm/can-run": [{ worker: "analytics" }] },
      capabilitiesVerified: true,
    });
    const evidence = new PeerEvidenceSet([PeerIdentityResult.available(capabilityOnly)]);
    const application = new AuthContext("bearer", true, "alice");
    const required = await requirePeerIdentity("tailscale")(evidence, application);
    expect(required.authenticated).toBe(true);
    expect(required.principal).toBe("alice");
    expect(peerIdentityPrimary("tailscale")(evidence, AuthContext.anonymous())).rejects.toThrow("stable subject");
  });
});
