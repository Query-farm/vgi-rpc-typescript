// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import { type ClientExpectation, clientExpectation, validateSnapshot } from "../conformance/tailnet.js";

const expectation: ClientExpectation = {
  evidenceSource: "localapi",
  assurance: "local_daemon",
  issuer: "tailnet:test",
  subjectKind: "tagged_node",
  subjectStability: "stable",
  capability: "query.farm/run",
  tag: "tag:vgi-client",
  targetKind: "destination_ip",
  authenticated: true,
  proxyPresent: false,
};

function snapshot(overrides: Record<string, unknown> = {}): string {
  return JSON.stringify({
    provider_status: { tailscale: "available" },
    identities: [
      {
        provider: "tailscale",
        evidence_source: "localapi",
        assurance: "local_daemon",
        issuer: "tailnet:test",
        subject_kind: "tagged_node",
        subject_stability: "stable",
        subject_verified: true,
        subject_fingerprint: "redacted-digest",
        tags: ["tag:vgi-client"],
        capability_names: ["query.farm/run"],
        capabilities_verified: true,
        capability_target: { kind: "destination_ip" },
        proxy_present: false,
        ...overrides,
      },
    ],
    auth: {
      authenticated: true,
      domain: "tailscale",
      principal_matches_identity: true,
      peer_evidence_binding_present: true,
    },
  });
}

describe("live Tailnet adapter", () => {
  test("accepts the redacted issuer, target, and authentication contract", () => {
    expect(() => validateSnapshot(snapshot(), expectation)).not.toThrow();
  });

  test("rejects issuer and target qualification drift", () => {
    expect(() => validateSnapshot(snapshot({ issuer: "tailnet:other" }), expectation)).toThrow();
    expect(() => validateSnapshot(snapshot({ capability_target: { kind: "service" } }), expectation)).toThrow();
  });

  test("parses the shared cross-language expectation flags", () => {
    expect(
      clientExpectation([
        "--expected-evidence-source",
        "serve_proxy",
        "--expected-assurance",
        "configured_proxy",
        "--expected-issuer",
        "tailnet:test",
        "--expected-subject-kind",
        "unknown",
        "--expected-subject-stability",
        "none",
        "--expected-capability",
        "query.farm/run",
        "--expect-proxy",
        "--spoof-login",
        "attacker@example.invalid",
      ]),
    ).toEqual({
      evidenceSource: "serve_proxy",
      assurance: "configured_proxy",
      issuer: "tailnet:test",
      subjectKind: "unknown",
      subjectStability: "none",
      capability: "query.farm/run",
      tag: undefined,
      targetKind: undefined,
      authenticated: false,
      proxyPresent: true,
      spoofLogin: "attacker@example.invalid",
    });
  });
});
