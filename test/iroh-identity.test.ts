// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import { AuthContext } from "../src/auth.js";
import { PeerEvidenceSet, PeerIdentityStatus, PeerResolutionContext, peerIdentityPrimary } from "../src/identity.js";
import { IROH_FORWARDED_ENDPOINT_HEADER, irohForwardedHeaderIdentityProvider } from "../src/iroh.js";

const ENDPOINT = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

function context(peer: string, values: readonly string[]): PeerResolutionContext {
  return new PeerResolutionContext("http", {
    immediatePeer: peer,
    headers: { [IROH_FORWARDED_ENDPOINT_HEADER]: values },
  });
}

describe("Iroh forwarded HTTP identity", () => {
  const provider = irohForwardedHeaderIdentityProvider({
    issuer: "production-mesh",
    trustedProxyAddresses: ["127.0.0.1"],
  });

  test("creates a stable locally namespaced identity", async () => {
    const result = await provider.resolve(context("127.0.0.1", [ENDPOINT]));
    expect(result.status).toBe(PeerIdentityStatus.AVAILABLE);
    expect(result.identities[0]).toMatchObject({
      issuer: "production-mesh",
      subjectKey: ENDPOINT,
      attributes: { original_assurance: "cryptographic_peer" },
    });
    const auth = await peerIdentityPrimary("iroh")(new PeerEvidenceSet([result]), AuthContext.anonymous());
    expect(auth.authenticated).toBe(true);
    expect(auth.principal).toContain("/production-mesh/");
  });

  test("rejects untrusted, missing, duplicate, and non-canonical evidence", async () => {
    for (const [peer, values, status] of [
      ["192.0.2.1", [ENDPOINT], PeerIdentityStatus.UNTRUSTED_PROXY],
      ["127.0.0.1", [], PeerIdentityStatus.NO_MATCH],
      ["127.0.0.1", [ENDPOINT, ENDPOINT], PeerIdentityStatus.INVALID],
      ["127.0.0.1", [ENDPOINT.toUpperCase()], PeerIdentityStatus.INVALID],
      ["127.0.0.1", ["00"], PeerIdentityStatus.INVALID],
    ] as const) {
      expect((await provider.resolve(context(peer, values))).status).toBe(status);
    }
  });
});
