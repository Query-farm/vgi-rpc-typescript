// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import { AuthContext } from "../src/auth.js";
import { PeerEvidenceSet } from "../src/identity.js";
import { irohHttpBridgeOptions } from "../src/iroh.js";

describe("Iroh worker helpers", () => {
  test("HTTP bridge defaults authenticate one loopback-forwarded EndpointId", async () => {
    const peerResolutionContext = () => ({ immediatePeer: "127.0.0.1", headers: {} });
    const options = irohHttpBridgeOptions({ issuer: "test-mesh", peerResolutionContext });
    expect(options.peerIdentityProviders).toHaveLength(1);
    expect(options.peerResolutionContext).toBe(peerResolutionContext);
    expect(options.peerAuthenticationPolicy).toBeDefined();
    await expect(options.peerAuthenticationPolicy?.(PeerEvidenceSet.EMPTY, AuthContext.anonymous())).rejects.toThrow();
  });
});
