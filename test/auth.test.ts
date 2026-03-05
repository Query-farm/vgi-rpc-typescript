// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import { AuthContext } from "../src/auth.js";
import { RpcError } from "../src/errors.js";

describe("AuthContext", () => {
  test("anonymous() returns unauthenticated context", () => {
    const ctx = AuthContext.anonymous();
    expect(ctx.authenticated).toBe(false);
    expect(ctx.principal).toBeNull();
    expect(ctx.domain).toBe("");
    expect(ctx.claims).toEqual({});
  });

  test("requireAuthenticated() throws for anonymous", () => {
    const ctx = AuthContext.anonymous();
    expect(() => ctx.requireAuthenticated()).toThrow(RpcError);
    expect(() => ctx.requireAuthenticated()).toThrow("Authentication required");
  });

  test("requireAuthenticated() succeeds for authenticated context", () => {
    const ctx = new AuthContext("jwt", true, "user@example.com", {
      sub: "user@example.com",
      iss: "https://auth.example.com",
    });
    expect(() => ctx.requireAuthenticated()).not.toThrow();
  });

  test("claims/principal/domain access", () => {
    const claims = { sub: "alice", role: "admin", iss: "https://issuer.example.com" };
    const ctx = new AuthContext("jwt", true, "alice", claims);
    expect(ctx.domain).toBe("jwt");
    expect(ctx.authenticated).toBe(true);
    expect(ctx.principal).toBe("alice");
    expect(ctx.claims).toEqual(claims);
    expect(ctx.claims.role).toBe("admin");
  });
});
