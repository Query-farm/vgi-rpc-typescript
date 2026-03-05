// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import { AuthContext } from "../src/auth.js";
import { httpConnect } from "../src/client/connect.js";
import { parseResourceMetadataUrl } from "../src/client/oauth.js";
import { RpcError } from "../src/errors.js";
import { createHttpHandler } from "../src/http/handler.js";
import { Protocol } from "../src/protocol.js";
import { str } from "../src/schema.js";

describe("parseResourceMetadataUrl", () => {
  test("extracts URL from Bearer challenge", () => {
    const url = parseResourceMetadataUrl(
      'Bearer resource_metadata="https://example.com/.well-known/oauth-protected-resource/vgi"',
    );
    expect(url).toBe("https://example.com/.well-known/oauth-protected-resource/vgi");
  });

  test("returns null for non-Bearer headers", () => {
    const url = parseResourceMetadataUrl('Basic realm="test"');
    expect(url).toBeNull();
  });

  test("returns null for Bearer without resource_metadata", () => {
    const url = parseResourceMetadataUrl('Bearer realm="test"');
    expect(url).toBeNull();
  });
});

describe("Client auth", () => {
  test("client sends Authorization header when configured", async () => {
    let receivedAuth: string | null = null;

    const p = new Protocol("test-service");
    p.unary("echo", {
      params: { message: str },
      result: { message: str },
      handler: async (params) => ({ message: params.message }),
    });

    const handler = createHttpHandler(p, {
      authenticate: async (req) => {
        receivedAuth = req.headers.get("Authorization");
        return new AuthContext("test", true, "alice", {});
      },
    });

    const server = Bun.serve({ port: 0, fetch: handler });
    try {
      const client = httpConnect(`http://localhost:${server.port}`, {
        authorization: "Bearer test-token-123",
      });
      const result = await client.call("echo", { message: "hello" });
      expect(result).toEqual({ message: "hello" });
      expect(receivedAuth).toBe("Bearer test-token-123");
      client.close();
    } finally {
      server.stop(true);
    }
  });

  test("client throws AuthenticationError on 401", async () => {
    const p = new Protocol("test-service");
    p.unary("echo", {
      params: { message: str },
      result: { message: str },
      handler: async (params) => ({ message: params.message }),
    });

    const handler = createHttpHandler(p, {
      authenticate: async () => {
        throw new Error("Invalid token");
      },
    });

    const server = Bun.serve({ port: 0, fetch: handler });
    try {
      const client = httpConnect(`http://localhost:${server.port}`);
      try {
        await client.call("echo", { message: "hello" });
        expect(true).toBe(false); // should not reach here
      } catch (e: any) {
        expect(e).toBeInstanceOf(RpcError);
        expect(e.errorType).toBe("AuthenticationError");
      }
      client.close();
    } finally {
      server.stop(true);
    }
  });
});
