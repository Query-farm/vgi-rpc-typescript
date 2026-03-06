// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import { AuthContext } from "../src/auth.js";
import { httpConnect } from "../src/client/connect.js";
import { parseClientId, parseClientSecret, parseResourceMetadataUrl, parseUseIdTokenAsBearer } from "../src/client/oauth.js";
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

describe("parseClientId", () => {
  test("extracts client_id from Bearer challenge", () => {
    const id = parseClientId(
      'Bearer resource_metadata="https://example.com/.well-known/oauth-protected-resource/vgi", client_id="my-app"',
    );
    expect(id).toBe("my-app");
  });

  test("returns null when client_id absent", () => {
    const id = parseClientId('Bearer resource_metadata="https://example.com/foo"');
    expect(id).toBeNull();
  });

  test("returns null for non-Bearer headers", () => {
    const id = parseClientId('Basic realm="test"');
    expect(id).toBeNull();
  });
});

describe("parseClientSecret", () => {
  test("extracts client_secret from Bearer challenge", () => {
    const secret = parseClientSecret(
      'Bearer resource_metadata="https://example.com/meta", client_id="my-app", client_secret="s3cret"',
    );
    expect(secret).toBe("s3cret");
  });

  test("returns null when client_secret absent", () => {
    const secret = parseClientSecret('Bearer resource_metadata="https://example.com/foo"');
    expect(secret).toBeNull();
  });

  test("returns null for non-Bearer headers", () => {
    const secret = parseClientSecret('Basic realm="test"');
    expect(secret).toBeNull();
  });
});

describe("parseUseIdTokenAsBearer", () => {
  test("returns true when use_id_token_as_bearer is true", () => {
    const result = parseUseIdTokenAsBearer(
      'Bearer resource_metadata="https://example.com/meta", use_id_token_as_bearer="true"',
    );
    expect(result).toBe(true);
  });

  test("returns false when use_id_token_as_bearer is absent", () => {
    const result = parseUseIdTokenAsBearer('Bearer resource_metadata="https://example.com/foo"');
    expect(result).toBe(false);
  });

  test("returns false for non-Bearer headers", () => {
    const result = parseUseIdTokenAsBearer('Basic realm="test"');
    expect(result).toBe(false);
  });

  test("returns false when value is not true", () => {
    const result = parseUseIdTokenAsBearer('Bearer use_id_token_as_bearer="false"');
    expect(result).toBe(false);
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
