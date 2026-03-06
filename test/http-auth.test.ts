// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import { RecordBatch, RecordBatchStreamWriter, recordBatchFromArrays, type Schema } from "@query-farm/apache-arrow";
import { AuthContext } from "../src/auth.js";
import { REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY } from "../src/constants.js";
import { buildWwwAuthenticateHeader, oauthResourceMetadataToJson, wellKnownPath } from "../src/http/auth.js";
import { ARROW_CONTENT_TYPE } from "../src/http/common.js";
import { createHttpHandler } from "../src/http/handler.js";
import { Protocol } from "../src/protocol.js";
import { str, toSchema } from "../src/schema.js";

function buildRequestIpc(schema: Schema, values: Record<string, any[]>, methodName: string): Uint8Array {
  const batch = recordBatchFromArrays(values, schema);
  const meta = new Map<string, string>();
  meta.set(RPC_METHOD_KEY, methodName);
  meta.set(REQUEST_VERSION_KEY, REQUEST_VERSION);
  const batchWithMeta = new RecordBatch(schema, batch.data, meta);
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, schema);
  writer.write(batchWithMeta);
  writer.close();
  return writer.toUint8Array(true);
}

function makeProtocol(): Protocol {
  const p = new Protocol("test-service");
  p.unary("echo", {
    params: { message: str },
    result: { message: str },
    handler: async (params, _ctx) => ({ message: params.message }),
  });
  return p;
}

function makeArrowBody(): Uint8Array {
  const schema = toSchema({ message: str });
  return buildRequestIpc(schema, { message: ["hello"] }, "echo");
}

describe("HTTP Auth", () => {
  test("handler without auth works as before", async () => {
    const handler = createHttpHandler(makeProtocol());
    const body = makeArrowBody();
    const resp = await handler(
      new Request("http://localhost/vgi/echo", {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );
    expect(resp.status).toBe(200);
  });

  test("handler with auth callback: success passes context", async () => {
    let capturedAuth: AuthContext | null = null;
    const p = new Protocol("test-service");
    p.unary("echo", {
      params: { message: str },
      result: { message: str },
      handler: async (params, ctx) => {
        capturedAuth = ctx.auth;
        return { message: params.message };
      },
    });

    const handler = createHttpHandler(p, {
      authenticate: async (_req) => {
        return new AuthContext("test", true, "alice", { sub: "alice" });
      },
    });

    const body = makeArrowBody();
    const resp = await handler(
      new Request("http://localhost/vgi/echo", {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );
    expect(resp.status).toBe(200);
    expect(capturedAuth).not.toBeNull();
    expect(capturedAuth!.authenticated).toBe(true);
    expect(capturedAuth!.principal).toBe("alice");
  });

  test("handler with auth callback: error returns 401", async () => {
    const handler = createHttpHandler(makeProtocol(), {
      authenticate: async () => {
        throw new Error("Invalid token");
      },
    });

    const body = makeArrowBody();
    const resp = await handler(
      new Request("http://localhost/vgi/echo", {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );
    expect(resp.status).toBe(401);
    const text = await resp.text();
    expect(text).toBe("Invalid token");
    expect(resp.headers.get("Content-Type")).toBe("text/plain");
  });

  test("well-known endpoint serves RFC 9728 JSON", async () => {
    const handler = createHttpHandler(makeProtocol(), {
      oauthResourceMetadata: {
        resource: "https://api.example.com/vgi",
        authorizationServers: ["https://auth.example.com"],
        scopesSupported: ["read", "write"],
      },
    });

    const resp = await handler(
      new Request("http://localhost/.well-known/oauth-protected-resource/vgi", {
        method: "GET",
      }),
    );
    expect(resp.status).toBe(200);
    expect(resp.headers.get("Content-Type")).toBe("application/json");
    expect(resp.headers.get("Cache-Control")).toBe("public, max-age=60");

    const json = await resp.json();
    expect(json.resource).toBe("https://api.example.com/vgi");
    expect(json.authorization_servers).toEqual(["https://auth.example.com"]);
    expect(json.scopes_supported).toEqual(["read", "write"]);
  });

  test("well-known endpoint bypasses auth", async () => {
    let authCalled = false;
    const handler = createHttpHandler(makeProtocol(), {
      authenticate: async () => {
        authCalled = true;
        throw new Error("Should not be called");
      },
      oauthResourceMetadata: {
        resource: "https://api.example.com/vgi",
        authorizationServers: ["https://auth.example.com"],
      },
    });

    const resp = await handler(
      new Request("http://localhost/.well-known/oauth-protected-resource/vgi", {
        method: "GET",
      }),
    );
    expect(resp.status).toBe(200);
    expect(authCalled).toBe(false);
  });

  test("well-known endpoint includes client_id when configured", async () => {
    const handler = createHttpHandler(makeProtocol(), {
      oauthResourceMetadata: {
        resource: "https://api.example.com/vgi",
        authorizationServers: ["https://auth.example.com"],
        clientId: "my-app",
      },
    });

    const resp = await handler(
      new Request("http://localhost/.well-known/oauth-protected-resource/vgi", {
        method: "GET",
      }),
    );
    expect(resp.status).toBe(200);
    const json = await resp.json();
    expect(json.client_id).toBe("my-app");
  });

  test("well-known endpoint includes client_secret when configured", async () => {
    const handler = createHttpHandler(makeProtocol(), {
      oauthResourceMetadata: {
        resource: "https://api.example.com/vgi",
        authorizationServers: ["https://auth.example.com"],
        clientSecret: "s3cret",
      },
    });

    const resp = await handler(
      new Request("http://localhost/.well-known/oauth-protected-resource/vgi", {
        method: "GET",
      }),
    );
    expect(resp.status).toBe(200);
    const json = await resp.json();
    expect(json.client_secret).toBe("s3cret");
  });

  test("well-known endpoint includes use_id_token_as_bearer when configured", async () => {
    const handler = createHttpHandler(makeProtocol(), {
      oauthResourceMetadata: {
        resource: "https://api.example.com/vgi",
        authorizationServers: ["https://auth.example.com"],
        useIdTokenAsBearer: true,
      },
    });

    const resp = await handler(
      new Request("http://localhost/.well-known/oauth-protected-resource/vgi", {
        method: "GET",
      }),
    );
    expect(resp.status).toBe(200);
    const json = await resp.json();
    expect(json.use_id_token_as_bearer).toBe(true);
  });

  test("401 WWW-Authenticate header includes client_secret and use_id_token_as_bearer", async () => {
    const handler = createHttpHandler(makeProtocol(), {
      authenticate: async () => {
        throw new Error("Unauthorized");
      },
      oauthResourceMetadata: {
        resource: "https://api.example.com/vgi",
        authorizationServers: ["https://auth.example.com"],
        clientId: "my-app",
        clientSecret: "s3cret",
        useIdTokenAsBearer: true,
      },
    });

    const body = makeArrowBody();
    const resp = await handler(
      new Request("http://localhost/vgi/echo", {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );
    expect(resp.status).toBe(401);
    const wwwAuth = resp.headers.get("WWW-Authenticate");
    expect(wwwAuth).toContain('client_secret="s3cret"');
    expect(wwwAuth).toContain('use_id_token_as_bearer="true"');
  });

  test("401 WWW-Authenticate header includes client_id when configured", async () => {
    const handler = createHttpHandler(makeProtocol(), {
      authenticate: async () => {
        throw new Error("Unauthorized");
      },
      oauthResourceMetadata: {
        resource: "https://api.example.com/vgi",
        authorizationServers: ["https://auth.example.com"],
        clientId: "my-app",
      },
    });

    const body = makeArrowBody();
    const resp = await handler(
      new Request("http://localhost/vgi/echo", {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );
    expect(resp.status).toBe(401);
    const wwwAuth = resp.headers.get("WWW-Authenticate");
    expect(wwwAuth).toContain('client_id="my-app"');
  });

  test("401 includes WWW-Authenticate header when metadata configured", async () => {
    const handler = createHttpHandler(makeProtocol(), {
      authenticate: async () => {
        throw new Error("Unauthorized");
      },
      oauthResourceMetadata: {
        resource: "https://api.example.com/vgi",
        authorizationServers: ["https://auth.example.com"],
      },
    });

    const body = makeArrowBody();
    const resp = await handler(
      new Request("http://localhost/vgi/echo", {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );
    expect(resp.status).toBe(401);
    const wwwAuth = resp.headers.get("WWW-Authenticate");
    expect(wwwAuth).toContain("Bearer");
    expect(wwwAuth).toContain("resource_metadata=");
    expect(wwwAuth).toContain("/.well-known/oauth-protected-resource/vgi");
  });

  test("CORS headers present on 401 responses", async () => {
    const handler = createHttpHandler(makeProtocol(), {
      corsOrigins: "*",
      authenticate: async () => {
        throw new Error("Unauthorized");
      },
    });

    const body = makeArrowBody();
    const resp = await handler(
      new Request("http://localhost/vgi/echo", {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );
    expect(resp.status).toBe(401);
    expect(resp.headers.get("Access-Control-Allow-Origin")).toBe("*");
  });
});

describe("oauthResourceMetadataToJson", () => {
  test("produces correct snake_case JSON with required fields", () => {
    const json = oauthResourceMetadataToJson({
      resource: "https://api.example.com",
      authorizationServers: ["https://auth.example.com"],
    });
    expect(json).toEqual({
      resource: "https://api.example.com",
      authorization_servers: ["https://auth.example.com"],
    });
  });

  test("includes optional fields when set", () => {
    const json = oauthResourceMetadataToJson({
      resource: "https://api.example.com",
      authorizationServers: ["https://auth.example.com"],
      scopesSupported: ["read"],
      resourceName: "My API",
    });
    expect(json.scopes_supported).toEqual(["read"]);
    expect(json.resource_name).toBe("My API");
    expect(json.bearer_methods_supported).toBeUndefined();
  });

  test("includes client_id when set", () => {
    const json = oauthResourceMetadataToJson({
      resource: "https://api.example.com",
      authorizationServers: ["https://auth.example.com"],
      clientId: "my-client-app",
    });
    expect(json.client_id).toBe("my-client-app");
  });

  test("omits client_id when not set", () => {
    const json = oauthResourceMetadataToJson({
      resource: "https://api.example.com",
      authorizationServers: ["https://auth.example.com"],
    });
    expect(json.client_id).toBeUndefined();
  });

  test("includes client_secret when set", () => {
    const json = oauthResourceMetadataToJson({
      resource: "https://api.example.com",
      authorizationServers: ["https://auth.example.com"],
      clientSecret: "s3cret",
    });
    expect(json.client_secret).toBe("s3cret");
  });

  test("throws on invalid client_secret characters", () => {
    expect(() =>
      oauthResourceMetadataToJson({
        resource: "https://api.example.com",
        authorizationServers: ["https://auth.example.com"],
        clientSecret: "bad secret!",
      }),
    ).toThrow("Invalid client_secret");
  });

  test("includes use_id_token_as_bearer when true", () => {
    const json = oauthResourceMetadataToJson({
      resource: "https://api.example.com",
      authorizationServers: ["https://auth.example.com"],
      useIdTokenAsBearer: true,
    });
    expect(json.use_id_token_as_bearer).toBe(true);
  });

  test("omits use_id_token_as_bearer when false or unset", () => {
    const json = oauthResourceMetadataToJson({
      resource: "https://api.example.com",
      authorizationServers: ["https://auth.example.com"],
      useIdTokenAsBearer: false,
    });
    expect(json.use_id_token_as_bearer).toBeUndefined();
  });

  test("throws on invalid client_id characters", () => {
    expect(() =>
      oauthResourceMetadataToJson({
        resource: "https://api.example.com",
        authorizationServers: ["https://auth.example.com"],
        clientId: "bad client id!",
      }),
    ).toThrow("Invalid client_id");
  });
});

describe("wellKnownPath", () => {
  test("returns correct path", () => {
    expect(wellKnownPath("/vgi")).toBe("/.well-known/oauth-protected-resource/vgi");
    expect(wellKnownPath("/api/v1")).toBe("/.well-known/oauth-protected-resource/api/v1");
  });
});

describe("buildWwwAuthenticateHeader", () => {
  test("without metadata URL", () => {
    expect(buildWwwAuthenticateHeader()).toBe("Bearer");
  });

  test("with metadata URL", () => {
    const header = buildWwwAuthenticateHeader("https://example.com/.well-known/oauth-protected-resource/vgi");
    expect(header).toBe('Bearer resource_metadata="https://example.com/.well-known/oauth-protected-resource/vgi"');
  });

  test("with metadata URL and client_id", () => {
    const header = buildWwwAuthenticateHeader("https://example.com/.well-known/oauth-protected-resource/vgi", "my-client");
    expect(header).toBe('Bearer resource_metadata="https://example.com/.well-known/oauth-protected-resource/vgi", client_id="my-client"');
  });

  test("with client_id only", () => {
    const header = buildWwwAuthenticateHeader(undefined, "my-client");
    expect(header).toBe('Bearer, client_id="my-client"');
  });

  test("with client_secret", () => {
    const header = buildWwwAuthenticateHeader(undefined, undefined, "s3cret");
    expect(header).toBe('Bearer, client_secret="s3cret"');
  });

  test("with use_id_token_as_bearer", () => {
    const header = buildWwwAuthenticateHeader(undefined, undefined, undefined, true);
    expect(header).toBe('Bearer, use_id_token_as_bearer="true"');
  });

  test("with all parameters", () => {
    const header = buildWwwAuthenticateHeader("https://example.com/meta", "my-client", "s3cret", true);
    expect(header).toBe('Bearer resource_metadata="https://example.com/meta", client_id="my-client", client_secret="s3cret", use_id_token_as_bearer="true"');
  });
});
