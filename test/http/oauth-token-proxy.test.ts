// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { createHttpHandler, Protocol, str } from "../../src/index.js";

function makeProtocol(): Protocol {
  const protocol = new Protocol("EchoService");
  protocol.unary("echo", {
    params: { x: str },
    result: { x: str },
    handler: async ({ x }) => ({ x }),
  });
  return protocol;
}

const RESOURCE = "http://localhost:8000/vgi";
const PREFIX = "/vgi";
const TOKEN_ENDPOINT_REAL = "https://auth.example.com/token";

function makePkceHandler() {
  return createHttpHandler(makeProtocol(), {
    prefix: PREFIX,
    serverId: "test-server",
    authenticate: async () => {
      throw new Error("unauth");
    },
    oauthResourceMetadata: {
      resource: RESOURCE,
      authorizationServers: ["https://auth.example.com"],
      clientId: "my-client-id",
      clientSecret: "my-client-secret",
    },
  });
}

interface FetchCapture {
  url?: string;
  body?: string;
}

function installFetchMock(opts: { capture?: FetchCapture; upstreamStatus?: number; upstreamBody?: string }) {
  const originalFetch = globalThis.fetch;
  const captured = opts.capture;
  globalThis.fetch = (async (input: any, init?: any) => {
    const url = typeof input === "string" ? input : input.url;
    if (url.endsWith("/.well-known/openid-configuration")) {
      return new Response(
        JSON.stringify({
          issuer: "https://auth.example.com",
          authorization_endpoint: "https://auth.example.com/authorize",
          token_endpoint: TOKEN_ENDPOINT_REAL,
        }),
        { headers: { "Content-Type": "application/json" } },
      );
    }
    if (url === TOKEN_ENDPOINT_REAL) {
      if (captured) {
        captured.url = url;
        captured.body = String(init?.body ?? "");
      }
      return new Response(opts.upstreamBody ?? '{"access_token":"new","token_type":"Bearer","expires_in":3600}', {
        status: opts.upstreamStatus ?? 200,
        headers: { "Content-Type": "application/json" },
      });
    }
    return originalFetch(input, init);
  }) as typeof fetch;
  return () => {
    globalThis.fetch = originalFetch;
  };
}

describe("OAuth token proxy", () => {
  let restoreFetch: () => void = () => {};
  afterEach(() => restoreFetch());

  test("well-known advertises token_endpoint pointing at the proxy", async () => {
    restoreFetch = installFetchMock({});
    const handler = makePkceHandler();
    const resp = await handler(
      new Request(`http://localhost${PREFIX === "" ? "" : "/.well-known/oauth-protected-resource" + PREFIX}`),
    );
    expect(resp.status).toBe(200);
    const json: any = await resp.json();
    expect(json.token_endpoint).toBe(`http://localhost:8000${PREFIX}/_oauth/token`);
  });

  test("OPTIONS returns 204 with CORS headers when Origin is allowed", async () => {
    restoreFetch = installFetchMock({});
    const handler = makePkceHandler();
    const resp = await handler(
      new Request(`http://localhost${PREFIX}/_oauth/token`, {
        method: "OPTIONS",
        headers: { Origin: "https://cupola.query-farm.services" },
      }),
    );
    expect(resp.status).toBe(204);
    expect(resp.headers.get("access-control-allow-origin")).toBe("https://cupola.query-farm.services");
    expect(resp.headers.get("access-control-allow-methods")).toContain("POST");
  });

  test("authorization_code is forwarded with injected client_secret", async () => {
    const captured: FetchCapture = {};
    restoreFetch = installFetchMock({ capture: captured });
    const handler = makePkceHandler();
    const body = new URLSearchParams({
      grant_type: "authorization_code",
      code: "abc",
      code_verifier: "v",
      redirect_uri: "https://x/cb",
      client_id: "my-client-id",
    }).toString();
    const resp = await handler(
      new Request(`http://localhost${PREFIX}/_oauth/token`, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body,
      }),
    );
    expect(resp.status).toBe(200);
    const upstream = new URLSearchParams(captured.body!);
    expect(upstream.get("client_secret")).toBe("my-client-secret");
    expect(upstream.get("client_id")).toBe("my-client-id");
    expect(upstream.get("code")).toBe("abc");
    expect(upstream.get("code_verifier")).toBe("v");
    expect(upstream.get("redirect_uri")).toBe("https://x/cb");
  });

  test("refresh_token is forwarded with injected client_secret", async () => {
    const captured: FetchCapture = {};
    restoreFetch = installFetchMock({ capture: captured });
    const handler = makePkceHandler();
    const body = new URLSearchParams({
      grant_type: "refresh_token",
      refresh_token: "rtok",
      client_id: "my-client-id",
      scope: "openid",
    }).toString();
    const resp = await handler(
      new Request(`http://localhost${PREFIX}/_oauth/token`, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body,
      }),
    );
    expect(resp.status).toBe(200);
    const upstream = new URLSearchParams(captured.body!);
    expect(upstream.get("grant_type")).toBe("refresh_token");
    expect(upstream.get("refresh_token")).toBe("rtok");
    expect(upstream.get("scope")).toBe("openid");
    expect(upstream.get("client_secret")).toBe("my-client-secret");
  });

  test("mismatched client_id is rejected with 400", async () => {
    restoreFetch = installFetchMock({});
    const handler = makePkceHandler();
    const body = new URLSearchParams({
      grant_type: "authorization_code",
      code: "abc",
      code_verifier: "v",
      redirect_uri: "https://x/cb",
      client_id: "evil",
    }).toString();
    const resp = await handler(
      new Request(`http://localhost${PREFIX}/_oauth/token`, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body,
      }),
    );
    expect(resp.status).toBe(400);
    const json: any = await resp.json();
    expect(json.error).toBe("invalid_client");
  });

  test("unsupported grant_type is rejected with 400", async () => {
    restoreFetch = installFetchMock({});
    const handler = makePkceHandler();
    const body = "grant_type=client_credentials";
    const resp = await handler(
      new Request(`http://localhost${PREFIX}/_oauth/token`, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body,
      }),
    );
    expect(resp.status).toBe(400);
    const json: any = await resp.json();
    expect(json.error).toBe("unsupported_grant_type");
  });

  test("IdP error response is forwarded verbatim", async () => {
    restoreFetch = installFetchMock({
      upstreamStatus: 400,
      upstreamBody: '{"error":"invalid_grant","error_description":"bad code"}',
    });
    const handler = makePkceHandler();
    const body = new URLSearchParams({
      grant_type: "authorization_code",
      code: "bad",
      code_verifier: "v",
      redirect_uri: "https://x/cb",
    }).toString();
    const resp = await handler(
      new Request(`http://localhost${PREFIX}/_oauth/token`, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body,
      }),
    );
    expect(resp.status).toBe(400);
    const json: any = await resp.json();
    expect(json.error).toBe("invalid_grant");
    expect(json.error_description).toBe("bad code");
  });

  test("non-form Content-Type is rejected with 415", async () => {
    restoreFetch = installFetchMock({});
    const handler = makePkceHandler();
    const resp = await handler(
      new Request(`http://localhost${PREFIX}/_oauth/token`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: '{"grant_type":"authorization_code"}',
      }),
    );
    expect(resp.status).toBe(415);
  });
});
