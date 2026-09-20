// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// `/health` is the mandatory, auth-exempt capability-discovery endpoint. The
// C++ client probes it with HEAD, the Python client with OPTIONS/GET — so the
// capability headers must be verb-independent.

import { describe, expect, test } from "bun:test";
import { createHttpHandler } from "../../src/http/handler.js";
import { Protocol } from "../../src/protocol.js";
import { str } from "../../src/schema.js";

function makeHandler() {
  const p = new Protocol("test.Service.v1");
  p.unary("echo", {
    params: { message: str },
    result: { message: str },
    handler: async (params) => ({ message: params.message }),
  });
  return createHttpHandler(p);
}

const CAPABILITY_HEADER = "VGI-Supported-Encodings";

describe("HEAD /health", () => {
  test("answers 200 with no body", async () => {
    const resp = await makeHandler()(new Request("http://localhost/health", { method: "HEAD" }));
    expect(resp.status).toBe(200);
    expect(resp.headers.get("Content-Type")).toBe("application/json");
    expect(await resp.text()).toBe("");
  });

  test("reports GET's Content-Length", async () => {
    const handler = makeHandler();
    const getResp = await handler(new Request("http://localhost/health", { method: "GET" }));
    const getBody = await getResp.text();

    const headResp = await handler(new Request("http://localhost/health", { method: "HEAD" }));
    expect(headResp.headers.get("Content-Length")).toBe(String(new TextEncoder().encode(getBody).byteLength));
  });

  test("exposes the same capability headers as GET", async () => {
    const handler = makeHandler();
    const getResp = await handler(new Request("http://localhost/health", { method: "GET" }));
    const headResp = await handler(new Request("http://localhost/health", { method: "HEAD" }));
    expect(headResp.headers.get(CAPABILITY_HEADER)).toBe(getResp.headers.get(CAPABILITY_HEADER));
  });

  test("GET still returns the health body", async () => {
    const resp = await makeHandler()(new Request("http://localhost/health", { method: "GET" }));
    expect(resp.status).toBe(200);
    expect(JSON.parse(await resp.text()).status).toBe("ok");
  });
});

// A browser's capability probe is preflighted, because it carries
// VGI-Accept-Max-Response-Bytes. The preflight is refused unless the method the
// client is about to use appears in Access-Control-Allow-Methods — so the list
// advertised here is what decides whether a browser can discover capabilities
// at all. It used to say only "POST, OPTIONS", which refused the probe itself.
describe("CORS preflight for the capability probe", () => {
  function corsHandler() {
    const p = new Protocol("test.Service.v1");
    p.unary("echo", {
      params: { message: str },
      result: { message: str },
      handler: async (params) => ({ message: params.message }),
    });
    return createHttpHandler(p, { corsOrigins: "*" });
  }

  test("allows the methods /health actually answers", async () => {
    const resp = await corsHandler()(
      new Request("http://localhost/health", {
        method: "OPTIONS",
        headers: {
          Origin: "https://example.test",
          "Access-Control-Request-Method": "HEAD",
          "Access-Control-Request-Headers": "vgi-accept-max-response-bytes",
        },
      }),
    );
    const allowed = (resp.headers.get("Access-Control-Allow-Methods") ?? "")
      .split(",")
      .map((method) => method.trim());
    expect(allowed).toContain("HEAD");
    expect(allowed).toContain("GET");
    // RPC itself is POST, and the preflight verb stays allowed.
    expect(allowed).toContain("POST");
    expect(allowed).toContain("OPTIONS");
  });

  test("echoes the probe's custom request header", async () => {
    const resp = await corsHandler()(
      new Request("http://localhost/health", {
        method: "OPTIONS",
        headers: {
          Origin: "https://example.test",
          "Access-Control-Request-Method": "HEAD",
          "Access-Control-Request-Headers": "vgi-accept-max-response-bytes",
        },
      }),
    );
    expect(resp.headers.get("Access-Control-Allow-Headers")).toContain(
      "vgi-accept-max-response-bytes",
    );
  });
});
