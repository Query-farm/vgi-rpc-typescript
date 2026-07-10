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
  const p = new Protocol("test-service");
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
