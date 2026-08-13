// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import { createHttpHandler, type LandingInfo, Protocol } from "../../src/index.js";

function makeHandler(prefix = "") {
  const protocol = new Protocol("LandingTest");
  protocol.unary("noop", { params: {}, result: {}, handler: async () => ({}) });

  const landingInfo: LandingInfo = {
    name: "LandingTest",
    doc: "A worker under test.",
    version: "0.0.0",
  };

  return createHttpHandler(protocol, { prefix, serverId: "srv123", landingInfo });
}

describe("VGI landing surface", () => {
  test("GET / serves the vendored landing.html for browsers", async () => {
    const handler = makeHandler();
    const resp = await handler(new Request("http://localhost/", { headers: { Accept: "text/html" } }));
    expect(resp.status).toBe(200);
    expect(resp.headers.get("Content-Type")).toContain("text/html");
    const body = await resp.text();
    expect(body).toContain("vgi-landing-asset v");
    expect(body).toContain('name="vgi-landing-version"');
  });

  // Worker identity is not catalog data and has no protocol method, so the
  // page reads it from the status document rather than over the wire.
  test("GET /?format=json carries the worker identity", async () => {
    const handler = makeHandler();
    const resp = await handler(new Request("http://localhost/?format=json"));
    expect(resp.status).toBe(200);
    expect(resp.headers.get("Content-Type")).toContain("application/json");
    const body = await resp.json();
    expect(body.status).toBe("ok");
    expect(body.server_id).toBe("srv123");
    expect(body.protocol).toBe("vgi");
    expect(body.worker).toBe("LandingTest");
    expect(body.doc).toBe("A worker under test.");
    expect(body.version).toBe("0.0.0");
    expect(body.lang).toBe("typescript");
    expect(body.cupola_base).toContain("cupola");
  });

  test("GET / with Accept: application/json returns the status document", async () => {
    const handler = makeHandler();
    const resp = await handler(new Request("http://localhost/", { headers: { Accept: "application/json" } }));
    expect(resp.status).toBe(200);
    expect((await resp.json()).worker).toBe("LandingTest");
  });

  // The page imports this same-origin; the worker serves it rather than the
  // page reaching for a CDN.
  test("GET /vgi-client.js serves the browser client build", async () => {
    const handler = makeHandler();
    const resp = await handler(new Request("http://localhost/vgi-client.js"));
    expect(resp.status).toBe(200);
    expect(resp.headers.get("Content-Type")).toContain("text/javascript");
    expect(resp.headers.get("Cache-Control")).toContain("max-age");
    const body = await resp.text();
    expect(body.length).toBeGreaterThan(1000);
    expect(body).toContain("VgiClient");
  });

  test("routes honour a non-empty prefix", async () => {
    const handler = makeHandler("/vgi");
    const page = await handler(new Request("http://localhost/vgi/", { headers: { Accept: "text/html" } }));
    expect(page.status).toBe(200);
    expect(await page.text()).toContain("vgi-landing-asset v");

    const bundle = await handler(new Request("http://localhost/vgi/vgi-client.js"));
    expect(bundle.status).toBe(200);
    expect(bundle.headers.get("Content-Type")).toContain("text/javascript");
  });

  // The vendored bundle is built in another repo and copied in, so it can fall
  // behind this server's wire behaviour without anything here failing to
  // compile. It did: the bundle predated `X-VGI-Content-Encoding` support by a
  // day, and on workerd — where the server always stamps that header, because
  // the Cloudflare edge re-gzips a standard `Content-Encoding` — the landing
  // page could not decode its own catalog response. Every CF-deployed VGI
  // worker showed "Could not load worker metadata" and nothing else noticed.
  //
  // These assert the bundle understands what this server can emit. They are
  // string checks on a minified artifact, which is crude, but the alternative
  // is running a browser against a workerd build in unit tests.
  test("the vendored client can decode every encoding this server stamps", async () => {
    const handler = makeHandler();
    const resp = await handler(new Request("http://localhost/vgi-client.js"));
    const bundle = await resp.text();

    // The header the server uses whenever a standard Content-Encoding would be
    // mangled in transit — the browser path and the whole of workerd.
    expect(bundle).toContain("X-VGI-Content-Encoding");
    // gzip is the only codec workerd can produce (CompressionStream does
    // gzip/deflate; there is no zstd encoder there), so a bundle that decodes
    // zstd alone is useless on Cloudflare.
    expect(bundle).toContain("DecompressionStream");
    expect(bundle.toLowerCase()).toContain("gzip");
  });

  // The describe document and its lazy column endpoint are gone: the page
  // reads the catalog over the protocol instead.
  test("the retired describe.json routes are not served", async () => {
    const handler = makeHandler();
    for (const path of ["/describe.json", "/describe/cat/main/t.json"]) {
      const resp = await handler(new Request("http://localhost" + path));
      expect(resp.status).not.toBe(200);
    }
  });
});
