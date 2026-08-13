// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Duplicate slashes in a request path.
 *
 * A client that joins a base URL already ending in "/" with "/<method>" sends
 * "//<method>". With an empty prefix the router used to slice exactly one
 * character off and dispatch the method name "/<method>", which matches
 * nothing — reported from the field as
 *
 *   Unknown method: '/__describe__'. Available methods: [aggregate_bind, ...]
 *
 * where every name in that list is unprefixed, so the leading slash was the
 * whole story. Health, the landing surface and the client bundle 404'd the
 * same way. Both halves are covered here: the server tolerates the doubled
 * path, and the client no longer produces one.
 */

import { describe, expect, test } from "bun:test";
import { createHttpHandler, float, Protocol } from "../../src/index.js";

function handlerFor(prefix?: string) {
  const protocol = new Protocol("PathSvc");
  protocol.unary("double", {
    params: { x: float },
    result: { y: float },
    handler: async ({ x }) => ({ y: x * 2 }),
  });
  return createHttpHandler(protocol, { serverId: "path-test", ...(prefix ? { prefix } : {}) });
}

const describeBody = (url: string) =>
  new Request(url, {
    method: "POST",
    headers: { "Content-Type": "application/vnd.apache.arrow.stream" },
    body: new Uint8Array(0),
  });

describe("duplicate slashes in the request path", () => {
  test("__describe__ resolves with a doubled leading slash", async () => {
    const handler = handlerFor();
    const single = await handler(describeBody("http://x/__describe__"));
    const doubled = await handler(describeBody("http://x//__describe__"));
    expect(single.status).toBe(200);
    expect(doubled.status).toBe(single.status);
  });

  test("the doubled path dispatches the same method, not '/method'", async () => {
    const handler = handlerFor();
    const resp = await handler(describeBody("http://x//__describe__"));
    // The old failure surfaced as a 404 whose body named the method with a
    // leading slash. Assert on that shape so a regression is unmistakable.
    const body = await resp.text().catch(() => "");
    expect(body).not.toContain("'/__describe__'");
  });

  test("health tolerates it too", async () => {
    const handler = handlerFor();
    for (const url of ["http://x/health", "http://x//health"]) {
      expect((await handler(new Request(url))).status).toBe(200);
    }
  });

  test("many slashes collapse, not just two", async () => {
    const handler = handlerFor();
    const resp = await handler(describeBody("http://x////__describe__"));
    expect(resp.status).toBe(200);
  });

  test("a prefixed worker collapses inside and around the prefix", async () => {
    const handler = handlerFor("/vgi");
    for (const url of ["http://x/vgi/__describe__", "http://x//vgi//__describe__"]) {
      expect((await handler(describeBody(url))).status).toBe(200);
    }
  });

  test("a path outside the prefix still 404s — normalization is not a bypass", async () => {
    const handler = handlerFor("/vgi");
    const resp = await handler(describeBody("http://x//other//__describe__"));
    expect(resp.status).toBe(404);
  });
});

describe("client base-URL normalization", () => {
  test("a trailing slash on the base URL does not produce a doubled path", async () => {
    const { httpConnect } = await import("../../src/client/connect.js");
    const handler = handlerFor();
    const seen: string[] = [];

    const server = Bun.serve({
      port: 0,
      fetch: (req) => {
        seen.push(new URL(req.url).pathname);
        return handler(req);
      },
    });

    try {
      // Deliberately trailing-slashed, as a pasted URL usually is.
      const client = httpConnect(`http://localhost:${server.port}/`);
      const result = await client.call("double", { x: 4 });
      expect(result?.y).toBe(8);
      client.close();
      // The server would have coped either way, so assert on the wire: the
      // client must not emit "//" at all.
      expect(seen.length).toBeGreaterThan(0);
      for (const p of seen) expect(p).not.toContain("//");
    } finally {
      server.stop(true);
    }
  });
});

describe("contributed routes see the normalized path", () => {
  test("ctx.url.pathname is collapsed, and the query string survives", async () => {
    const protocol = new Protocol("ExtraSvc");
    protocol.unary("noop", {
      params: { x: float },
      result: { y: float },
      handler: async ({ x }) => ({ y: x }),
    });

    const seen: string[] = [];
    const handler = createHttpHandler(protocol, {
      serverId: "extra-test",
      extraRoutes: (_req, ctx) => {
        seen.push(ctx.url.pathname);
        if (ctx.url.pathname === "/asset.js") {
          return new Response(`q=${ctx.url.searchParams.get("v") ?? ""}`, { status: 200 });
        }
        return null;
      },
    });

    // This is the shape that stayed broken after the RPC routes were fixed:
    // contributed routes match on ctx.url.pathname, not the handler's `path`.
    const resp = await handler(new Request("http://x//asset.js?v=7"));
    expect(resp.status).toBe(200);
    expect(await resp.text()).toBe("q=7");
    expect(seen).toContain("/asset.js");
    for (const p of seen) expect(p).not.toContain("//");
  });
});
