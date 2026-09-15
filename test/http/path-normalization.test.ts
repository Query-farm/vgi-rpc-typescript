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
 *   Unknown method: '/aggregate_bind'. Available methods: [aggregate_bind, ...]
 *
 * where every name in that list is unprefixed, so the leading slash was the
 * whole story. Health, the landing surface and the client bundle 404'd the
 * same way. Both halves are covered here: the server tolerates the doubled
 * path, and the client no longer produces one.
 */

import { describe, expect, test } from "bun:test";
import { buildRequestIpc } from "../../src/client/ipc.js";
import { rpcPath } from "../../src/http/common.js";
import { createHttpHandler, float, Protocol, toSchema } from "../../src/index.js";

function handlerFor(prefix?: string) {
  const protocol = new Protocol("PathSvc");
  protocol.unary("double", {
    params: { x: float },
    result: { y: float },
    handler: async ({ x }) => ({ y: x * 2 }),
  });
  return createHttpHandler(protocol, { serverId: "path-test", ...(prefix ? { prefix } : {}) });
}

/** A well-formed `PathSvc.double` call, addressed at whatever URL is given.
 *
 *  A real dispatchable route rather than a reserved one: `__describe__` used
 *  to serve as the probe here, and its retirement would otherwise have taken
 *  the normalization coverage with it. */
const callBody = (url: string) =>
  new Request(url, {
    method: "POST",
    headers: { "Content-Type": "application/vnd.apache.arrow.stream" },
    body: buildRequestIpc(toSchema({ x: float }) as never, { x: 4 }, "double", {
      protocol: "PathSvc",
    }) as unknown as BodyInit,
  });

const path = (prefix?: string) => rpcPath("PathSvc", "double", prefix ? { prefix } : undefined);

describe("duplicate slashes in the request path", () => {
  test("a method resolves with a doubled leading slash", async () => {
    const handler = handlerFor();
    const single = await handler(callBody(`http://x${path()}`));
    const doubled = await handler(callBody(`http://x/${path()}`));
    expect(single.status).toBe(200);
    expect(doubled.status).toBe(single.status);
  });

  test("the doubled path dispatches the same method, not '/method'", async () => {
    const handler = handlerFor();
    const resp = await handler(callBody(`http://x/${path()}`));
    // The old failure surfaced as a 404 whose body named the method with a
    // leading slash. Assert on that shape so a regression is unmistakable.
    const body = await resp.text().catch(() => "");
    expect(body).not.toContain("'/double'");
  });

  test("the retired flat path is collapsed too, so the refusal is reachable", async () => {
    // The exact shape from the field report, and the reason it still matters:
    // an *un*-collapsed `//__describe__` matches no route and 404s as bare
    // text, which reads identically to "this server never had introspection".
    // Collapsed, it reaches the refusal that names where introspection went.
    const handler = handlerFor();
    const resp = await handler(callBody("http://x//__describe__"));
    expect(resp.status).toBe(404);
    expect(await resp.text()).toContain("vgi_rpc.Reflection.v1");
  });

  test("health tolerates it too", async () => {
    const handler = handlerFor();
    for (const url of ["http://x/health", "http://x//health"]) {
      expect((await handler(new Request(url))).status).toBe(200);
    }
  });

  test("many slashes collapse, not just two", async () => {
    const handler = handlerFor();
    const resp = await handler(callBody(`http://x///${path()}`));
    expect(resp.status).toBe(200);
  });

  test("a prefixed worker collapses inside and around the prefix", async () => {
    const handler = handlerFor("/vgi");
    for (const url of [`http://x${path("/vgi")}`, `http://x//vgi//PathSvc//double`]) {
      expect((await handler(callBody(url))).status).toBe(200);
    }
  });

  test("a path outside the prefix still 404s — normalization is not a bypass", async () => {
    const handler = handlerFor("/vgi");
    const resp = await handler(callBody("http://x//other//PathSvc//double"));
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
