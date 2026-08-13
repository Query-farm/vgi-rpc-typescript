// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// `extraRoutes` is how a layer above this one contributes its own pages. It
// replaces the `landingInfo` option, which built the VGI landing surface in
// here — a page describing catalogs, plus a compiled bundle of the client
// library that depends on this package. Those now live in `@query-farm/vgi`
// and arrive through this hook.
//
// What matters here is not that a route can be added, but *where in the
// pipeline* it runs. The whole reason this is a hook rather than a wrapper
// around the returned handler is ordering: a contributed page must sit behind
// authentication, and must be able to pre-empt the generic landing page and
// the 404. Those three are what these tests pin.

import { describe, expect, test } from "bun:test";
import { AuthFailure, AuthReason, createHttpHandler, Protocol } from "../../src/index.js";
import type { ExtraRouteContext } from "../../src/index.js";

function makeProtocol() {
  const protocol = new Protocol("ExtraRouteTest");
  protocol.unary("noop", { params: {}, result: {}, handler: async () => ({}) });
  return protocol;
}

const hello = (_req: Request, ctx: ExtraRouteContext): Response | null => {
  if (ctx.url.pathname === `${ctx.prefix}/hello`) {
    const headers = new Headers({ "Content-Type": "text/plain" });
    ctx.addCorsHeaders(headers);
    return new Response("hello", { status: 200, headers });
  }
  return null;
};

const get = (h: (r: Request) => Promise<Response>, path: string, init?: RequestInit) =>
  h(new Request(`http://localhost${path}`, init));

describe("extraRoutes", () => {
  test("serves a contributed route", async () => {
    const handler = createHttpHandler(makeProtocol(), { serverId: "s", extraRoutes: hello });
    const resp = await get(handler, "/hello");
    expect(resp.status).toBe(200);
    expect(await resp.text()).toBe("hello");
  });

  test("declining with null falls through to normal routing", async () => {
    const handler = createHttpHandler(makeProtocol(), { serverId: "s", extraRoutes: hello });
    const resp = await get(handler, "/health");
    expect(resp.status).toBe(200);
    expect(resp.headers.get("Content-Type")).toContain("application/json");
  });

  test("a contributed route pre-empts the generic landing page", async () => {
    const root = (_r: Request, ctx: ExtraRouteContext) =>
      ctx.url.pathname === `${ctx.prefix}/` ? new Response("mine", { status: 200 }) : null;
    const handler = createHttpHandler(makeProtocol(), { serverId: "s", extraRoutes: root });
    const resp = await get(handler, "/", { headers: { Accept: "text/html" } });
    expect(await resp.text()).toBe("mine");
  });

  test("and pre-empts the 404 page", async () => {
    const handler = createHttpHandler(makeProtocol(), { serverId: "s", extraRoutes: hello });
    expect((await get(handler, "/hello")).status).toBe(200);
    // Same handler, unclaimed path: still the 404 it would otherwise be.
    expect((await get(handler, "/nope")).status).toBe(404);
  });

  test("receives the server identity it cannot otherwise see", async () => {
    let seen: ExtraRouteContext | null = null;
    const capture = (_r: Request, ctx: ExtraRouteContext) => {
      seen = ctx;
      return new Response("ok");
    };
    const handler = createHttpHandler(makeProtocol(), {
      serverId: "srv-42",
      prefix: "/vgi",
      extraRoutes: capture,
    });
    await get(handler, "/vgi/anything");
    expect(seen!.serverId).toBe("srv-42");
    expect(seen!.prefix).toBe("/vgi");
    // No PKCE configured here, so the flow is off — the value a status document
    // would report.
    expect(seen!.oauthActive).toBe(false);
  });

  test("applies the handler's CORS policy through addCorsHeaders", async () => {
    const handler = createHttpHandler(makeProtocol(), {
      serverId: "s",
      corsOrigins: "https://example.test",
      extraRoutes: hello,
    });
    const resp = await get(handler, "/hello", { headers: { Origin: "https://example.test" } });
    expect(resp.headers.get("access-control-allow-origin")).toBe("https://example.test");
  });

  test("an async route is awaited", async () => {
    const handler = createHttpHandler(makeProtocol(), {
      serverId: "s",
      extraRoutes: async () => new Response("async", { status: 200 }),
    });
    expect(await (await get(handler, "/whatever")).text()).toBe("async");
  });

  // A contributed route sits at exactly the pipeline position the built-in
  // pages occupy, so it carries their auth exposure — no more, no less. That
  // equivalence is the invariant worth pinning: it is what makes moving the VGI
  // landing surface out of this package a pure relocation.
  //
  // Note what that exposure actually is, because it is easy to assume
  // otherwise: GET pages are *not* gated by `authenticate` on its own. The GET
  // auth branch is conditioned on `authenticate && pkceConfig`, and exists to
  // redirect unauthenticated *browsers* to the identity provider; a non-browser
  // caller falls through to normal page serving. So with `authenticate` alone,
  // pages are public — the generic landing page always was, and a contributed
  // one is too.
  test("a contributed route has the same auth exposure as the built-in pages", async () => {
    const reject = () => {
      throw new AuthFailure(AuthReason.MissingCredential, "no credential");
    };
    const contributed = createHttpHandler(makeProtocol(), {
      serverId: "s",
      authenticate: reject,
      extraRoutes: hello,
    });
    const builtin = createHttpHandler(makeProtocol(), { serverId: "s", authenticate: reject });

    const page = await get(contributed, "/hello");
    const generic = await get(builtin, "/", { headers: { Accept: "text/html" } });
    expect(page.status).toBe(generic.status);
    expect(page.status).toBe(200);

    // The RPC surface is a different matter, and is gated.
    const rpc = await contributed(
      new Request("http://localhost/noop", { method: "POST", body: new Uint8Array() }),
    );
    expect(rpc.status).toBe(401);
  });

  test("honours a non-empty prefix", async () => {
    const handler = createHttpHandler(makeProtocol(), {
      serverId: "s",
      prefix: "/vgi",
      extraRoutes: hello,
    });
    expect((await get(handler, "/vgi/hello")).status).toBe(200);
  });
});
