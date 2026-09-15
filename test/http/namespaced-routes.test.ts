// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Namespaced HTTP routes: `{prefix}/{protocol}/{method}`.
 *
 * A server hosts several protocols and dispatch resolves the pair
 * `(protocol, method)`. On HTTP the protocol therefore rides twice — in the
 * request batch's `vgi_rpc.protocol` metadata and as a path segment.
 *
 * **The metadata is canonical.** It is the only carrier on the stdio, unix and
 * named-pipe transports, so it is what dispatch is defined in terms of. The
 * path segment is a required *faithful projection* of it, present so an edge
 * device — proxy, WAF, gateway — can act on the protocol without an Arrow
 * parser. Everything asserted below follows from that one relationship: the
 * segment must be there, it must agree, and it must not be decodable into
 * something other than what the edge read.
 */

import { beforeAll, describe, expect, test } from "bun:test";
import { deserializeBatch, field, list, utf8 } from "../../src/arrow/index.js";
import { AuthContext } from "../../src/auth.js";
import { buildRequestIpc } from "../../src/client/ipc.js";
import {
  CALL_STATE_KEY,
  ERROR_KIND_KEY,
  LOG_EXTRA_KEY,
  PROTOCOL_KEY,
  REQUEST_VERSION,
  REQUEST_VERSION_KEY,
  RPC_METHOD_KEY,
  STATE_KEY,
} from "../../src/constants.js";
import { ARROW_CONTENT_TYPE, reservedPath, rpcPath } from "../../src/http/common.js";
import { createHttpHandler } from "../../src/http/handler.js";
import { Protocol } from "../../src/protocol.js";
import { REFLECTION_PROTOCOL_NAME } from "../../src/reflection.js";
import { int, str, toSchema } from "../../src/schema.js";
import { VgiRpcServer } from "../../src/server.js";
import { IDENTITY_PROTOCOL_NAME, IdentityImpl, type IssuedGrant } from "../../src/token-identity.js";

const PREFIX = "/vgi";
const APP_PROTOCOL = "demo.App.v1";
const BASE = "http://worker.example";

/** `auth_time` only ever arrives on an OIDC/JWT credential — i.e. over HTTP.
 *  Peer identity on TCP/unix authenticates a principal but carries no session
 *  age, which is why `issue_grant` is reachable-but-refusing there and
 *  genuinely usable only here. */
function authenticate(request: Request): AuthContext {
  const header = (request.headers.get("Authorization") ?? "").replace(/^Bearer\s+/i, "");
  if (header === "fresh") {
    return new AuthContext("oidc", true, "alice", { auth_time: Math.floor(Date.now() / 1000) - 30 });
  }
  if (header === "peer") {
    // What a transport-peer credential looks like: a principal, and no claim
    // about when the human behind it last authenticated.
    return new AuthContext("peer", true, "alice", {});
  }
  return AuthContext.anonymous();
}

const minted: IssuedGrant = {
  token: "grant-opaque",
  expiresAt: Math.floor(Date.now() / 1000) + 600,
  grantId: "g-1",
};

function makeServer(): VgiRpcServer {
  const app = new Protocol(APP_PROTOCOL)
    .unary("echo", {
      params: { message: str },
      result: { message: str },
      handler: (params) => ({ message: String(params.message) }),
    })
    .producer<{ n: number }>("count", {
      params: { upto: int },
      outputSchema: { n: int },
      init: (params) => ({ n: Number(params.upto ?? 0) }),
      produce: (_state, out) => {
        out.emit({ n: [1] });
        return true;
      },
    });
  const server = new VgiRpcServer(app, { serverId: "namespaced-test" });
  server.registerReflection();
  server.registerIdentity(new IdentityImpl({ mintGrant: () => minted }));
  return server;
}

/** Build one request body. `protocol` defaults to the path's protocol because
 *  agreement is the normal case; the disagreement tests override it. */
function body(
  protocol: string | null,
  method: string,
  schema: ReturnType<typeof toSchema>,
  params: Record<string, unknown>,
): Uint8Array {
  return buildRequestIpc(schema, params, method, protocol === null ? {} : { protocol });
}

function post(
  handler: (r: Request) => Response | Promise<Response>,
  url: string,
  payload: Uint8Array,
  headers: Record<string, string> = {},
): Promise<Response> {
  return Promise.resolve(
    handler(
      new Request(url, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE, ...headers },
        body: payload as unknown as BodyInit,
      }),
    ),
  );
}

/** Decode a unary response's single data batch into a plain object. */
async function readResult(response: Response): Promise<Record<string, unknown>> {
  const { RecordBatchReader } = await import("@query-farm/apache-arrow");
  const reader = await RecordBatchReader.from(new Uint8Array(await response.arrayBuffer()));
  await reader.open();
  const data = reader.readAll().find((b) => b.numRows > 0);
  expect(data).toBeDefined();
  const out: Record<string, unknown> = {};
  data!.schema.fields.forEach((f, i) => {
    out[f.name] = data!.getChildAt(i)?.get(0);
  });
  return out;
}

/** The exception type and typed `error_kind` an EXCEPTION batch carries.
 *
 *  The kind is the part a client branches on -- it is stable across ports,
 *  where the class name is not. */
async function readError(response: Response): Promise<{ type: string; kind: string }> {
  const { RecordBatchReader } = await import("@query-farm/apache-arrow");
  const reader = await RecordBatchReader.from(new Uint8Array(await response.arrayBuffer()));
  await reader.open();
  for (const batch of reader.readAll()) {
    const extra = batch.metadata?.get(LOG_EXTRA_KEY);
    if (extra) {
      return {
        type: String(JSON.parse(extra).exception_type ?? ""),
        kind: batch.metadata?.get(ERROR_KIND_KEY) ?? "",
      };
    }
  }
  return { type: "", kind: "" };
}

/** The scopes column of `issue_grant`: `list<item?: utf8>`, item nullable --
 *  Arrow's own convention for a list child, and part of the protocol hash. */
const SCOPES = list(field("item", utf8(), true));

describe("namespaced HTTP routes", () => {
  let handler: (r: Request) => Response | Promise<Response>;

  beforeAll(() => {
    handler = createHttpHandler(makeServer(), { prefix: PREFIX, authenticate, compressionLevel: null });
  });

  test("the application protocol is reachable under its own name", async () => {
    const url = BASE + rpcPath(APP_PROTOCOL, "echo", { prefix: PREFIX });
    expect(url).toBe("http://worker.example/vgi/demo.App.v1/echo");
    const resp = await post(handler, url, body(APP_PROTOCOL, "echo", toSchema({ message: str }), { message: "hi" }));
    expect(resp.status).toBe(200);
    expect((await readResult(resp)).message).toBe("hi");
  });

  test("reflection is reachable over HTTP, not just the raw transports", async () => {
    // Before namespacing, a co-hosted protocol had no path to be reached on:
    // the route carried a method and nothing else, so only the primary was
    // addressable over HTTP.
    const resp = await post(
      handler,
      BASE + rpcPath(REFLECTION_PROTOCOL_NAME, "list_protocols", { prefix: PREFIX }),
      body(REFLECTION_PROTOCOL_NAME, "list_protocols", toSchema({}), {}),
    );
    expect(resp.status).toBe(200);
    const listing = deserializeBatch((await readResult(resp)).result as Uint8Array);
    const protocols = listing.getChildAt(listing.schema.fields.findIndex((f) => f.name === "protocols"))?.get(0);
    const names = [...(protocols as Iterable<{ protocol: string }>)].map((p) => String(p.protocol)).sort();
    expect(names).toEqual([APP_PROTOCOL, IDENTITY_PROTOCOL_NAME, REFLECTION_PROTOCOL_NAME].sort());
  });

  test("issue_grant succeeds over HTTP, where a credential can carry auth_time", async () => {
    // The whole point of routing identity over HTTP. `checkFreshness` requires
    // an `auth_time` claim, which arrives only on an OIDC/JWT credential — so
    // on TCP/unix `issue_grant` is reachable only where it must refuse.
    const resp = await post(
      handler,
      BASE + rpcPath(IDENTITY_PROTOCOL_NAME, "issue_grant", { prefix: PREFIX }),
      body(IDENTITY_PROTOCOL_NAME, "issue_grant", toSchema({ purpose: str, scopes: SCOPES, ttl_seconds: int }), {
        purpose: "ci",
        scopes: ["read"],
        ttl_seconds: 600,
      }),
      { Authorization: "Bearer fresh" },
    );
    expect(resp.status).toBe(200);
    const grant = deserializeBatch((await readResult(resp)).result as Uint8Array);
    expect(grant.getChildAt(grant.schema.fields.findIndex((f) => f.name === "token"))?.get(0)).toBe("grant-opaque");
  });

  test("the same call refuses a credential with no auth_time", async () => {
    // The transport-peer shape: authenticated, but silent about session age.
    const resp = await post(
      handler,
      BASE + rpcPath(IDENTITY_PROTOCOL_NAME, "issue_grant", { prefix: PREFIX }),
      body(IDENTITY_PROTOCOL_NAME, "issue_grant", toSchema({ purpose: str, scopes: SCOPES, ttl_seconds: int }), {
        purpose: "ci",
        scopes: ["read"],
        ttl_seconds: 600,
      }),
      { Authorization: "Bearer peer" },
    );
    expect(await readError(resp)).toMatchObject({ type: "StaleAuthError" });
  });

  test("method names may collide across protocols and still resolve", async () => {
    // Independent authorship is the reason protocols exist; a port that merged
    // them into one namespace would answer this from the wrong one.
    const collide = new Protocol("other.App.v1").unary("echo", {
      params: { message: str },
      result: { message: str },
      handler: () => ({ message: "from-other" }),
    });
    const server = makeServer();
    server.addProtocol({ name: "other.App.v1", protocol: collide, protocolHash: "", versionExempt: false });
    const h = createHttpHandler(server, { prefix: PREFIX, compressionLevel: null });
    const schema = toSchema({ message: str });
    const first = await post(
      h,
      BASE + rpcPath(APP_PROTOCOL, "echo", { prefix: PREFIX }),
      body(APP_PROTOCOL, "echo", schema, { message: "hi" }),
    );
    const second = await post(
      h,
      BASE + rpcPath("other.App.v1", "echo", { prefix: PREFIX }),
      body("other.App.v1", "echo", schema, { message: "hi" }),
    );
    expect((await readResult(first)).message).toBe("hi");
    expect((await readResult(second)).message).toBe("from-other");
  });

  test("streams init and exchange under the protocol segment", async () => {
    const initUrl = BASE + rpcPath(APP_PROTOCOL, "count", { prefix: PREFIX, suffix: "/init" });
    expect(initUrl).toBe("http://worker.example/vgi/demo.App.v1/count/init");
    const init = await post(handler, initUrl, body(APP_PROTOCOL, "count", toSchema({ upto: int }), { upto: 3 }));
    expect(init.status).toBe(200);

    const { RecordBatchReader } = await import("@query-farm/apache-arrow");
    const reader = await RecordBatchReader.from(new Uint8Array(await init.arrayBuffer()));
    await reader.open();
    const batches = reader.readAll();
    const token = batches.map((b) => b.metadata?.get(STATE_KEY)).find((t) => t);
    expect(token).toBeDefined();

    const meta = new Map<string, string>([
      [RPC_METHOD_KEY, "count"],
      [PROTOCOL_KEY, APP_PROTOCOL],
      [REQUEST_VERSION_KEY, REQUEST_VERSION],
      [STATE_KEY, token!],
    ]);
    const {
      batchFromColumns,
      schema: makeSchema,
      serializeBatches,
      withBatchMetadata,
    } = await import("../../src/arrow/index.js");
    const empty = makeSchema([]);
    const cursorBody = serializeBatches(empty, [withBatchMetadata(batchFromColumns(empty, {}), meta)]);
    const exchange = await post(
      handler,
      BASE + rpcPath(APP_PROTOCOL, "count", { prefix: PREFIX, suffix: "/exchange" }),
      cursorBody,
    );
    expect(exchange.status).toBe(200);
  });
});

describe("the path is a projection of the metadata, and is checked against it", () => {
  let handler: (r: Request) => Response | Promise<Response>;

  beforeAll(() => {
    handler = createHttpHandler(makeServer(), { prefix: PREFIX, authenticate, compressionLevel: null });
  });

  test("a path that disagrees with vgi_rpc.protocol is rejected", async () => {
    // Left unchecked, edge policy is applied to one protocol while the worker
    // dispatches another: the Content-Length/Transfer-Encoding shape.
    const resp = await post(
      handler,
      BASE + rpcPath(APP_PROTOCOL, "echo", { prefix: PREFIX }),
      body(REFLECTION_PROTOCOL_NAME, "echo", toSchema({ message: str }), { message: "hi" }),
    );
    expect(resp.status).toBe(400);
    expect(await readError(resp)).toEqual({ type: "ProtocolNotSupportedError", kind: "protocol_not_supported" });
  });

  test("an absent vgi_rpc.protocol is rejected, with no single-protocol exemption", async () => {
    // An intermediary that rebuilds a request and drops the field must be told,
    // not landed silently on whichever protocol happened to be first.
    const resp = await post(
      handler,
      BASE + rpcPath(APP_PROTOCOL, "echo", { prefix: PREFIX }),
      body(null, "echo", toSchema({ message: str }), { message: "hi" }),
    );
    expect(resp.status).toBe(400);
    expect(await readError(resp)).toEqual({ type: "ProtocolNotSpecifiedError", kind: "protocol_not_specified" });
  });

  test("a percent sign in the protocol segment is rejected without decoding", async () => {
    // `demo%2EApp%2Ev1` percent-decodes to the hosted `demo.App.v1`. Decoding
    // it would route the request to a protocol the edge never saw that name
    // for. The charset never requires encoding, so a `%` is a bug or an attack.
    const resp = await post(
      handler,
      `${BASE}${PREFIX}/demo%2EApp%2Ev1/echo`,
      body(APP_PROTOCOL, "echo", toSchema({ message: str }), { message: "hi" }),
    );
    expect(resp.status).toBe(404);
    expect(await readError(resp)).toEqual({ type: "ProtocolNotSpecifiedError", kind: "protocol_not_specified" });
  });

  test("the raw segment is what is inspected, not a decoded copy", async () => {
    // Guards the JS-specific trap: a framework that hands back an already
    // decoded path would see `demo.App.v1` here and route it. `URL.pathname`
    // does not decode, and nothing downstream may either.
    const seen: string[] = [];
    const wrapped = (r: Request) => {
      seen.push(new URL(r.url).pathname);
      return handler(r);
    };
    await post(
      wrapped,
      `${BASE}${PREFIX}/demo%2EApp%2Ev1/echo`,
      body(APP_PROTOCOL, "echo", toSchema({ message: str }), { message: "hi" }),
    );
    expect(seen[0]).toBe("/vgi/demo%2EApp%2Ev1/echo");
  });

  test("an encoded slash cannot smuggle a second path segment", async () => {
    const resp = await post(
      handler,
      `${BASE}${PREFIX}/demo.App.v1%2Fecho/echo`,
      body(APP_PROTOCOL, "echo", toSchema({ message: str }), { message: "hi" }),
    );
    expect(resp.status).toBe(404);
  });

  test("a protocol this server does not host is 404", async () => {
    const resp = await post(
      handler,
      BASE + rpcPath("nobody.Else.v1", "echo", { prefix: PREFIX }),
      body("nobody.Else.v1", "echo", toSchema({ message: str }), { message: "hi" }),
    );
    expect(resp.status).toBe(404);
    expect(await readError(resp)).toEqual({ type: "ProtocolNotSupportedError", kind: "protocol_not_supported" });
  });

  test("a hosted protocol without the named method is a different answer", async () => {
    // The documented capability-probe signal: a client testing for an optional
    // method must be able to tell "you do not speak this protocol" from "you
    // speak it but lack this method".
    const resp = await post(
      handler,
      BASE + rpcPath(APP_PROTOCOL, "nope", { prefix: PREFIX }),
      body(APP_PROTOCOL, "nope", toSchema({}), {}),
    );
    expect(resp.status).toBe(404);
    expect(await readError(resp)).toEqual({ type: "MethodNotImplementedError", kind: "method_not_implemented" });
  });

  test("a segment that cannot be a protocol name is 404, not a lookup", async () => {
    // Rejected before the lookup, so a request-supplied string never reaches an
    // error message, a log field or a metric label.
    const resp = await post(
      handler,
      `${BASE}${PREFIX}/9not-a-name/echo`,
      body(APP_PROTOCOL, "echo", toSchema({ message: str }), { message: "hi" }),
    );
    expect(resp.status).toBe(404);
    expect(await readError(resp)).toEqual({ type: "ProtocolNotSupportedError", kind: "protocol_not_supported" });
  });

  test("a flat one-segment RPC path no longer routes", async () => {
    const resp = await post(
      handler,
      `${BASE}${PREFIX}/echo`,
      body(APP_PROTOCOL, "echo", toSchema({ message: str }), { message: "hi" }),
    );
    expect(resp.status).toBe(404);
  });
});

describe("a continuation stays on the protocol its stream started on", () => {
  /** Two protocols that both host `count`, so only the token binding can tell
   *  a legitimate continuation from a cross-protocol one. */
  function twoProtocolServer(): VgiRpcServer {
    const server = makeServer();
    const other = new Protocol("other.App.v1").producer<{ n: number }>("count", {
      params: { upto: int },
      outputSchema: { n: int },
      init: (params) => ({ n: Number(params.upto ?? 0) }),
      produce: (_state, out) => {
        out.emit({ n: [99] });
        return true;
      },
    });
    server.addProtocol({ name: "other.App.v1", protocol: other, protocolHash: "", versionExempt: false });
    return server;
  }

  test("a cursor minted under one protocol is refused under another", async () => {
    // The protocol is bound into the AEAD associated data of the cursor rather
    // than compared in application code: there is no comparison to forget, and
    // it covers the call-state cache-hit path where the call token is never
    // opened at all.
    const handler = createHttpHandler(twoProtocolServer(), { prefix: PREFIX, compressionLevel: null });
    const init = await post(
      handler,
      BASE + rpcPath(APP_PROTOCOL, "count", { prefix: PREFIX, suffix: "/init" }),
      body(APP_PROTOCOL, "count", toSchema({ upto: int }), { upto: 3 }),
    );
    expect(init.status).toBe(200);

    const { RecordBatchReader } = await import("@query-farm/apache-arrow");
    const reader = await RecordBatchReader.from(new Uint8Array(await init.arrayBuffer()));
    await reader.open();
    const batches = reader.readAll();
    const cursor = batches.map((b) => b.metadata?.get(STATE_KEY)).find((t) => t);
    const callToken = batches.map((b) => b.metadata?.get(CALL_STATE_KEY)).find((t) => t);
    expect(cursor).toBeDefined();

    const {
      batchFromColumns,
      schema: makeSchema,
      serializeBatches,
      withBatchMetadata,
    } = await import("../../src/arrow/index.js");
    const empty = makeSchema([]);
    const meta = new Map<string, string>([
      [RPC_METHOD_KEY, "count"],
      [REQUEST_VERSION_KEY, REQUEST_VERSION],
      [STATE_KEY, cursor!],
    ]);
    if (callToken) meta.set(CALL_STATE_KEY, callToken);
    const cursorBody = serializeBatches(empty, [withBatchMetadata(batchFromColumns(empty, {}), meta)]);

    // Same cursor, same method name, different protocol segment.
    const crossed = await post(
      handler,
      BASE + rpcPath("other.App.v1", "count", { prefix: PREFIX, suffix: "/exchange" }),
      cursorBody,
    );
    expect(crossed.status).toBe(400);
    expect((await readError(crossed)).type).toBe("HttpRpcError");

    // The same bytes on the protocol the stream started on still work, so the
    // rejection above is the binding and not a broken cursor.
    const honest = await post(
      handler,
      BASE + rpcPath(APP_PROTOCOL, "count", { prefix: PREFIX, suffix: "/exchange" }),
      cursorBody,
    );
    expect(honest.status).toBe(200);
  });
});

describe("a protocol that cannot be a path segment cannot be hosted", () => {
  test("createHttpHandler refuses a name the route shape cannot carry", () => {
    // Caught at construction rather than as a 404 on every call, which is what
    // an unroutable name would otherwise look like from the outside.
    const bad = new Protocol("not-a-name");
    expect(() => createHttpHandler(bad, { prefix: PREFIX })).toThrow(/Cannot serve protocol 'not-a-name' over HTTP/);
  });
});

describe("reserved endpoints belong to the server, not to a protocol", () => {
  let handler: (r: Request) => Response | Promise<Response>;

  beforeAll(() => {
    handler = createHttpHandler(makeServer(), {
      prefix: PREFIX,
      authenticate,
      compressionLevel: null,
      introspectResolver: () => ({ principal: "bob" }),
      introspectPrincipals: ["alice"],
    });
  });

  test("__describe__ stays flat", async () => {
    const url = BASE + reservedPath("__describe__", { prefix: PREFIX });
    expect(url).toBe("http://worker.example/vgi/__describe__");
    const resp = await post(handler, url, body(APP_PROTOCOL, "__describe__", toSchema({}), {}));
    expect(resp.status).toBe(200);
  });

  test("health stays flat", async () => {
    const resp = await handler(new Request(`${BASE}${PREFIX}/health`, { method: "GET" }));
    expect(resp.status).toBe(200);
    expect((await resp.json()).protocol).toBe(APP_PROTOCOL);
  });

  test("the legacy __introspect_token__ JSON route is still served", async () => {
    // Identity now also rides `vgi_rpc.Identity.v1`, but the prefix-level JSON
    // route a proxy was built against does not move under a protocol segment.
    const resp = await handler(
      new Request(`${BASE}${PREFIX}/__introspect_token__`, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: "Bearer fresh" },
        body: JSON.stringify({ token: "opaque-credential" }),
      }),
    );
    expect(resp.status).toBe(200);
    expect((await resp.json()).principal).toBe("bob");
  });
});
