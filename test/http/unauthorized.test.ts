// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * The standardized 401 of `docs/unauthorized-spec.md`, asserted at both ends:
 * the pure renderers here, and the response the handler actually emits.
 */

import { describe, expect, test } from "bun:test";
import { RecordBatch, RecordBatchStreamWriter, recordBatchFromArrays } from "@query-farm/apache-arrow";
import { AuthContext } from "../../src/auth.js";
import { REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY } from "../../src/constants.js";
import { chainAuthenticate } from "../../src/http/bearer.js";
import { ARROW_CONTENT_TYPE } from "../../src/http/common.js";
import { createHttpHandler } from "../../src/http/handler.js";
import {
  AUTH_PROXY_REQUIRED_HEADER,
  AUTH_REASON_HEADER,
  AuthFailure,
  AuthReason,
  buildProxyHint,
  classifyAuthFailure,
  unauthorizedEnvelope,
} from "../../src/http/unauthorized.js";
import { Protocol } from "../../src/protocol.js";
import { str, toSchema } from "../../src/schema.js";

const PROXY_HEADER = "X-Forwarded-Client-Cert";

function makeProtocol(): Protocol {
  const p = new Protocol("unauthorized-test");
  p.unary("echo", {
    params: { message: str },
    result: { message: str },
    handler: async (params: any) => ({ message: params.message }),
  });
  return p;
}

function makeBody(): Uint8Array {
  const schema = toSchema({ message: str });
  const batch = recordBatchFromArrays({ message: ["hi"] }, schema);
  const meta = new Map<string, string>([
    [RPC_METHOD_KEY, "echo"],
    [REQUEST_VERSION_KEY, REQUEST_VERSION],
  ]);
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, schema);
  writer.write(new RecordBatch(schema, batch.data, meta));
  writer.close();
  return writer.toUint8Array(true);
}

/** POST a well-formed unary body at a handler, optionally naming an Accept. */
function post(handler: (r: Request) => Promise<Response>, accept?: string): Promise<Response> {
  const headers: Record<string, string> = { "Content-Type": ARROW_CONTENT_TYPE };
  if (accept) headers.Accept = accept;
  return handler(new Request("http://localhost/echo", { method: "POST", headers, body: makeBody() }));
}

describe("envelope", () => {
  test("omits the hint when it does not apply", () => {
    // Absent, not empty — presence alone has to be a usable signal.
    const body = JSON.parse(unauthorizedEnvelope(AuthReason.InvalidCredential, "nope"));
    expect(body).toEqual({ error: "unauthorized", reason: "invalid_credential", detail: "nope" });
    expect("proxy_hint" in body).toBe(false);
  });

  test("carries the hint when it applies", () => {
    const body = JSON.parse(unauthorizedEnvelope(AuthReason.ProxyRequired, "", buildProxyHint([PROXY_HEADER])));
    expect(body.proxy_hint).toContain(PROXY_HEADER);
  });

  test("the hint names every header the proxy must set, once", () => {
    const hint = buildProxyHint([PROXY_HEADER, "VGI-Proxy-Proof", PROXY_HEADER]);
    expect(hint).toContain("VGI-Proxy-Proof");
    expect(hint.split(PROXY_HEADER).length - 1).toBe(1);
    expect(buildProxyHint([])).toBe("");
  });
});

describe("classification", () => {
  test("an AuthFailure names its own code", () => {
    expect(classifyAuthFailure(new AuthFailure(AuthReason.ExpiredCredential, "token aged out"))).toEqual({
      reason: AuthReason.ExpiredCredential,
      detail: "token aged out",
    });
  });

  test("an unclassified error lands on the fallback rather than a guess", () => {
    // Deriving "expired" from the word "expired" would misclassify the moment
    // someone rewords the string, so it must not be attempted.
    expect(classifyAuthFailure(new Error("credential expired")).reason).toBe(AuthReason.Unauthorized);
  });

  test("a PermissionError is insufficient scope", () => {
    // A guess from the error's type, not its wording: the name says the
    // caller got as far as being identified.
    const err = new Error("nope");
    err.name = "PermissionError";
    expect(classifyAuthFailure(err).reason).toBe(AuthReason.InsufficientScope);
  });

  test("a code outside the closed set is ignored", () => {
    const err = Object.assign(new Error("x"), { vgiAuthReason: "teapot" });
    expect(classifyAuthFailure(err).reason).toBe(AuthReason.Unauthorized);
  });
});

describe("chained credentials", () => {
  const missing = () => {
    throw new AuthFailure(AuthReason.MissingCredential, "nothing presented");
  };
  const invalid = () => {
    throw new AuthFailure(AuthReason.InvalidCredential, "wrong token");
  };

  test('"send a credential" survives only when every alternative agreed', async () => {
    const chain = chainAuthenticate(missing, missing);
    await expect(chain(new Request("http://localhost/"))).rejects.toMatchObject({
      reason: AuthReason.MissingCredential,
    });
  });

  test("one alternative that saw a credential wins", async () => {
    // Telling a caller who did present something to "send a credential" is
    // actively misleading, so the substantive code takes precedence.
    const chain = chainAuthenticate(missing, invalid);
    await expect(chain(new Request("http://localhost/"))).rejects.toMatchObject({
      reason: AuthReason.InvalidCredential,
    });
  });

  test("a classified failure still composes", async () => {
    // An AuthFailure is a rejection that merely names its reason; if the chain
    // rethrew it, classifying a refusal would silently disable composition.
    const chain = chainAuthenticate(invalid, () => new AuthContext("test", true, "alice"));
    const ctx = await chain(new Request("http://localhost/"));
    expect(ctx.principal).toBe("alice");
  });
});

describe("handler response", () => {
  test("a machine client gets the JSON envelope and the reason header", async () => {
    const handler = createHttpHandler(makeProtocol(), {
      authenticate: () => {
        throw new AuthFailure(AuthReason.InsufficientScope, "read-only key");
      },
    });
    const resp = await post(handler, "*/*");
    expect(resp.status).toBe(401);
    expect(resp.headers.get("Content-Type")).toBe("application/json");
    expect(resp.headers.get(AUTH_REASON_HEADER)).toBe("insufficient_scope");
    // A 401 flips to 200 on the next attempt with a credential, so a shared
    // cache must never hold it.
    expect(resp.headers.get("Cache-Control")).toContain("no-store");
    const body = await resp.json();
    expect(body.reason).toBe(resp.headers.get(AUTH_REASON_HEADER));
    expect(body.detail).toBe("read-only key");
  });

  test("a browser is never answered with HTML this port does not render", async () => {
    const handler = createHttpHandler(makeProtocol(), {
      authenticate: () => {
        throw new Error("no");
      },
    });
    const resp = await post(handler, "text/html,application/xhtml+xml");
    // §4.2 allows answering an HTML request with JSON; the reason header — the
    // part clients parse — is there either way.
    expect(resp.headers.get("Content-Type")).toBe("application/json");
    expect(resp.headers.get(AUTH_REASON_HEADER)).toBe("unauthorized");
  });

  test("a service with no proxy dependency stays quiet about proxies", async () => {
    const handler = createHttpHandler(makeProtocol(), {
      authenticate: () => {
        throw new Error("no");
      },
    });
    const resp = await post(handler);
    expect(resp.headers.get(AUTH_PROXY_REQUIRED_HEADER)).toBeNull();
    expect("proxy_hint" in (await resp.json())).toBe(false);
  });

  test("declared proxy headers add the note to every 401", async () => {
    // Derived from configuration, not from what failed: the note is identical
    // on a rejection that has nothing to do with the proxy, which is what
    // keeps it from being an oracle.
    const handler = createHttpHandler(makeProtocol(), {
      proxyAuthHeaders: [PROXY_HEADER],
      authenticate: () => {
        throw new AuthFailure(AuthReason.InvalidCredential, "bad token");
      },
    });
    const resp = await post(handler);
    expect(resp.headers.get(AUTH_PROXY_REQUIRED_HEADER)).toBe("true");
    expect(resp.headers.get(AUTH_REASON_HEADER)).toBe("invalid_credential");
    expect((await resp.json()).proxy_hint).toContain(PROXY_HEADER);
  });

  test("the rejection headers are CORS-exposed, and only when they exist", async () => {
    // A browser that cannot read them is back to guessing from the body.
    const plain = createHttpHandler(makeProtocol(), { corsOrigins: "*" });
    const proxied = createHttpHandler(makeProtocol(), { corsOrigins: "*", proxyAuthHeaders: [PROXY_HEADER] });
    const expose = async (h: (r: Request) => Promise<Response>) =>
      (await h(new Request("http://localhost/echo", { method: "OPTIONS" }))).headers.get(
        "Access-Control-Expose-Headers",
      );
    expect(await expose(plain)).toContain(AUTH_REASON_HEADER);
    expect(await expose(plain)).not.toContain(AUTH_PROXY_REQUIRED_HEADER);
    expect(await expose(proxied)).toContain(AUTH_PROXY_REQUIRED_HEADER);
  });

  test("neither header rides a successful response", async () => {
    // They describe a rejection, not a capability.
    const handler = createHttpHandler(makeProtocol(), { proxyAuthHeaders: [PROXY_HEADER] });
    const resp = await post(handler);
    expect(resp.status).toBe(200);
    expect(resp.headers.get(AUTH_REASON_HEADER)).toBeNull();
    expect(resp.headers.get(AUTH_PROXY_REQUIRED_HEADER)).toBeNull();
  });
});
