// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "bun:test";
import { RecordBatch, RecordBatchStreamWriter, recordBatchFromArrays } from "@query-farm/apache-arrow";
import { AuthContext } from "../src/auth.js";
import { REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY } from "../src/constants.js";
import { ARROW_CONTENT_TYPE } from "../src/http/common.js";
import { bearerAuthenticate, bearerAuthenticateStatic, chainAuthenticate } from "../src/http/bearer.js";
import { createHttpHandler } from "../src/http/handler.js";
import { Protocol } from "../src/protocol.js";
import { str, toSchema } from "../src/schema.js";

const ALICE = new AuthContext("bearer", true, "alice", {});
const BOB = new AuthContext("apikey", true, "bob", { role: "admin" });

function makeRequest(authorization?: string): Request {
  const headers: Record<string, string> = {};
  if (authorization) headers.Authorization = authorization;
  return new Request("http://localhost/vgi/test", { method: "POST", headers });
}

function makeArrowBody(methodName: string): Uint8Array {
  const schema = toSchema({ message: str });
  const batch = recordBatchFromArrays({ message: ["hello"] }, schema);
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

describe("bearerAuthenticate", () => {
  it("valid token returns AuthContext", async () => {
    const authFn = bearerAuthenticate({
      validate: (token) => {
        if (token === "good") return ALICE;
        throw new Error("bad");
      },
    });
    const auth = await authFn(makeRequest("Bearer good"));
    expect(auth.authenticated).toBe(true);
    expect(auth.principal).toBe("alice");
  });

  it("invalid token raises", async () => {
    const authFn = bearerAuthenticate({
      validate: () => {
        throw new Error("invalid token");
      },
    });
    await expect(authFn(makeRequest("Bearer bad-token"))).rejects.toThrow("invalid token");
  });

  it("missing header raises", async () => {
    const authFn = bearerAuthenticate({ validate: () => ALICE });
    await expect(authFn(makeRequest())).rejects.toThrow("Missing");
  });

  it("non-Bearer scheme raises", async () => {
    const authFn = bearerAuthenticate({ validate: () => ALICE });
    await expect(authFn(makeRequest("Basic dXNlcjpwYXNz"))).rejects.toThrow("Missing");
  });

  it("supports async validate", async () => {
    const authFn = bearerAuthenticate({
      validate: async (token) => {
        await Promise.resolve();
        if (token === "async-good") return BOB;
        throw new Error("bad");
      },
    });
    const auth = await authFn(makeRequest("Bearer async-good"));
    expect(auth.principal).toBe("bob");
  });
});

describe("bearerAuthenticateStatic", () => {
  it("known token returns mapped AuthContext (object)", async () => {
    const authFn = bearerAuthenticateStatic({
      tokens: { "token-alice": ALICE, "token-bob": BOB },
    });
    const auth = await authFn(makeRequest("Bearer token-alice"));
    expect(auth.principal).toBe("alice");
    expect(auth.domain).toBe("bearer");
  });

  it("known token returns mapped AuthContext (Map)", async () => {
    const authFn = bearerAuthenticateStatic({
      tokens: new Map([
        ["token-alice", ALICE],
        ["token-bob", BOB],
      ]),
    });
    const auth = await authFn(makeRequest("Bearer token-bob"));
    expect(auth.principal).toBe("bob");
  });

  it("unknown token raises", async () => {
    const authFn = bearerAuthenticateStatic({
      tokens: { "token-alice": ALICE },
    });
    await expect(authFn(makeRequest("Bearer unknown"))).rejects.toThrow("Unknown bearer token");
  });
});

describe("chainAuthenticate", () => {
  it("first succeeds — returned immediately", async () => {
    const first = bearerAuthenticateStatic({ tokens: { t1: ALICE } });
    const second = bearerAuthenticateStatic({ tokens: { t2: BOB } });
    const chain = chainAuthenticate(first, second);
    const auth = await chain(makeRequest("Bearer t1"));
    expect(auth.principal).toBe("alice");
  });

  it("fallback to second", async () => {
    const first = bearerAuthenticateStatic({ tokens: { t1: ALICE } });
    const second = bearerAuthenticateStatic({ tokens: { t2: BOB } });
    const chain = chainAuthenticate(first, second);
    const auth = await chain(makeRequest("Bearer t2"));
    expect(auth.principal).toBe("bob");
  });

  it("all fail raises", async () => {
    const first = bearerAuthenticateStatic({ tokens: { t1: ALICE } });
    const second = bearerAuthenticateStatic({ tokens: { t2: BOB } });
    const chain = chainAuthenticate(first, second);
    await expect(chain(makeRequest("Bearer unknown"))).rejects.toThrow("No authenticator accepted");
  });

  it("PermissionError propagates immediately", async () => {
    const forbidden = async (_req: Request) => {
      const err = new Error("access denied");
      err.name = "PermissionError";
      throw err;
    };
    const neverCalled = bearerAuthenticateStatic({ tokens: { t: ALICE } });
    const chain = chainAuthenticate(forbidden, neverCalled);
    await expect(chain(makeRequest("Bearer t"))).rejects.toThrow("access denied");
  });

  it("TypeError propagates immediately (not swallowed as credential error)", async () => {
    const broken = async () => {
      throw new TypeError("bug in authenticator");
    };
    const neverCalled = bearerAuthenticateStatic({ tokens: { t: ALICE } });
    const chain = chainAuthenticate(broken, neverCalled);
    await expect(chain(makeRequest("Bearer t"))).rejects.toThrow("bug in authenticator");
  });

  it("non-Error throw propagates immediately", async () => {
    const broken = async () => {
      throw "string error";
    };
    const neverCalled = bearerAuthenticateStatic({ tokens: { t: ALICE } });
    const chain = chainAuthenticate(broken, neverCalled);
    await expect(chain(makeRequest("Bearer t"))).rejects.toBe("string error");
  });

  it("empty chain raises at construction time", () => {
    expect(() => chainAuthenticate()).toThrow("at least one");
  });
});

describe("bearer + HTTP handler integration", () => {
  function makeProtocol(): Protocol {
    const p = new Protocol("test-service");
    p.unary("whoami", {
      params: { _dummy: str },
      result: { identity: str },
      handler: async (_params, ctx) => {
        ctx.auth.requireAuthenticated();
        return { identity: `${ctx.auth.domain}:${ctx.auth.principal}` };
      },
    });
    return p;
  }

  it("valid bearer token returns 200", async () => {
    const auth = bearerAuthenticateStatic({ tokens: { "secret-key": ALICE } });
    const handler = createHttpHandler(makeProtocol(), { authenticate: auth });
    const body = makeArrowBody("whoami");
    const resp = await handler(
      new Request("http://localhost/vgi/whoami", {
        method: "POST",
        headers: {
          "Content-Type": ARROW_CONTENT_TYPE,
          Authorization: "Bearer secret-key",
        },
        body,
      }),
    );
    expect(resp.status).toBe(200);
  });

  it("invalid bearer token returns 401", async () => {
    const auth = bearerAuthenticateStatic({ tokens: { "secret-key": ALICE } });
    const handler = createHttpHandler(makeProtocol(), { authenticate: auth });
    const body = makeArrowBody("whoami");
    const resp = await handler(
      new Request("http://localhost/vgi/whoami", {
        method: "POST",
        headers: {
          "Content-Type": ARROW_CONTENT_TYPE,
          Authorization: "Bearer wrong-key",
        },
        body,
      }),
    );
    expect(resp.status).toBe(401);
    const text = await resp.text();
    expect(text).toContain("Unknown bearer token");
  });

  it("missing authorization header returns 401", async () => {
    const auth = bearerAuthenticateStatic({ tokens: { "secret-key": ALICE } });
    const handler = createHttpHandler(makeProtocol(), { authenticate: auth });
    const body = makeArrowBody("whoami");
    const resp = await handler(
      new Request("http://localhost/vgi/whoami", {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body,
      }),
    );
    expect(resp.status).toBe(401);
  });

  it("chained auth: rejects invalid token with 401", async () => {
    const first = bearerAuthenticateStatic({ tokens: { t1: ALICE } });
    const second = bearerAuthenticateStatic({ tokens: { t2: BOB } });
    const auth = chainAuthenticate(first, second);
    const handler = createHttpHandler(makeProtocol(), { authenticate: auth });
    const body = makeArrowBody("whoami");
    const resp = await handler(
      new Request("http://localhost/vgi/whoami", {
        method: "POST",
        headers: {
          "Content-Type": ARROW_CONTENT_TYPE,
          Authorization: "Bearer totally-invalid",
        },
        body,
      }),
    );
    expect(resp.status).toBe(401);
  });
});
