// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Cross-runtime smoke test for @query-farm/vgi-rpc.
 *
 * Verifies that all major public exports are importable and functional,
 * then starts an HTTP server, calls methods via httpConnect, and checks
 * the results.  Exits 0 with "PASS" on success.
 *
 * Usage:
 *   bun run test/smoke-import.ts
 *   bun build test/smoke-import.ts --outfile .smoke-bundle/smoke.js --target node --format esm
 *   node .smoke-bundle/smoke.js
 *   deno run --allow-all .smoke-bundle/smoke.js
 */

import {
  ARROW_CONTENT_TYPE,
  bool,
  bytes,
  // HTTP handler
  createHttpHandler,
  float,
  float32,
  // Client
  httpConnect,
  inferParamTypes,
  int,
  int32,
  LOG_LEVEL_KEY,
  LOG_MESSAGE_KEY,
  // Types
  MethodType,
  OutputCollector,
  // Server
  Protocol,
  REQUEST_VERSION_KEY,
  // Constants
  RPC_METHOD_KEY,
  // Errors
  RpcError,
  // Schema shorthand
  str,
  toSchema,
  VersionError,
  VgiRpcServer,
} from "../src/index.js";

// ---------------------------------------------------------------------------
// 1. Verify exports exist and are the right type
// ---------------------------------------------------------------------------
function assertDefined(name: string, value: unknown): void {
  if (value === undefined || value === null) {
    throw new Error(`Expected ${name} to be defined`);
  }
}

function assertType(name: string, value: unknown, expected: string): void {
  const actual = typeof value;
  if (actual !== expected) {
    throw new Error(`Expected ${name} to be ${expected}, got ${actual}`);
  }
}

assertType("Protocol", Protocol, "function");
assertType("VgiRpcServer", VgiRpcServer, "function");
assertType("createHttpHandler", createHttpHandler, "function");
assertType("httpConnect", httpConnect, "function");
assertType("toSchema", toSchema, "function");
assertType("inferParamTypes", inferParamTypes, "function");
assertType("RpcError", RpcError, "function");
assertType("VersionError", VersionError, "function");
assertType("OutputCollector", OutputCollector, "function");
assertDefined("MethodType", MethodType);
assertDefined("str", str);
assertDefined("int", int);
assertDefined("float", float);
assertDefined("bool", bool);
assertDefined("int32", int32);
assertDefined("float32", float32);
assertDefined("bytes", bytes);
assertType("ARROW_CONTENT_TYPE", ARROW_CONTENT_TYPE, "string");
assertType("RPC_METHOD_KEY", RPC_METHOD_KEY, "string");
assertType("REQUEST_VERSION_KEY", REQUEST_VERSION_KEY, "string");
assertType("LOG_LEVEL_KEY", LOG_LEVEL_KEY, "string");
assertType("LOG_MESSAGE_KEY", LOG_MESSAGE_KEY, "string");

// ---------------------------------------------------------------------------
// 2. Build a Protocol with unary methods using schema shorthand
// ---------------------------------------------------------------------------
const protocol = new Protocol("SmokeTest");

protocol.unary("add", {
  params: { a: float, b: float },
  result: { result: float },
  handler: async ({ a, b }) => ({ result: a + b }),
  doc: "Add two numbers.",
});

protocol.unary("greet", {
  params: { name: str },
  result: { greeting: str },
  handler: async ({ name }) => ({ greeting: `Hello, ${name}!` }),
  doc: "Greet by name.",
});

protocol.unary("noop", {
  params: {},
  result: { ok: bool },
  handler: async () => ({ ok: true }),
  doc: "No-op method.",
});

// ---------------------------------------------------------------------------
// 3. Exercise toSchema / inferParamTypes
// ---------------------------------------------------------------------------
const schema = toSchema({ x: int, y: float });
if (schema.fields.length !== 2) {
  throw new Error(`Expected 2 fields, got ${schema.fields.length}`);
}

const paramTypes = inferParamTypes({ a: int32, b: str });
if (Object.keys(paramTypes).length !== 2) {
  throw new Error("inferParamTypes failed");
}

// ---------------------------------------------------------------------------
// 4. Create HTTP handler and start a server
// ---------------------------------------------------------------------------
const handler = createHttpHandler(protocol, {
  prefix: "/vgi",
  serverId: "smoke-test",
  enableDescribe: true,
});

interface ServerHandle {
  port: number;
  close: () => void | Promise<void>;
}

async function startServer(): Promise<ServerHandle> {
  // Bun
  if (typeof globalThis.Bun !== "undefined") {
    const server = Bun.serve({ port: 0, fetch: handler });
    return { port: server.port, close: () => server.stop() };
  }

  // Deno
  if (typeof (globalThis as any).Deno !== "undefined") {
    const Deno = (globalThis as any).Deno;
    const ac = new AbortController();
    let resolvePort: (port: number) => void;
    const portPromise = new Promise<number>((r) => {
      resolvePort = r;
    });
    const server = Deno.serve(
      { port: 0, signal: ac.signal, onListen: ({ port }: { port: number }) => resolvePort(port) },
      handler,
    );
    const port = await portPromise;
    return {
      port,
      close: async () => {
        ac.abort();
        await server.finished;
      },
    };
  }

  // Node.js
  const http = await import("node:http");
  return new Promise<ServerHandle>((resolve) => {
    const server = http.createServer(async (req, res) => {
      // Convert Node IncomingMessage to a Web Request
      const proto = "http";
      const url = `${proto}://${req.headers.host}${req.url}`;
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(chunk);
      const body = Buffer.concat(chunks);

      const webReq = new Request(url, {
        method: req.method,
        headers: req.headers as Record<string, string>,
        body: ["GET", "HEAD"].includes(req.method!) ? undefined : body,
      });

      const webResp = await handler(webReq);

      res.writeHead(webResp.status, Object.fromEntries(webResp.headers.entries()));
      const respBody = new Uint8Array(await webResp.arrayBuffer());
      res.end(respBody);
    });

    server.listen(0, () => {
      const addr = server.address() as { port: number };
      resolve({
        port: addr.port,
        close: () => server.close(),
      });
    });
  });
}

// ---------------------------------------------------------------------------
// 5. Run client tests
// ---------------------------------------------------------------------------
async function main(): Promise<void> {
  const server = await startServer();
  const baseUrl = `http://localhost:${server.port}`;

  try {
    const client = httpConnect(baseUrl);

    // 5a. Call unary methods
    const addResult = await client.call("add", { a: 2, b: 3 });
    if (!addResult || addResult.result !== 5) {
      throw new Error(`add: expected { result: 5 }, got ${JSON.stringify(addResult)}`);
    }

    const greetResult = await client.call("greet", { name: "World" });
    if (!greetResult || greetResult.greeting !== "Hello, World!") {
      throw new Error(`greet: expected greeting "Hello, World!", got ${JSON.stringify(greetResult)}`);
    }

    const noopResult = await client.call("noop", {});
    if (!noopResult || noopResult.ok !== true) {
      throw new Error(`noop: expected { ok: true }, got ${JSON.stringify(noopResult)}`);
    }

    // 5b. Describe
    const desc = await client.describe();
    if (desc.protocolName !== "SmokeTest") {
      throw new Error(`describe: expected protocolName "SmokeTest", got "${desc.protocolName}"`);
    }
    const methodNames = desc.methods.map((m) => m.name).sort();
    const expected = ["add", "greet", "noop"].sort();
    if (JSON.stringify(methodNames) !== JSON.stringify(expected)) {
      throw new Error(`describe: expected methods ${JSON.stringify(expected)}, got ${JSON.stringify(methodNames)}`);
    }

    client.close();
    console.log("PASS");
  } finally {
    await server.close();
  }
}

main().catch((err) => {
  console.error("FAIL:", err);
  process.exit(1);
});
