// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * The connection-reusing reflection client: `listProtocols` and
 * `describeProtocol` over a client the caller already holds.
 *
 * Every transport this port's client speaks is driven against this port's
 * own conformance host and, where the Python reference is importable, against
 * the reference conformance worker (`vgi-rpc-conformance --describe`). Both
 * host the primary, then `conformance.Secondary.v1`, then reflection.
 *
 * "The held connection" is checked, not assumed: the in-memory pipe and the
 * unix client count the bytes they write and fail if their writable is ended;
 * the HTTP client counts the requests through its own fetch. A reflection
 * hook that closed the connection, or reached the server some other way,
 * turns these red.
 */

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { createConnection, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createNode } from "@momics/iroh-http-node";
import type { Subprocess } from "bun";
import { field, schema, singleRowBatch, utf8 } from "#vgi-rpc-arrow";
import { conformanceHost, protocol as conformanceProtocol } from "../examples/conformance-protocol.js";
import { REFLECTION_CALL } from "../src/client/introspect.js";
import { buildSecondaryProtocol } from "../src/conformance/secondary.js";
import {
  PROTOCOL_KEY,
  PROTOCOL_VERSION_KEY,
  REQUEST_VERSION,
  REQUEST_VERSION_KEY,
  RPC_METHOD_KEY,
} from "../src/constants.js";
import {
  createHttpHandler,
  describeProtocol,
  httpConnect,
  httpiConnect,
  listProtocols,
  type PipeWritable,
  pipeConnect,
  ReflectionNotSupportedError,
  type RpcClient,
  RpcError,
  serveTcp,
  serveUnix,
  subprocessConnect,
  tcpConnect,
} from "../src/index.js";
import { VgiRpcServer } from "../src/server.js";
import { TransportKind } from "../src/types.js";
import { PYTHON_BIN } from "./reference.js";

const PRIMARY = "ConformanceService";
const SECONDARY = "conformance.Secondary.v1";
const REFLECTION = "vgi_rpc.Reflection.v1";
const EXPECTED_ORDER = [PRIMARY, SECONDARY, REFLECTION];

const pythonOk =
  Bun.spawnSync([PYTHON_BIN, "-c", "import vgi_rpc.conformance._cli, waitress"], { stderr: "ignore" }).exitCode === 0;
const describePython = pythonOk ? describe : describe.skip;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/** A spy on a byte-stream client's writable: how much it wrote, and whether
 *  anything ended it. */
interface WriteSpy {
  writes: number;
  ended: boolean;
}

function spyWritable(inner: PipeWritable, spy: WriteSpy): PipeWritable {
  return {
    write(data) {
      spy.writes++;
      return inner.write(data);
    },
    flush() {
      inner.flush?.();
    },
    end() {
      spy.ended = true;
      inner.end();
    },
  };
}

/** A client over an in-memory pipe to `server`, which serves exactly this
 *  one connection. */
function inMemoryClient(server: VgiRpcServer, spy: WriteSpy): RpcClient {
  let toServer!: ReadableStreamDefaultController<Uint8Array>;
  let toClient!: ReadableStreamDefaultController<Uint8Array>;
  const serverIn = new ReadableStream<Uint8Array>({
    start(c) {
      toServer = c;
    },
  });
  const clientIn = new ReadableStream<Uint8Array>({
    start(c) {
      toClient = c;
    },
  });
  void server
    .serveConnection(serverIn, { write: (bytes) => toClient.enqueue(bytes.slice()) }, TransportKind.PIPE)
    .catch(() => {});
  return pipeConnect(
    clientIn,
    spyWritable(
      {
        write: (data) => toServer.enqueue(data.slice()),
        end: () => toServer.close(),
      },
      spy,
    ),
  );
}

/** A client over an AF_UNIX socket: the port has no dedicated constructor,
 *  so this is `pipeConnect` over the socket, exactly as `tcpConnect` is. */
async function unixClient(path: string, spy: WriteSpy): Promise<{ client: RpcClient; socket: Socket }> {
  const socket = createConnection(path);
  await new Promise<void>((resolve, reject) => {
    socket.once("connect", resolve);
    socket.once("error", reject);
  });
  const client = pipeConnect(
    socket as unknown as ReadableStream<Uint8Array>,
    spyWritable({ write: (d) => void socket.write(d), end: () => void socket.end() }, spy),
  );
  return { client, socket };
}

/** An HTTP client whose every request is counted. */
function countingHttpClient(baseUrl: string): { client: RpcClient; requests: string[] } {
  const requests: string[] = [];
  const counted = ((input: RequestInfo | URL, init?: RequestInit) => {
    requests.push(typeof input === "string" ? input : input instanceof URL ? input.href : input.url);
    return fetch(input, init);
  }) as typeof globalThis.fetch;
  return { client: httpConnect(baseUrl, { fetch: counted }), requests };
}

/** Spawn a Python reference conformance worker and wait for its announcement. */
async function spawnPython(args: string[], prefix: string): Promise<{ proc: Subprocess; value: string }> {
  const proc = Bun.spawn([PYTHON_BIN, "-m", "vgi_rpc.conformance._cli", ...args], {
    stdout: "pipe",
    stderr: "ignore",
  });
  const reader = (proc.stdout as ReadableStream<Uint8Array>).getReader();
  const decoder = new TextDecoder();
  let out = "";
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    const { value, done } = await reader.read();
    if (done) break;
    out += decoder.decode(value);
    const line = out.split("\n").find((l) => l.startsWith(prefix));
    if (line && out.includes("\n")) {
      reader.releaseLock();
      return { proc, value: line.slice(prefix.length).trim() };
    }
  }
  proc.kill();
  throw new Error(`reference worker did not announce '${prefix}': ${out}`);
}

async function waitForHttp(baseUrl: string): Promise<void> {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    try {
      await (await fetch(`${baseUrl}/health`)).arrayBuffer();
      return;
    } catch {
      await Bun.sleep(25);
    }
  }
  throw new Error(`no HTTP server at ${baseUrl}`);
}

// ---------------------------------------------------------------------------
// The shared assertions
// ---------------------------------------------------------------------------

/** Listing, describe, unknown-protocol, and the connection still answering
 *  its own protocol afterwards. */
async function exerciseReflection(client: RpcClient): Promise<void> {
  // Bind the client first, so reflection runs on an already-used connection.
  expect(await client.call("echo_string", { value: "before" })).toEqual({ result: "before" });

  const listed = await listProtocols(client);
  expect(listed.map((p) => p.name)).toEqual(EXPECTED_ORDER);
  for (const p of listed) {
    expect(p.hash).toMatch(/^[0-9a-f]{64}$/);
    expect(p.deprecated).toBe(false);
    expect(p.deprecationMessage).toBe("");
    expect(Array.isArray(p.features)).toBe(true);
    expect(Object.isFrozen(p)).toBe(true);
  }

  const secondary = await describeProtocol(client, SECONDARY);
  expect(secondary.protocolName).toBe(SECONDARY);
  expect(secondary.protocolHash).toBe(listed[1].hash);
  expect(secondary.hostedProtocols).toEqual(EXPECTED_ORDER);
  expect(secondary.methods.map((m) => m.name)).toContain("echo_string");

  const primary = await describeProtocol(client, PRIMARY);
  expect(primary.protocolHash).toBe(listed[0].hash);

  // An unknown name is an ordinary RPC error, not "no reflection".
  const unknown = await describeProtocol(client, "no.such.Protocol.v1").catch((e) => e);
  expect(unknown).toBeInstanceOf(RpcError);
  expect(unknown).not.toBeInstanceOf(ReflectionNotSupportedError);
  expect((unknown as RpcError).errorKind).toBe("protocol_not_supported");

  // The held connection is still the caller's.
  expect(await client.call("echo_string", { value: "after" })).toEqual({ result: "after" });
}

/** `echo_string` as an encoded call: needs no description, so it works on a
 *  client that has no reflection to bind through. */
async function rawEcho(client: RpcClient, value: string): Promise<string | null> {
  const reply = await client.callRaw("echo_string", {
    batch: singleRowBatch(schema([field("value", utf8(), false)]), { value }) as any,
    metadata: new Map([
      [RPC_METHOD_KEY, "echo_string"],
      [PROTOCOL_KEY, PRIMARY],
      [REQUEST_VERSION_KEY, REQUEST_VERSION],
      // Both conformance primaries declare a version and gate on it.
      [PROTOCOL_VERSION_KEY, conformanceProtocol.protocolVersion ?? ""],
    ]),
  });
  return reply ? String(reply.batch.getChildAt(0)?.get(0)) : null;
}

/** "No reflection" is its own error, carries the server's fields, and leaves
 *  the connection usable. */
async function exerciseNoReflection(client: RpcClient): Promise<void> {
  expect(await rawEcho(client, "before")).toBe("before");

  const listed = await listProtocols(client).catch((e) => e);
  expect(listed).toBeInstanceOf(ReflectionNotSupportedError);
  expect(listed).toBeInstanceOf(RpcError);
  const err = listed as ReflectionNotSupportedError;
  expect(err.errorType).not.toBe("");
  expect(err.errorKind === "protocol_not_supported" || err.errorCode === "UNIMPLEMENTED").toBe(true);

  const described = await describeProtocol(client, PRIMARY).catch((e) => e);
  expect(described).toBeInstanceOf(ReflectionNotSupportedError);

  // The connection still carries the next request.
  expect(await rawEcho(client, "after")).toBe("after");
}

function noReflectionHost(): VgiRpcServer {
  return new VgiRpcServer(conformanceProtocol, {
    enableDescribe: false,
    protocols: [buildSecondaryProtocol()],
    grantKeys: null,
  });
}

// ---------------------------------------------------------------------------
// This port's own server
// ---------------------------------------------------------------------------

describe("listProtocols / describeProtocol against the TypeScript server", () => {
  test("in-memory pipe: reuses the held stream and never ends it", async () => {
    const spy: WriteSpy = { writes: 0, ended: false };
    const client = inMemoryClient(conformanceHost({ grantKeys: null }), spy);
    try {
      await client.call("echo_string", { value: "warm" });
      const before = spy.writes;
      await listProtocols(client);
      expect(spy.writes).toBeGreaterThan(before);
      await exerciseReflection(client);
      expect(spy.ended).toBe(false);
    } finally {
      client.close();
    }
  });

  test("in-memory pipe, without reflection", async () => {
    const spy: WriteSpy = { writes: 0, ended: false };
    const client = inMemoryClient(noReflectionHost(), spy);
    try {
      await exerciseNoReflection(client);
      expect(spy.ended).toBe(false);
    } finally {
      client.close();
    }
  });

  test("subprocess", async () => {
    const client = subprocessConnect(["bun", "run", join(import.meta.dir, "..", "examples", "conformance.ts")]);
    try {
      await exerciseReflection(client);
    } finally {
      client.close();
    }
  }, 30_000);

  test("subprocess bound to the secondary protocol", async () => {
    const client = subprocessConnect(["bun", "run", join(import.meta.dir, "..", "examples", "conformance.ts")], {
      protocol: SECONDARY,
    });
    try {
      expect(await client.call("echo_string", { value: "x" })).toEqual({ result: "secondary:x" });
      expect((await client.describe()).protocolName).toBe(SECONDARY);
      expect((await listProtocols(client)).map((p) => p.name)).toEqual(EXPECTED_ORDER);
      expect(await client.call("echo_string", { value: "y" })).toEqual({ result: "secondary:y" });
    } finally {
      client.close();
    }
  }, 30_000);

  test("tcp", async () => {
    const handle = await serveTcp(conformanceHost({ grantKeys: null }), {
      host: "127.0.0.1",
      port: 0,
      idleTimeout: 0,
      announcementSink: { write: () => true } as unknown as NodeJS.WritableStream,
    });
    const client = tcpConnect(handle.host, handle.port);
    try {
      await exerciseReflection(client);
    } finally {
      client.close();
      await handle.stop();
    }
  });

  test("unix", async () => {
    const dir = mkdtempSync(join(tmpdir(), "vgi-refl-"));
    const handle = await serveUnix(conformanceHost({ grantKeys: null }), {
      unixPath: join(dir, "s.sock"),
      idleTimeout: 0,
      announcementSink: { write: () => true } as unknown as NodeJS.WritableStream,
    });
    const spy: WriteSpy = { writes: 0, ended: false };
    const { client, socket } = await unixClient(handle.socketPath, spy);
    try {
      await exerciseReflection(client);
      expect(spy.ended).toBe(false);
    } finally {
      client.close();
      socket.destroy();
      await handle.stop();
      rmSync(dir, { recursive: true, force: true });
    }
  });

  describe("http", () => {
    let withReflection: ReturnType<typeof Bun.serve>;
    let without: ReturnType<typeof Bun.serve>;
    beforeAll(() => {
      withReflection = Bun.serve({
        port: 0,
        fetch: createHttpHandler(conformanceHost({ serverId: "refl-http", grantKeys: null })),
      });
      without = Bun.serve({ port: 0, fetch: createHttpHandler(noReflectionHost()) });
    });
    afterAll(() => {
      withReflection.stop(true);
      without.stop(true);
    });

    test("goes through the client's own fetch", async () => {
      const { client, requests } = countingHttpClient(`http://127.0.0.1:${withReflection.port}`);
      await client.call("echo_string", { value: "warm" });
      const before = requests.length;
      await listProtocols(client);
      expect(requests.length).toBe(before + 1);
      expect(requests[requests.length - 1]).toEndWith(`/${REFLECTION}/list_protocols`);
      await exerciseReflection(client);
    });

    test("without reflection", async () => {
      const { client, requests } = countingHttpClient(`http://127.0.0.1:${without.port}`);
      await exerciseNoReflection(client);
      expect(requests.some((url) => url.endsWith(`/${REFLECTION}/list_protocols`))).toBe(true);
    });
  });

  test("httpi (HTTP over Iroh)", async () => {
    const serverNode = await createNode({ relay: { mode: "disabled" } });
    const clientNode = await createNode({ relay: { mode: "disabled" } });
    const server = serverNode.serve(createHttpHandler(conformanceHost({ grantKeys: null }), { prefix: "/vgi" }));
    try {
      const discovery = await serverNode.discoveryInfo();
      const endpointHex = Buffer.from(serverNode.publicKey.bytes).toString("hex");
      const client = await httpiConnect(`httpi://${endpointHex}/vgi`, {
        node: clientNode,
        directAddresses: discovery.directAddresses,
        requestTimeoutMs: 10_000,
      });
      try {
        await exerciseReflection(client);
      } finally {
        client.close();
      }
    } finally {
      await server.close();
      await clientNode.close({ force: true });
      await serverNode.close({ force: true });
    }
  }, 30_000);
});

// ---------------------------------------------------------------------------
// Older servers and odd targets, through the hook
// ---------------------------------------------------------------------------

describe("classifying a server without reflection", () => {
  /** A target whose reflection call fails with `error`, and how often it was asked. */
  function failing(error: unknown): { target: RpcClient; calls: string[] } {
    const calls: string[] = [];
    const target = {} as RpcClient;
    Object.defineProperty(target, REFLECTION_CALL, {
      value: async (method: string) => {
        calls.push(method);
        throw error;
      },
    });
    return { target, calls };
  }

  const notHosted: Array<[string, RpcError]> = [
    ["protocol_not_supported", new RpcError("ProtocolError", "no", "", { errorKind: "protocol_not_supported" })],
    ["method_not_implemented", new RpcError("AttributeError", "no", "", { errorKind: "method_not_implemented" })],
    ["UNIMPLEMENTED", new RpcError("Whatever", "no", "", { errorCode: "UNIMPLEMENTED" })],
    ["ProtocolNotSupportedError", new RpcError("ProtocolNotSupportedError", "no", "")],
    ["MethodNotImplementedError", new RpcError("MethodNotImplementedError", "no", "")],
    ["bare HTTP 404", new RpcError("HttpError", "HTTP 404: Not Found", "")],
  ];
  for (const [label, error] of notHosted) {
    test(`${label} is ReflectionNotSupportedError carrying the server's fields`, async () => {
      const { target, calls } = failing(error);
      const thrown = await listProtocols(target).catch((e) => e);
      expect(thrown).toBeInstanceOf(ReflectionNotSupportedError);
      expect(thrown.errorType).toBe(error.errorType);
      expect(thrown.errorMessage).toBe(error.errorMessage);
      expect(thrown.errorKind).toBe(error.errorKind);
      expect(thrown.errorCode).toBe(error.errorCode);
      // describe is never reached: "no reflection" stays distinct from "no such protocol".
      const described = await describeProtocol(target, "x").catch((e) => e);
      expect(described).toBeInstanceOf(ReflectionNotSupportedError);
      expect(calls).toEqual(["list_protocols", "list_protocols"]);
    });
  }

  test("an RpcError from another bundle of this package is classified by shape", async () => {
    // `.` and `./connect` are bundled separately, so their RpcError classes differ.
    class ForeignRpcError extends Error {
      errorType = "ProtocolError";
      errorMessage = "no";
      remoteTraceback = "";
      errorKind = "protocol_not_supported";
      errorCode = "UNIMPLEMENTED";
      errorDetails = [];
      requestId = "";
    }
    const thrown = await listProtocols(failing(new ForeignRpcError("no")).target).catch((e) => e);
    expect(thrown).toBeInstanceOf(ReflectionNotSupportedError);
    expect(thrown.errorKind).toBe("protocol_not_supported");
  });

  test("any other error propagates as itself", async () => {
    const other = new RpcError("ValueError", "boom", "", { errorKind: "invalid_argument" });
    const thrown = await listProtocols(failing(other).target).catch((e) => e);
    expect(thrown).toBe(other);
  });

  test("a bare 404 from an HTTP server is classified, never inferred as a listing", async () => {
    const bare = (async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
      if ((init?.method ?? "GET") === "HEAD" || url.endsWith("/health")) {
        return new Response(null, { status: 200, headers: { "VGI-Accept-Max-Response-Bytes-Support": "true" } });
      }
      return new Response("Not Found", { status: 404, headers: { "Content-Type": "text/plain" } });
    }) as typeof globalThis.fetch;
    const client = httpConnect("http://old.example", { fetch: bare });
    const thrown = await listProtocols(client).catch((e) => e);
    expect(thrown).toBeInstanceOf(ReflectionNotSupportedError);
    expect(thrown.errorType).toBe("HttpError");
  });

  test("an object that is not a client is a TypeError", async () => {
    await expect(listProtocols({} as RpcClient)).rejects.toBeInstanceOf(TypeError);
  });
});

// ---------------------------------------------------------------------------
// The Python reference conformance worker
// ---------------------------------------------------------------------------

describePython("listProtocols / describeProtocol against the Python reference", () => {
  test("pipe (subprocess)", async () => {
    const client = subprocessConnect([PYTHON_BIN, "-m", "vgi_rpc.conformance._cli", "--describe"]);
    try {
      await exerciseReflection(client);
    } finally {
      client.close();
    }
  }, 30_000);

  test("pipe (subprocess), without reflection", async () => {
    const client = subprocessConnect([PYTHON_BIN, "-m", "vgi_rpc.conformance._cli"]);
    try {
      await exerciseNoReflection(client);
    } finally {
      client.close();
    }
  }, 30_000);

  test("tcp", async () => {
    const { proc, value } = await spawnPython(["--tcp", "127.0.0.1:0", "--describe"], "TCP:");
    const idx = value.lastIndexOf(":");
    const client = tcpConnect(value.slice(0, idx), Number(value.slice(idx + 1)));
    try {
      await exerciseReflection(client);
    } finally {
      client.close();
      proc.kill();
    }
  }, 30_000);

  test("unix", async () => {
    const dir = mkdtempSync(join(tmpdir(), "vgi-refl-py-"));
    const { proc, value } = await spawnPython(["--unix", join(dir, "s.sock"), "--describe"], "UNIX:");
    const spy: WriteSpy = { writes: 0, ended: false };
    const { client, socket } = await unixClient(value, spy);
    try {
      await exerciseReflection(client);
      expect(spy.ended).toBe(false);
    } finally {
      client.close();
      socket.destroy();
      proc.kill();
      rmSync(dir, { recursive: true, force: true });
    }
  }, 30_000);

  test("http", async () => {
    const { proc, value } = await spawnPython(["--http", "0", "--describe"], "PORT:");
    const baseUrl = `http://127.0.0.1:${value}`;
    try {
      await waitForHttp(baseUrl);
      const { client, requests } = countingHttpClient(baseUrl);
      await client.call("echo_string", { value: "warm" });
      const before = requests.length;
      await listProtocols(client);
      expect(requests.length).toBe(before + 1);
      await exerciseReflection(client);
    } finally {
      proc.kill();
    }
  }, 30_000);

  test("http, without reflection", async () => {
    const { proc, value } = await spawnPython(["--http", "0"], "PORT:");
    const baseUrl = `http://127.0.0.1:${value}`;
    try {
      await waitForHttp(baseUrl);
      await exerciseNoReflection(httpConnect(baseUrl));
    } finally {
      proc.kill();
    }
  }, 30_000);
});
