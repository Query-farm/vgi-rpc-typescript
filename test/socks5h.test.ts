// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { afterEach, describe, expect, test } from "bun:test";
import { createServer as createHttpServer } from "node:http";
import { createConnection, createServer, type Server, type Socket } from "node:net";
import {
  createSocks5hFetch,
  dialSocks5h,
  httpConnectSocks5h,
  parseSocks5hProxy,
  tcpConnectSocks5h,
} from "../src/client/socks5h.js";
import { createHttpHandler } from "../src/http/handler.js";
import { serveTcp } from "../src/launcher/serve-tcp.js";
import { Protocol } from "../src/protocol.js";
import { str } from "../src/schema.js";

const servers: Array<Server | ReturnType<typeof createHttpServer>> = [];
const protocol = new Protocol("SocksTest").unary("ping", {
  params: { value: str },
  result: { value: str },
  handler: ({ value }) => ({ value: `pong:${value}` }),
});
afterEach(async () => {
  for (const server of servers.splice(0)) {
    server.close();
    if ("closeAllConnections" in server) server.closeAllConnections();
  }
});

async function listen(server: Server | ReturnType<typeof createHttpServer>): Promise<number> {
  server.listen(0, "127.0.0.1");
  await new Promise((resolve) => server.once("listening", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("missing address");
  return address.port;
}

function exact(socket: Socket, size: number): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const cleanup = () => {
      socket.off("readable", readable);
      socket.off("end", ended);
      socket.off("error", failed);
    };
    const readable = () => {
      const chunk = socket.read(size) as Buffer | null;
      if (!chunk) return;
      cleanup();
      resolve(chunk);
    };
    const ended = () => {
      cleanup();
      reject(new Error("truncated test request"));
    };
    const failed = (error: Error) => {
      cleanup();
      reject(error);
    };
    socket.on("readable", readable);
    socket.once("end", ended);
    socket.once("error", failed);
    readable();
  });
}

async function targetFromRequest(socket: Socket): Promise<{ host: string; port: number; atyp: number }> {
  const head = await exact(socket, 4);
  let host: string;
  if (head[3] === 1) host = [...(await exact(socket, 4))].join(".");
  else if (head[3] === 4) host = (await exact(socket, 16)).toString("hex");
  else {
    const length = (await exact(socket, 1))[0];
    host = (await exact(socket, length)).toString("ascii");
  }
  const portBytes = await exact(socket, 2);
  return { host, port: portBytes.readUInt16BE(), atyp: head[3] };
}

async function relayProxy(upstreamPort: number, onTarget?: (host: string) => void): Promise<number> {
  const proxy = createServer(async (client) => {
    await exact(client, 3);
    client.write(Buffer.from([5, 0]));
    const target = await targetFromRequest(client);
    onTarget?.(target.host);
    const upstream = createConnection({ host: "127.0.0.1", port: upstreamPort }, () => {
      client.write(Buffer.from([5, 0, 0, 1, 127, 0, 0, 1, upstreamPort >>> 8, upstreamPort & 255]));
      client.pipe(upstream).pipe(client);
    });
  });
  servers.push(proxy);
  return listen(proxy);
}

describe("SOCKS5h", () => {
  test("strictly rejects credentials and options before network access", () => {
    for (const value of [
      "socks5://127.0.0.1:1080",
      "socks5h://user@127.0.0.1:1080",
      "socks5h://127.0.0.1:1080/path",
      "socks5h://127.0.0.1:1080?x=1",
      "socks5h://127.0.0.1",
    ])
      expect(() => parseSocks5hProxy(value)).toThrow();
  });

  test("sends unresolved IDNA target domains and accepts partial replies", async () => {
    let target: Awaited<ReturnType<typeof targetFromRequest>> | undefined;
    const proxy = createServer(async (socket) => {
      expect(await exact(socket, 3)).toEqual(Buffer.from([5, 1, 0]));
      socket.write(Buffer.from([5]));
      socket.write(Buffer.from([0]));
      target = await targetFromRequest(socket);
      for (const byte of [5, 0, 0, 3, 2, 111, 107, 0x12, 0x34]) socket.write(Buffer.from([byte]));
    });
    servers.push(proxy);
    const port = await listen(proxy);
    const socket = await dialSocks5h(`socks5h://127.0.0.1:${port}`, "bücher.example", 9400);
    expect(target).toEqual({ host: "xn--bcher-kva.example", port: 9400, atyp: 3 });
    socket.destroy();
  });

  test("encodes IPv4 and IPv6 target literals without local DNS", async () => {
    const seen: number[] = [];
    const proxy = createServer(async (socket) => {
      await exact(socket, 3);
      socket.write(Buffer.from([5, 0]));
      const target = await targetFromRequest(socket);
      seen.push(target.atyp);
      socket.write(Buffer.from([5, 0, 0, 1, 127, 0, 0, 1, 0, 1]));
    });
    servers.push(proxy);
    const port = await listen(proxy);
    (await dialSocks5h(`socks5h://127.0.0.1:${port}`, "192.0.2.4", 80)).destroy();
    (await dialSocks5h(`socks5h://127.0.0.1:${port}`, "2001:db8::4", 80)).destroy();
    expect(seen).toEqual([1, 4]);
  });

  test("uses one deadline, observes AbortSignal, and never falls back directly", async () => {
    const stalled = createServer(async (socket) => {
      await exact(socket, 3);
    });
    servers.push(stalled);
    const port = await listen(stalled);
    const started = performance.now();
    await expect(
      dialSocks5h(`socks5h://127.0.0.1:${port}`, "example.invalid", 80, { connectTimeoutMs: 30 }),
    ).rejects.toThrow(/deadline/i);
    expect(performance.now() - started).toBeLessThan(250);

    const controller = new AbortController();
    const cancelled = dialSocks5h(`socks5h://127.0.0.1:${port}`, "example.invalid", 80, {
      connectTimeoutMs: 1000,
      signal: controller.signal,
    });
    controller.abort(new Error("stop now"));
    await expect(cancelled).rejects.toThrow(/stop now/);

    let directConnections = 0;
    const direct = createServer((socket) => {
      directConnections++;
      socket.destroy();
    });
    servers.push(direct);
    const directPort = await listen(direct);
    const refusing = createServer(async (socket) => {
      await exact(socket, 3);
      socket.end(Buffer.from([5, 0xff]));
    });
    servers.push(refusing);
    const refusingPort = await listen(refusing);
    await expect(dialSocks5h(`socks5h://127.0.0.1:${refusingPort}`, "127.0.0.1", directPort)).rejects.toThrow(
      /NO AUTH/,
    );
    expect(directConnections).toBe(0);
  });

  test("HTTP fetch uses the tunnel and leaves domain resolution to the proxy", async () => {
    const targetServer = createHttpServer((request, response) => {
      expect(request.url).toBe("/health");
      response.setHeader("X-Via", "target");
      response.end("ok");
    });
    servers.push(targetServer);
    const targetPort = await listen(targetServer);
    let requestedHost = "";
    const proxy = createServer(async (client) => {
      await exact(client, 3);
      client.write(Buffer.from([5, 0]));
      const target = await targetFromRequest(client);
      requestedHost = target.host;
      const upstream = createConnection({ host: "127.0.0.1", port: targetPort }, () => {
        client.write(Buffer.from([5, 0, 0, 1, 127, 0, 0, 1, targetPort >>> 8, targetPort & 255]));
        client.pipe(upstream).pipe(client);
      });
    });
    servers.push(proxy);
    const proxyPort = await listen(proxy);
    const response = await createSocks5hFetch(`socks5h://127.0.0.1:${proxyPort}`)(
      `http://rpc.internal:${targetPort}/health`,
    );
    expect(await response.text()).toBe("ok");
    expect(response.headers.get("x-via")).toBe("target");
    expect(requestedHost).toBe("rpc.internal");
  });

  test("HTTP fetch parses headers and chunks split across socket reads", async () => {
    const fragmented = createServer((socket) => {
      socket.once("data", () => {
        const pieces = ["HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r", "\n4\r", "\nWiki\r\n0\r", "\n\r\n"];
        let index = 0;
        const send = () => {
          socket.write(pieces[index++]);
          if (index === pieces.length) socket.end();
          else setTimeout(send, 2);
        };
        send();
      });
    });
    servers.push(fragmented);
    const fragmentedPort = await listen(fragmented);
    const fragmentedProxy = await relayProxy(fragmentedPort);
    const response = await createSocks5hFetch(`socks5h://127.0.0.1:${fragmentedProxy}`)(
      "http://worker.invalid/fragmented",
    );
    expect(await response.text()).toBe("Wiki");
  });

  test("HTTP fetch rejects ambiguous framing, bounds responses, and times out", async () => {
    const malformedResponses: Array<[string, RegExp]> = [
      ["HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Length: 2\r\n\r\nok", /framing/],
      ["HTTP/1.1 200 OK\r\nContent-Length: 2\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n", /framing/],
      ["HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n", /framing/],
      ["HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\n\r\n", /Transfer-Encoding/],
      ["HTTP/1.1 200 OK\r\nContent-Length: 2, 2\r\n\r\nok", /Content-Length/],
      ["HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nTransfer-Encoding: chunked\r\n\r\n", /framing/],
    ];
    let responseIndex = 0;
    const malformed = createServer((socket) => {
      socket.once("data", () => socket.end(malformedResponses[responseIndex++][0]));
    });
    servers.push(malformed);
    const malformedPort = await listen(malformed);
    const malformedProxy = await relayProxy(malformedPort);
    const malformedFetch = createSocks5hFetch(`socks5h://127.0.0.1:${malformedProxy}`);
    for (const [_response, expected] of malformedResponses) {
      await expect(malformedFetch(`http://worker.invalid/malformed-${responseIndex}`)).rejects.toThrow(expected);
    }

    const oversized = createServer((socket) => {
      socket.once("data", () => {
        socket.write("HTTP/1.1 200 OK\r\nContent-Length: 70000\r\n\r\n");
      });
    });
    servers.push(oversized);
    const oversizedPort = await listen(oversized);
    const oversizedProxy = await relayProxy(oversizedPort);
    const oversizedStarted = performance.now();
    await expect(
      createSocks5hFetch(
        `socks5h://127.0.0.1:${oversizedProxy}`,
        5_000,
        5_000,
        65_536,
      )("http://worker.invalid/oversized"),
    ).rejects.toThrow(/limit/);
    expect(performance.now() - oversizedStarted).toBeLessThan(250);

    const oversizedHeaders = createServer((socket) => {
      socket.once("data", () => socket.end(`HTTP/1.1 200 OK\r\nX-Large: ${"x".repeat(512)}\r\n\r\n`));
    });
    servers.push(oversizedHeaders);
    const oversizedHeadersPort = await listen(oversizedHeaders);
    const oversizedHeadersProxy = await relayProxy(oversizedHeadersPort);
    await expect(
      createSocks5hFetch(
        `socks5h://127.0.0.1:${oversizedHeadersProxy}`,
        5_000,
        5_000,
        2_048,
        128,
      )("http://worker.invalid/oversized-headers"),
    ).rejects.toThrow(/headers/);

    const truncated = createServer((socket) => {
      socket.once("data", () => socket.end("HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort"));
    });
    servers.push(truncated);
    const truncatedPort = await listen(truncated);
    const truncatedProxy = await relayProxy(truncatedPort);
    await expect(
      createSocks5hFetch(`socks5h://127.0.0.1:${truncatedProxy}`)("http://worker.invalid/truncated"),
    ).rejects.toThrow(/truncated/);

    const stalled = createServer((socket) => socket.once("data", () => {}));
    servers.push(stalled);
    const stalledPort = await listen(stalled);
    const stalledProxy = await relayProxy(stalledPort);
    const started = performance.now();
    await expect(
      createSocks5hFetch(`socks5h://127.0.0.1:${stalledProxy}`, 5_000, 30)("http://worker.invalid/stall"),
    ).rejects.toThrow();
    expect(performance.now() - started).toBeLessThan(250);
  });

  test("HTTP fetch validates request framing and method before dialing", async () => {
    const unreachable = "socks5h://127.0.0.1:1";
    const socksFetch = createSocks5hFetch(unreachable);
    await expect(socksFetch("http://worker.invalid/", { method: "GET\r\nX-Evil: yes" })).rejects.toThrow(/method/);
    await expect(
      socksFetch("http://worker.invalid/", { body: "ok", method: "POST", headers: { "Content-Length": "3" } }),
    ).rejects.toThrow(/Content-Length/);
    await expect(socksFetch("http://worker.invalid/", { headers: { "Transfer-Encoding": "chunked" } })).rejects.toThrow(
      /Transfer-Encoding/,
    );
  });

  test("HTTP RPC constructor keeps introspection and calls on the tunnel", async () => {
    const rpcServer = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: createHttpHandler(protocol) });
    let requestedHost = "";
    const proxyPort = await relayProxy(rpcServer.port, (host) => {
      requestedHost = host;
    });
    const client = httpConnectSocks5h(`http://worker.internal:${rpcServer.port}`, `socks5h://127.0.0.1:${proxyPort}`);
    try {
      expect(await client.call("ping", { value: "http" })).toEqual({ value: "pong:http" });
      expect(requestedHost).toBe("worker.internal");
    } finally {
      client.close();
      await rpcServer.stop(true);
    }
  });

  test("raw TCP RPC constructor uses the negotiated SOCKS tunnel", async () => {
    const rpcServer = await serveTcp(protocol, {
      host: "127.0.0.1",
      port: 0,
      idleTimeout: 0,
      announcementSink: { write: () => true } as unknown as NodeJS.WritableStream,
    });
    const proxyPort = await relayProxy(rpcServer.port);
    const client = await tcpConnectSocks5h("worker.internal", rpcServer.port, `socks5h://127.0.0.1:${proxyPort}`);
    try {
      expect(await client.call("ping", { value: "tcp" })).toEqual({ value: "pong:tcp" });
    } finally {
      client.close();
      await rpcServer.stop();
    }
  });
});
