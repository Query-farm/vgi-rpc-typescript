// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import { createNode, PublicKey } from "@momics/iroh-http-node";
import {
  IROH_ARROW_MUX_ALPN,
  IROH_HTTP_ALPN,
  type IrohNativeBinding,
  IrohTransportError,
  IrohUriError,
  irohConnect,
  parseIrohEndpoint,
} from "../src/client/iroh.js";
import {
  createHttpHandler,
  float,
  httpiConnect,
  type IrohHttpFetchInit,
  type IrohHttpNode,
  Protocol,
} from "../src/index.js";

const ID = "0123456789abcdef".repeat(4);
const vectors = (await Bun.file(new URL("./fixtures/iroh_transport_vectors.json", import.meta.url)).json()) as {
  alpns: { iroh: string; httpi: string };
  uri_cases: Array<{ uri: string; valid: boolean; scheme?: "iroh" | "httpi"; base_path?: string }>;
  error_cases: Array<{ stage: string; category: string; dispatch_certainty: string }>;
};

describe("Iroh endpoint contract", () => {
  test("official Node binding satisfies the native adapter surface", async () => {
    const official: IrohNativeBinding = await import("@number0/iroh");
    const builder = official.Endpoint.builder();
    builder.applyN0DisableRelay();
    const endpoint = await builder.bind();
    await endpoint.close();
  });

  test("parses raw and HTTP endpoints without changing the endpoint ID", () => {
    const raw = parseIrohEndpoint(`iroh://${ID}`);
    expect(raw.scheme).toBe("iroh");
    expect(raw.endpointId).toBe(ID);
    expect(raw.endpointIdBytes.byteLength).toBe(32);
    expect(raw.basePath).toBe("");
    expect(raw.alpn).toBe(IROH_ARROW_MUX_ALPN);

    const http = parseIrohEndpoint(`httpi://${ID}/api/v1`);
    expect(http.scheme).toBe("httpi");
    expect(http.basePath).toBe("/api/v1");
    expect(http.alpn).toBe(IROH_HTTP_ALPN);
  });

  test("passes every canonical URI fixture", () => {
    expect(IROH_ARROW_MUX_ALPN).toBe(vectors.alpns.iroh);
    expect(IROH_HTTP_ALPN).toBe(vectors.alpns.httpi);
    for (const vector of vectors.uri_cases) {
      if (!vector.valid) {
        expect(() => parseIrohEndpoint(vector.uri), vector.uri).toThrow(IrohUriError);
        continue;
      }
      const endpoint = parseIrohEndpoint(vector.uri);
      expect(endpoint.scheme, vector.uri).toBe(vector.scheme);
      expect(endpoint.basePath, vector.uri).toBe(vector.base_path);
    }
  });

  test("exposes all portable error dimensions from the canonical fixture", () => {
    const stages = new Set([
      "parse",
      "bind",
      "resolve",
      "connect",
      "alpn",
      "open_stream",
      "write",
      "read",
      "cancel",
      "close",
      "internal",
    ]);
    const categories = new Set([
      "invalid_input",
      "unsupported",
      "unavailable",
      "timeout",
      "protocol",
      "connection_reset",
      "cancelled",
      "authentication",
      "resource_exhausted",
      "internal",
    ]);
    const certainties = new Set(["not_sent", "unknown", "sent"]);
    for (const vector of vectors.error_cases) {
      expect(stages.has(vector.stage)).toBe(true);
      expect(categories.has(vector.category)).toBe(true);
      expect(certainties.has(vector.dispatch_certainty)).toBe(true);
    }
    const error = (() => {
      try {
        parseIrohEndpoint("invalid");
      } catch (caught) {
        return caught;
      }
    })();
    expect(error).toBeInstanceOf(IrohTransportError);
    expect(error).toMatchObject({ stage: "parse", category: "invalid_input", dispatchCertainty: "not_sent" });
  });

  test("reports cancellation and setup timeout structurally", async () => {
    const controller = new AbortController();
    controller.abort("test cancellation");
    await expect(irohConnect(`iroh://${ID}`, { signal: controller.signal })).rejects.toMatchObject({
      stage: "cancel",
      category: "cancelled",
      dispatchCertainty: "not_sent",
    });

    const stalled = {
      Endpoint: {
        builder: () => ({
          applyN0() {},
          applyN0DisableRelay() {},
          secretKey() {},
          relayMode() {},
          bind: () => new Promise(() => {}),
        }),
      },
    } as unknown as IrohNativeBinding;
    await expect(irohConnect(`iroh://${ID}`, { binding: stalled, connectTimeoutMs: 1 })).rejects.toMatchObject({
      stage: "bind",
      category: "timeout",
      dispatchCertainty: "not_sent",
    });
  });

  test.each([
    `iroh://${ID.toUpperCase()}`,
    `iroh://${ID}/`,
    `iroh://${ID}:443`,
    `iroh://user@${ID}`,
    `iroh://${ID}?x=1`,
    `httpi://${ID}/a//b`,
    `httpi://${ID}/a/../b`,
    `httpi://${ID}/bad%2`,
    `httpi://${ID}#fragment`,
  ])("rejects non-canonical endpoint %s", (endpoint) => {
    expect(() => parseIrohEndpoint(endpoint)).toThrow();
  });

  test("opens the canonical ALPN through the injectable native surface", async () => {
    let observedAlpn: number[] = [];
    const observedSecrets: number[][] = [];
    let observedRelay: string | null | undefined;
    let observedAddresses: string[] | null | undefined;
    let endpointClosed = false;
    const binding = {
      Endpoint: {
        builder: () => ({
          applyN0() {},
          applyN0DisableRelay() {},
          secretKey(bytes: number[]) {
            observedSecrets.push(bytes);
          },
          relayMode() {},
          async bind() {
            return {
              async connect(_address: unknown, alpn: number[]) {
                observedAlpn = alpn;
                return {
                  async openBi() {
                    return {
                      recv: {
                        async read() {
                          return [];
                        },
                        async stop() {},
                      },
                      send: { async writeAll() {}, async finish() {}, async reset() {} },
                    };
                  },
                  close() {},
                };
              },
              async close() {
                endpointClosed = true;
              },
            };
          },
        }),
      },
      EndpointId: { fromBytes: (bytes: number[]) => bytes },
      EndpointAddr: class {
        readonly id: unknown;
        constructor(id: unknown, relay?: string | null, addresses?: string[] | null) {
          this.id = id;
          observedRelay = relay;
          observedAddresses = addresses;
        }
      },
      RelayMode: { customFromUrls: (urls: string[]) => urls },
    } as IrohNativeBinding;

    const client = await irohConnect(`iroh://${ID}`, { binding });
    expect(new TextDecoder().decode(Uint8Array.from(observedAlpn))).toBe(IROH_ARROW_MUX_ALPN);
    client.close();
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(endpointClosed).toBe(true);
    endpointClosed = false;
    const second = await irohConnect(`iroh://${ID}`, {
      binding,
      remoteRelayUrl: "https://relay.example",
      directAddresses: ["127.0.0.1:4433"],
    });
    second.close();
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(observedSecrets).toHaveLength(2);
    expect(observedSecrets[0]).toEqual(observedSecrets[1]);
    expect(observedRelay).toBe("https://relay.example");
    expect(observedAddresses).toEqual(["127.0.0.1:4433"]);
  });
});

describe("HTTP-over-Iroh client", () => {
  function addProtocol(): Protocol {
    return new Protocol("IrohHttpTest").unary("add", {
      params: { a: float, b: float },
      result: { result: float },
      handler: ({ a, b }) => ({ result: a + b }),
    });
  }

  test("routes the complete VGI HTTP client through typed iroh-http fetch", async () => {
    const handler = createHttpHandler(addProtocol(), { prefix: "/vgi", serverId: "httpi-test" });
    const expectedNativeId = PublicKey.fromBytes(parseIrohEndpoint(`httpi://${ID}`).endpointIdBytes).toString();
    const requests: Array<{ url: string; init?: IrohHttpFetchInit }> = [];
    let nodeCloses = 0;
    const node: IrohHttpNode = {
      async fetch(input, init) {
        const url = String(input);
        requests.push({ url, init });
        const parsed = new URL(url);
        return handler(
          new Request(`http://vgi.test${parsed.pathname}`, {
            method: init?.method,
            headers: init?.headers,
            body: init?.body,
          }),
        );
      },
      async close() {
        nodeCloses++;
      },
    };

    const client = await httpiConnect(`httpi://${ID}/vgi`, {
      node,
      closeNode: true,
      directAddresses: ["127.0.0.1:4433"],
      remoteRelayUrl: "https://relay.example",
      requestTimeoutMs: 12_345,
    });
    expect(await client.call("add", { a: 2, b: 5 })).toEqual({ result: 7 });
    expect(requests.length).toBeGreaterThanOrEqual(3);
    for (const request of requests) {
      expect(new URL(request.url).hostname).toBe(expectedNativeId);
      expect(request.init?.directAddrs).toEqual(["127.0.0.1:4433"]);
      expect(request.init?.relayUrl).toBe("https://relay.example");
      expect(request.init?.requestTimeout).toBe(12_345);
      expect(request.init?.decompress).toBe(false);
      expect(request.init?.maxResponseBodyBytes).toBe(256 * 1024 * 1024);
    }
    client.close();
    await Bun.sleep(0);
    expect(nodeCloses).toBe(1);
  });

  test("owns an injected binding node and rejects raw Iroh endpoints", async () => {
    let created = 0;
    let closed = 0;
    const binding = {
      async createNode() {
        created++;
        return {
          async fetch() {
            throw new Error("unused");
          },
          async close() {
            closed++;
          },
        };
      },
    };
    const client = await httpiConnect(`httpi://${ID}`, { binding });
    expect(created).toBe(1);
    client.close();
    await Bun.sleep(0);
    expect(closed).toBe(1);
    await expect(httpiConnect(`iroh://${ID}`, { binding })).rejects.toMatchObject({
      stage: "parse",
      category: "invalid_input",
      dispatchCertainty: "not_sent",
    });
    await expect(
      httpiConnect(`httpi://${ID}`, {
        binding,
        acceptedMaxResponseBytes: 256 * 1024 * 1024 + 1,
      }),
    ).rejects.toMatchObject({
      stage: "parse",
      category: "invalid_input",
      dispatchCertainty: "not_sent",
    });
  });

  test("performs a real iroh-http/2 VGI request over native Iroh", async () => {
    const serverNode = await createNode({ relay: { mode: "disabled" } });
    const clientNode = await createNode({ relay: { mode: "disabled" } });
    const server = serverNode.serve(createHttpHandler(addProtocol(), { prefix: "/vgi", serverId: "httpi-live" }));
    try {
      const discovery = await serverNode.discoveryInfo();
      expect(discovery.directAddresses.length).toBeGreaterThan(0);
      const endpointHex = Buffer.from(serverNode.publicKey.bytes).toString("hex");
      const client = await httpiConnect(`httpi://${endpointHex}/vgi`, {
        node: clientNode,
        directAddresses: discovery.directAddresses,
        requestTimeoutMs: 10_000,
      });
      try {
        expect(await client.call("add", { a: 19, b: 23 })).toEqual({ result: 42 });
      } finally {
        client.close();
      }
    } finally {
      await server.close();
      await clientNode.close({ force: true });
      await serverNode.close({ force: true });
    }
  });
});
