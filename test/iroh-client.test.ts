// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import {
  IROH_ARROW_MUX_ALPN,
  IROH_HTTP_ALPN,
  type IrohNativeBinding,
  IrohTransportError,
  IrohUriError,
  irohConnect,
  parseIrohEndpoint,
} from "../src/client/iroh.js";

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
