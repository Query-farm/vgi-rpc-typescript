// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import { parseCapabilitiesFromHeaders, requireResponseBudgetSupport } from "../../src/client/capabilities.js";
import { httpConnect } from "../../src/client/connect.js";
import { decodeResponseBody, readResponseBodyBounded } from "../../src/client/decode.js";

describe("HTTP client response budgets", () => {
  test("capability decimals use the positive safe-integer grammar", () => {
    const headers = new Headers({
      "VGI-Max-Request-Bytes": "1",
      "VGI-Max-Response-Bytes": "9007199254740991",
      "VGI-Accept-Max-Response-Bytes-Support": "true",
    });
    const caps = parseCapabilitiesFromHeaders(headers);
    expect(caps.maxRequestBytes).toBe(1);
    expect(caps.maxResponseBytes).toBe(Number.MAX_SAFE_INTEGER);
    expect(caps.acceptMaxResponseBytesSupport).toBe(true);

    for (const value of ["0", "01", "+1", "1.0", "9007199254740992"]) {
      expect(() => parseCapabilitiesFromHeaders(new Headers({ "VGI-Max-Response-Bytes": value }))).toThrow();
    }
  });

  test("support capability requires one exact lowercase true value", () => {
    for (const value of ["false", "TRUE", "true, true"]) {
      expect(() =>
        requireResponseBudgetSupport(new Headers({ "VGI-Accept-Max-Response-Bytes-Support": value })),
      ).toThrow();
    }
  });

  test("native HTTP client advertises 256 MiB by default on introspection", async () => {
    let seen: string | null = null;
    const client = httpConnect("http://test", {
      fetch: async (_input, init) => {
        const request = new Request("http://test/__describe__", init);
        seen = request.headers.get("VGI-Accept-Max-Response-Bytes");
        if (request.method === "OPTIONS") {
          return new Response(null, {
            status: 204,
            headers: { "VGI-Accept-Max-Response-Bytes-Support": "true" },
          });
        }
        return new Response(null, {
          status: 200,
          headers: {
            "Content-Length": String(256 * 1024 * 1024 + 1),
            "VGI-Accept-Max-Response-Bytes-Support": "true",
          },
        });
      },
    });
    await expect(client.describe()).rejects.toThrow("accepted limit");
    expect(seen).toBe(String(256 * 1024 * 1024));
  });

  test("custom accepted limit is advertised and bounds streaming reads", async () => {
    let seen: string | null = null;
    const client = httpConnect("http://test", {
      acceptedMaxResponseBytes: 65_536,
      fetch: async (_input, init) => {
        const request = new Request("http://test/__describe__", init);
        seen = request.headers.get("VGI-Accept-Max-Response-Bytes");
        if (request.method === "OPTIONS") {
          return new Response(null, {
            status: 204,
            headers: { "VGI-Accept-Max-Response-Bytes-Support": "true" },
          });
        }
        return new Response(new Uint8Array(65_537), {
          status: 200,
          headers: { "VGI-Accept-Max-Response-Bytes-Support": "true" },
        });
      },
    });
    await expect(client.describe()).rejects.toThrow("accepted limit");
    expect(seen).toBe("65536");
  });

  test("the advertised server maximum also bounds the decoded response", async () => {
    const client = httpConnect("http://test", {
      acceptedMaxResponseBytes: 128 * 1024,
      fetch: async (_input, init) => {
        const request = new Request("http://test/__describe__", init);
        if (request.method === "OPTIONS") {
          return new Response(null, {
            status: 204,
            headers: { "VGI-Accept-Max-Response-Bytes-Support": "true" },
          });
        }
        return new Response(new Uint8Array(70_000), {
          status: 200,
          headers: {
            "VGI-Accept-Max-Response-Bytes-Support": "true",
            "VGI-Max-Response-Bytes": "65536",
          },
        });
      },
    });
    await expect(client.describe()).rejects.toThrow("65536");
  });

  test("bounded reader cancels the response stream on overshoot", async () => {
    let cancelled = false;
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array([1, 2, 3]));
        controller.enqueue(new Uint8Array([4, 5, 6]));
      },
      cancel() {
        cancelled = true;
      },
    });
    await expect(readResponseBodyBounded(new Response(stream), 4)).rejects.toThrow("accepted limit");
    expect(cancelled).toBe(true);
  });

  test("compressed representation uses an independent safety ceiling before decoded enforcement", async () => {
    const representation = new Uint8Array(65_537);
    const response = new Response(representation, {
      headers: {
        "Content-Length": String(representation.byteLength),
        "X-VGI-Content-Encoding": "zstd",
      },
    });
    const raw = await readResponseBodyBounded(response, 65_536, 70_000);
    expect(raw.byteLength).toBe(65_537);
    const decoded = await decodeResponseBody(
      response.headers,
      raw,
      async (_body, maxOutputSize) => {
        expect(maxOutputSize).toBe(65_536);
        return new Uint8Array(65_536);
      },
      65_536,
    );
    expect(decoded.byteLength).toBe(65_536);

    const overSafety = new Response(new Uint8Array(70_001), {
      headers: { "X-VGI-Content-Encoding": "zstd" },
    });
    await expect(readResponseBodyBounded(overSafety, 65_536, 70_000)).rejects.toThrow(
      "representation safety limit",
    );
  });

  test("transparent compression ignores encoded Content-Length while bounding decoded bytes", async () => {
    const response = new Response(new Uint8Array(65_536), {
      headers: { "Content-Encoding": "gzip", "Content-Length": "70000" },
    });
    const body = await readResponseBodyBounded(response, 65_536, 70_000);
    expect(body.byteLength).toBe(65_536);
  });

  test("identity Content-Length rejects and cancels before body accumulation", async () => {
    let cancelled = false;
    const stream = new ReadableStream<Uint8Array>({
      pull(controller) {
        controller.enqueue(new Uint8Array(1024));
      },
      cancel() {
        cancelled = true;
      },
    });
    const response = new Response(stream, { headers: { "Content-Length": "65537" } });
    await expect(readResponseBodyBounded(response, 65_536)).rejects.toThrow("accepted limit");
    expect(cancelled).toBe(true);
  });

  test("mandatory discovery is cached and every RPC response repeats exact support", async () => {
    let options = 0;
    let posts = 0;
    const client = httpConnect("http://test", {
      fetch: async (_input, init) => {
        const request = new Request("http://test/__describe__", init);
        if (request.method === "OPTIONS") {
          options += 1;
          return new Response(null, {
            status: 204,
            headers: { "VGI-Accept-Max-Response-Bytes-Support": "true" },
          });
        }
        posts += 1;
        return new Response(new Uint8Array(), {
          status: 200,
          headers: { "VGI-Accept-Max-Response-Bytes-Support": "true" },
        });
      },
    });
    await expect(client.describe()).rejects.toThrow();
    await expect(client.describe()).rejects.toThrow();
    expect(options).toBe(1);
    expect(posts).toBe(2);
  });

  test("missing support prevents dispatch and missing per-response support is rejected", async () => {
    let posts = 0;
    const unsupported = httpConnect("http://test", {
      fetch: async (_input, init) => {
        const request = new Request("http://test/__describe__", init);
        if (request.method === "OPTIONS") return new Response(null, { status: 204 });
        posts += 1;
        return new Response();
      },
    });
    await expect(unsupported.describe()).rejects.toThrow("before RPC dispatch");
    expect(posts).toBe(0);

    const missingOnRpc = httpConnect("http://test", {
      fetch: async (_input, init) => {
        const request = new Request("http://test/__describe__", init);
        if (request.method === "OPTIONS") {
          return new Response(null, {
            status: 204,
            headers: { "VGI-Accept-Max-Response-Bytes-Support": "true" },
          });
        }
        return new Response(new Uint8Array(), { status: 200 });
      },
    });
    await expect(missingOnRpc.describe()).rejects.toThrow("on every RPC response");
  });

  test("discovery rejects a non-2xx response even when it claims support", async () => {
    let posts = 0;
    const client = httpConnect("http://test", {
      fetch: async (_input, init) => {
        const request = new Request("http://test/__describe__", init);
        if (request.method === "OPTIONS") {
          return new Response(null, {
            status: 503,
            headers: { "VGI-Accept-Max-Response-Bytes-Support": "true" },
          });
        }
        posts += 1;
        return new Response();
      },
    });
    await expect(client.describe()).rejects.toThrow("HTTP 503");
    expect(posts).toBe(0);
  });

  test("rejects non-positive or unsafe client configuration", () => {
    for (const value of [0, 1, 65_535, -1, Number.MAX_SAFE_INTEGER + 1, 1.5]) {
      expect(() => httpConnect("http://test", { acceptedMaxResponseBytes: value })).toThrow();
    }
  });
});
