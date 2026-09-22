// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// The capability probe's METHOD is a compatibility constraint, not an
// implementation detail: it is the first request a client makes, and a browser
// preflights it (the probe carries VGI-Accept-Max-Response-Bytes, so it is not
// a simple request). A server answers that preflight with the methods its
// /health route implements — GET and HEAD — so an OPTIONS probe is refused
// before it is sent, and no browser client can connect at all.

import { describe, expect, test } from "bun:test";
import { discoverHttpCapabilities } from "../../src/client/capabilities.js";

/** A fetch that records the request and answers like a conforming server. */
function recordingFetch(): {
  calls: { url: string; method: string; headers: Headers }[];
  fetch: typeof globalThis.fetch;
} {
  const calls: { url: string; method: string; headers: Headers }[] = [];
  const fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push({
      url: String(input),
      method: init?.method ?? "GET",
      headers: new Headers(init?.headers),
    });
    return new Response(null, {
      status: 200,
      headers: {
        "VGI-Accept-Max-Response-Bytes-Support": "true",
        "VGI-Supported-Encodings": "zstd, gzip",
        "VGI-Externalization-Enabled": "false",
      },
    });
  }) as typeof globalThis.fetch;
  return { calls, fetch };
}

describe("discoverHttpCapabilities", () => {
  test("probes with HEAD, which a browser preflight permits", async () => {
    const { calls, fetch } = recordingFetch();
    await discoverHttpCapabilities("http://server.test", "", undefined, undefined, fetch);
    expect(calls).toHaveLength(1);
    expect(calls[0]!.method).toBe("HEAD");
    expect(calls[0]!.url).toBe("http://server.test/health");
  });

  test("probes the prefixed health endpoint", async () => {
    const { calls, fetch } = recordingFetch();
    await discoverHttpCapabilities("http://server.test", "/api", undefined, undefined, fetch);
    expect(calls[0]!.url).toBe("http://server.test/api/health");
  });

  test("advertises the accepted response budget and any authorization", async () => {
    const { calls, fetch } = recordingFetch();
    await discoverHttpCapabilities("http://server.test", "", "Bearer t0ken", 65_536, fetch);
    expect(calls[0]!.headers.get("VGI-Accept-Max-Response-Bytes")).toBe("65536");
    expect(calls[0]!.headers.get("Authorization")).toBe("Bearer t0ken");
  });

  test("reads capabilities from the headers of a bodyless reply", async () => {
    const { fetch } = recordingFetch();
    const caps = await discoverHttpCapabilities("http://server.test", "", undefined, undefined, fetch);
    expect(caps.acceptMaxResponseBytesSupport).toBe(true);
    expect(caps.supportedEncodings).toEqual(["zstd", "gzip"]);
  });

  test("a non-2xx probe is a transport error", async () => {
    const failing = (async () => new Response(null, { status: 503 })) as typeof globalThis.fetch;
    await expect(discoverHttpCapabilities("http://server.test", "", undefined, undefined, failing)).rejects.toThrow(
      /Capability discovery failed: HTTP 503/,
    );
  });
});
