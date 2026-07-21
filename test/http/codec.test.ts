// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Response-codec negotiation — parity with the canonical Python
 * implementation (`_codec.parse_encoding_list` +
 * `_CompressionMiddleware._pick_response_encoding`).
 *
 * Two facts under test:
 *  - `X-VGI-Accept-Encoding` is consulted at all. A browser `fetch()` cannot
 *    set `Accept-Encoding` (forbidden header name), so the custom header is
 *    the only channel a WASM/browser client has; the response is then stamped
 *    `X-VGI-Content-Encoding` so the fetch layer doesn't double-decode.
 *  - The client's stated order wins. cpp-httplib (the DuckDB extension's HTTP
 *    client) injects `Accept-Encoding: deflate, gzip, br, zstd` — gzip first —
 *    while VGI states `X-VGI-Accept-Encoding: zstd, gzip`; zstd must win.
 */

import { describe, expect, test } from "bun:test";
import { RecordBatch, RecordBatchStreamWriter, recordBatchFromArrays, type Schema } from "@query-farm/apache-arrow";
import { REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY } from "../../src/constants.js";
import {
  COMPRESSION_ENCODINGS,
  DEFAULT_COMPRESSION_LEVEL,
  type Encoding,
  parseEncodingList,
  pickResponseEncoding,
} from "../../src/http/codec.js";
import { ARROW_CONTENT_TYPE, createHttpHandler, float, Protocol } from "../../src/index.js";
import { gzipDecompress } from "../../src/util/gzip.js";
import { isZstdCompressAvailable, zstdCompress, zstdDecompress } from "../../src/util/zstd.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const ALL: (e: Encoding) => boolean = () => true;
const GZIP_ONLY: (e: Encoding) => boolean = (e) => e === "gzip";
const NONE: (e: Encoding) => boolean = () => false;

function buildRequestIpc(schema: Schema, values: Record<string, any[]>, methodName: string): Uint8Array {
  const batch = recordBatchFromArrays(values, schema);
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

function makeProtocol(): Protocol {
  const protocol = new Protocol("CodecSvc");
  protocol.unary("add", {
    params: { a: float, b: float },
    result: { result: float },
    handler: async ({ a, b }) => ({ result: a + b }),
  });
  return protocol;
}

/** POST `add` with the supplied accept headers and return the raw Response. */
async function callAdd(
  handler: (r: Request) => Response | Promise<Response>,
  headers: Record<string, string>,
): Promise<Response> {
  const protocol = makeProtocol();
  const method = protocol.getMethods().get("add")!;
  const body = buildRequestIpc(method.paramsSchema as unknown as Schema, { a: [2], b: [3] }, "add");
  return await handler(
    new Request("http://localhost/add", {
      method: "POST",
      headers: { "Content-Type": ARROW_CONTENT_TYPE, ...headers },
      body: body as unknown as BodyInit,
    }),
  );
}

// ---------------------------------------------------------------------------
// parseEncodingList
// ---------------------------------------------------------------------------

describe("parseEncodingList", () => {
  test("empty / missing header parses to an empty list", () => {
    expect(parseEncodingList("")).toEqual([]);
    expect(parseEncodingList(null)).toEqual([]);
    expect(parseEncodingList(undefined)).toEqual([]);
  });

  test("preserves the client's order and lowercases", () => {
    expect(parseEncodingList("GZIP, ZSTD")).toEqual(["gzip", "zstd"]);
    expect(parseEncodingList("zstd, gzip")).toEqual(["zstd", "gzip"]);
  });

  test("skips unknown codecs", () => {
    expect(parseEncodingList("deflate, gzip, br, zstd")).toEqual(["gzip", "zstd"]);
    expect(parseEncodingList("br, deflate, *")).toEqual([]);
  });

  test("identity is a known token", () => {
    expect(parseEncodingList("identity")).toEqual(["identity"]);
    expect(parseEncodingList("br, IDENTITY, gzip")).toEqual(["identity", "gzip"]);
    expect(parseEncodingList("identity;q=0")).toEqual(["identity"]);
  });

  test("q-values are parsed off and ignored, not honoured", () => {
    // zstd has the *lower* q-value but comes first: order is positional.
    expect(parseEncodingList("zstd;q=0.5, gzip")).toEqual(["zstd", "gzip"]);
    expect(parseEncodingList("gzip;q=1.0, zstd;q=0.1")).toEqual(["gzip", "zstd"]);
  });

  test("de-duplicates keeping first occurrence", () => {
    expect(parseEncodingList("gzip, zstd, gzip")).toEqual(["gzip", "zstd"]);
  });
});

// ---------------------------------------------------------------------------
// pickResponseEncoding
// ---------------------------------------------------------------------------

describe("pickResponseEncoding", () => {
  test("custom header alone wins and flags usedCustom", () => {
    expect(pickResponseEncoding(null, "zstd, gzip", ALL)).toEqual({ codec: "zstd", usedCustom: true });
  });

  test("cpp-httplib case: standard lists gzip first, custom lists zstd first", () => {
    const picked = pickResponseEncoding("deflate, gzip, br, zstd", "zstd, gzip", ALL);
    // Custom list is walked first, so zstd wins over the standard header's
    // gzip-before-zstd ordering...
    expect(picked.codec).toBe("zstd");
    // ...but zstd is present in *both* lists, so the standard response header
    // is used.
    expect(picked.usedCustom).toBe(false);
  });

  test("standard header alone is honoured in the client's order", () => {
    expect(pickResponseEncoding("gzip, zstd", null, ALL)).toEqual({ codec: "gzip", usedCustom: false });
  });

  test("neither header yields no codec", () => {
    expect(pickResponseEncoding(null, null, ALL)).toEqual({ codec: null, usedCustom: false });
  });

  test("a codec we cannot produce is skipped; the next producible one wins", () => {
    expect(pickResponseEncoding(null, "zstd, gzip", GZIP_ONLY)).toEqual({ codec: "gzip", usedCustom: true });
  });

  test("only-unproducible offer yields no codec", () => {
    expect(pickResponseEncoding("zstd", null, GZIP_ONLY).codec).toBeNull();
    expect(pickResponseEncoding("gzip, zstd", "zstd", NONE).codec).toBeNull();
  });

  test("unknown-codec-only offer yields no codec", () => {
    expect(pickResponseEncoding("br, deflate", null, ALL).codec).toBeNull();
  });

  test("standard-only codec after a custom list still uses the standard header", () => {
    // custom offers zstd (unproducible here), standard adds gzip.
    const picked = pickResponseEncoding("gzip", "zstd", GZIP_ONLY);
    expect(picked).toEqual({ codec: "gzip", usedCustom: false });
  });

  // The worked-example table from the addendum, assuming zstd + gzip are both
  // producible.
  test("identity wins where it stands in the merged walk", () => {
    expect(pickResponseEncoding("gzip, zstd", "identity", ALL)).toEqual({ codec: null, usedCustom: false });
    expect(pickResponseEncoding(null, "identity, zstd", ALL)).toEqual({ codec: null, usedCustom: false });
    expect(pickResponseEncoding("identity, gzip", null, ALL)).toEqual({ codec: null, usedCustom: false });
  });

  test("a producible codec ahead of identity still wins", () => {
    expect(pickResponseEncoding(null, "zstd, identity", ALL)).toEqual({ codec: "zstd", usedCustom: true });
    expect(pickResponseEncoding("gzip, identity", null, ALL)).toEqual({ codec: "gzip", usedCustom: false });
  });

  test("identity is producible even when nothing else is", () => {
    expect(pickResponseEncoding("zstd, identity", null, NONE)).toEqual({ codec: null, usedCustom: false });
  });

  test("an unproducible codec before identity is skipped, not fatal", () => {
    // zstd unavailable → walk continues to identity → uncompressed.
    expect(pickResponseEncoding(null, "zstd, identity, gzip", GZIP_ONLY)).toEqual({ codec: null, usedCustom: false });
  });
});

// ---------------------------------------------------------------------------
// End-to-end through the HTTP handler
// ---------------------------------------------------------------------------

describe("HTTP handler response-codec negotiation", () => {
  test("compression is ON by default: no compressionLevel still compresses", async () => {
    const handler = createHttpHandler(makeProtocol());
    const resp = await callAdd(handler, { "X-VGI-Accept-Encoding": "zstd, gzip", "Accept-Encoding": "gzip, zstd" });
    expect(resp.status).toBe(200);
    // Custom header is walked first, so its head wins — zstd where the
    // runtime can encode it, gzip on workerd / Node <22.15. Both lists carry
    // the winner, so it lands on the standard response header.
    const expected = isZstdCompressAvailable() ? "zstd" : "gzip";
    expect(resp.headers.get("Content-Encoding")).toBe(expected);
    expect(resp.headers.get("X-VGI-Content-Encoding")).toBeNull();
    const raw = new Uint8Array(await resp.arrayBuffer());
    const plain = expected === "zstd" ? await zstdDecompress(raw) : await gzipDecompress(raw);
    expect(plain[0]).toBe(0xff); // Arrow IPC continuation marker
  });

  test("the default level is zstd 1", async () => {
    // Level 1 is a deliberate choice, not an arbitrary one: on Arrow IPC
    // bodies it measured 4.7x faster than level 3 *and* produced a smaller
    // result, and every VGI SDK standardises on it.
    expect(DEFAULT_COMPRESSION_LEVEL).toBe(1);
    if (!isZstdCompressAvailable()) return;
    const resp = await callAdd(createHttpHandler(makeProtocol()), { "Accept-Encoding": "zstd" });
    const raw = new Uint8Array(await resp.arrayBuffer());
    // Re-compressing the server's own plaintext at the claimed level must
    // reproduce the frame byte-for-byte. (Comparing two handlers' bodies
    // would not work — each mints a random serverId into response metadata.)
    const plain = await zstdDecompress(raw);
    expect(new Uint8Array(await zstdCompress(plain, DEFAULT_COMPRESSION_LEVEL))).toEqual(raw);
  });

  test("browser/WASM case: custom header alone → X-VGI-Content-Encoding", async () => {
    const handler = createHttpHandler(makeProtocol(), { compressionLevel: 3 });
    // No Accept-Encoding at all — a browser `fetch()` cannot set it.
    const resp = await callAdd(handler, { "X-VGI-Accept-Encoding": "zstd, gzip" });
    expect(resp.status).toBe(200);
    const expected = isZstdCompressAvailable() ? "zstd" : "gzip";
    expect(resp.headers.get("X-VGI-Content-Encoding")).toBe(expected);
    // Must NOT claim standard encoding — the fetch layer would double-decode.
    expect(resp.headers.get("Content-Encoding")).toBeNull();
    // And the body really is compressed with the codec we announced.
    const raw = new Uint8Array(await resp.arrayBuffer());
    const plain = expected === "zstd" ? await zstdDecompress(raw) : await gzipDecompress(raw);
    expect(plain.byteLength).toBeGreaterThan(0);
    expect(plain.byteLength).not.toBe(raw.byteLength);
  });

  test("custom header with gzip only → gzip on X-VGI-Content-Encoding", async () => {
    const handler = createHttpHandler(makeProtocol(), { compressionLevel: 3 });
    const resp = await callAdd(handler, { "X-VGI-Accept-Encoding": "gzip" });
    expect(resp.headers.get("X-VGI-Content-Encoding")).toBe("gzip");
    expect(resp.headers.get("Content-Encoding")).toBeNull();
    const plain = await gzipDecompress(new Uint8Array(await resp.arrayBuffer()));
    expect(plain.byteLength).toBeGreaterThan(0);
  });

  test("cpp-httplib regression: gzip-first standard + zstd-first custom → zstd on Content-Encoding", async () => {
    if (!isZstdCompressAvailable()) return; // no encoder on this runtime
    const handler = createHttpHandler(makeProtocol(), { compressionLevel: 3 });
    const resp = await callAdd(handler, {
      "Accept-Encoding": "deflate, gzip, br, zstd",
      "X-VGI-Accept-Encoding": "zstd, gzip",
    });
    expect(resp.headers.get("Content-Encoding")).toBe("zstd");
    // zstd was in both lists → standard header, not the VGI one.
    expect(resp.headers.get("X-VGI-Content-Encoding")).toBeNull();
    const plain = await zstdDecompress(new Uint8Array(await resp.arrayBuffer()));
    expect(plain.byteLength).toBeGreaterThan(0);
  });

  test("q-values do not reorder: `zstd;q=0.5, gzip` still picks zstd", async () => {
    if (!isZstdCompressAvailable()) return;
    const handler = createHttpHandler(makeProtocol(), { compressionLevel: 3 });
    const resp = await callAdd(handler, { "Accept-Encoding": "zstd;q=0.5, gzip" });
    expect(resp.headers.get("Content-Encoding")).toBe("zstd");
  });

  test("neither header → uncompressed, nothing stamped", async () => {
    const handler = createHttpHandler(makeProtocol(), { compressionLevel: 3 });
    const resp = await callAdd(handler, {});
    expect(resp.status).toBe(200);
    expect(resp.headers.get("Content-Encoding")).toBeNull();
    expect(resp.headers.get("X-VGI-Content-Encoding")).toBeNull();
  });

  test("client offers only codecs we cannot produce → uncompressed", async () => {
    const handler = createHttpHandler(makeProtocol(), { compressionLevel: 3 });
    const resp = await callAdd(handler, { "Accept-Encoding": "br, deflate", "X-VGI-Accept-Encoding": "br" });
    expect(resp.status).toBe(200);
    expect(resp.headers.get("Content-Encoding")).toBeNull();
    expect(resp.headers.get("X-VGI-Content-Encoding")).toBeNull();
  });

  test("existing standard-header clients are unaffected", async () => {
    // The TS client sends `Accept-Encoding: zstd` and no custom header.
    if (!isZstdCompressAvailable()) return;
    const handler = createHttpHandler(makeProtocol(), { compressionLevel: 3 });
    const resp = await callAdd(handler, { "Accept-Encoding": "zstd" });
    expect(resp.headers.get("Content-Encoding")).toBe("zstd");
    expect(resp.headers.get("X-VGI-Content-Encoding")).toBeNull();
  });

  test("explicit identity in the custom header → uncompressed, nothing stamped", async () => {
    const handler = createHttpHandler(makeProtocol(), { compressionLevel: 3 });
    const resp = await callAdd(handler, { "X-VGI-Accept-Encoding": "identity", "Accept-Encoding": "gzip, zstd" });
    expect(resp.status).toBe(200);
    expect(resp.headers.get("Content-Encoding")).toBeNull();
    expect(resp.headers.get("X-VGI-Content-Encoding")).toBeNull();
    // Body is a readable Arrow IPC stream, not a compressed frame.
    const raw = new Uint8Array(await resp.arrayBuffer());
    expect(raw[0]).toBe(0xff); // Arrow IPC continuation marker
  });

  test("explicit identity in the standard header → uncompressed", async () => {
    const handler = createHttpHandler(makeProtocol(), { compressionLevel: 3 });
    const resp = await callAdd(handler, { "Accept-Encoding": "identity, gzip" });
    expect(resp.headers.get("Content-Encoding")).toBeNull();
    expect(resp.headers.get("X-VGI-Content-Encoding")).toBeNull();
  });

  test("identity after a producible codec → the codec still wins", async () => {
    if (!isZstdCompressAvailable()) return;
    const handler = createHttpHandler(makeProtocol(), { compressionLevel: 3 });
    const resp = await callAdd(handler, { "X-VGI-Accept-Encoding": "zstd, identity" });
    expect(resp.headers.get("X-VGI-Content-Encoding")).toBe("zstd");
    expect(resp.headers.get("Content-Encoding")).toBeNull();
  });

  test("VGI-Supported-Encodings lists what the server speaks both ways", async () => {
    // No options at all — a stock server must advertise a non-empty set.
    const handler = createHttpHandler(makeProtocol());
    // zstd is always decodable (native or the fzstd fallback), so the
    // intersection is decided by whether this runtime can *encode* it:
    // `zstd, gzip` on Bun / Node ≥22.15 / Deno ≥2.6.9, `gzip` alone on
    // workerd and older Node. Derived from the runtime predicate, never a
    // hardcoded string.
    const expected = isZstdCompressAvailable() ? "zstd, gzip" : "gzip";
    for (const req of [
      new Request("http://localhost/health", { method: "OPTIONS" }),
      new Request("http://localhost/health", { method: "GET" }),
    ]) {
      const resp = await handler(req);
      expect(resp.headers.get("VGI-Supported-Encodings")).toBe(expected);
    }
    // identity is never advertised.
    const resp = await handler(new Request("http://localhost/health", { method: "OPTIONS" }));
    expect(resp.status).toBe(204);
    expect(resp.headers.get("VGI-Supported-Encodings")).not.toContain("identity");
  });

  test("the advertisement is derived from the runtime, not hardcoded", async () => {
    // Runtime-independent invariant: whatever the header claims must be
    // exactly what the server will actually emit. On Bun that means
    // `zstd, gzip`; on workerd / Node <22.15 (no zstd *encoder*, only the
    // fzstd decoder) it collapses to `gzip` — and this loop holds either way,
    // which is what proves the list comes from `isZstdCompressAvailable()`.
    const handler = createHttpHandler(makeProtocol());
    const opts = await handler(new Request("http://localhost/health", { method: "OPTIONS" }));
    const advertised = (opts.headers.get("VGI-Supported-Encodings") ?? "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    expect(advertised.length).toBeGreaterThan(0);

    for (const codec of advertised) {
      const r = await callAdd(handler, { "X-VGI-Accept-Encoding": codec });
      expect(r.headers.get("Content-Encoding") ?? r.headers.get("X-VGI-Content-Encoding")).toBe(codec);
    }
    for (const codec of COMPRESSION_ENCODINGS.filter((c) => !advertised.includes(c))) {
      const r = await callAdd(handler, { "X-VGI-Accept-Encoding": codec });
      expect(r.headers.get("Content-Encoding")).toBeNull();
      expect(r.headers.get("X-VGI-Content-Encoding")).toBeNull();
    }
    expect(advertised).toEqual(isZstdCompressAvailable() ? ["zstd", "gzip"] : ["gzip"]);
  });

  test("explicit compressionLevel: null → header present but empty, responses uncompressed", async () => {
    // The only way to reach the empty advertisement now that compression is
    // on by default. A shared cross-language conformance case pins this
    // state, so it has to stay reachable.
    const handler = createHttpHandler(makeProtocol(), { compressionLevel: null });
    const opts = await handler(new Request("http://localhost/health", { method: "OPTIONS" }));
    // Present-but-empty ≠ absent. An absent header means "legacy server".
    // NB: the header does go out on the wire as `VGI-Supported-Encodings: `
    // and Python/curl clients read it back as an empty string; Bun's *fetch
    // client* normalises empty header values away on read, so assert against
    // the handler's Response directly rather than through a live socket.
    expect(opts.headers.has("VGI-Supported-Encodings")).toBe(true);
    expect(opts.headers.get("VGI-Supported-Encodings")).toBe("");

    const resp = await callAdd(handler, { "Accept-Encoding": "zstd", "X-VGI-Accept-Encoding": "zstd" });
    expect(resp.status).toBe(200);
    expect(resp.headers.get("Content-Encoding")).toBeNull();
    expect(resp.headers.get("X-VGI-Content-Encoding")).toBeNull();
    const raw = new Uint8Array(await resp.arrayBuffer());
    expect(raw[0]).toBe(0xff); // Arrow IPC continuation marker, not a codec frame
  });

  test("VGI-Supported-Encodings is readable cross-origin", async () => {
    const handler = createHttpHandler(makeProtocol(), { compressionLevel: 3, corsOrigins: "*" });
    const resp = await handler(new Request("http://localhost/health", { method: "OPTIONS" }));
    expect(resp.headers.get("Access-Control-Expose-Headers")).toContain("VGI-Supported-Encodings");
  });

  test("custom header survives CORS preflight (Access-Control-Allow-Headers echo)", async () => {
    const handler = createHttpHandler(makeProtocol(), { compressionLevel: 3, corsOrigins: "*" });
    const resp = await handler(
      new Request("http://localhost/add", {
        method: "OPTIONS",
        headers: { "Access-Control-Request-Headers": "content-type, x-vgi-accept-encoding" },
      }),
    );
    expect(resp.headers.get("Access-Control-Allow-Headers")).toContain("x-vgi-accept-encoding");
    expect(resp.headers.get("Access-Control-Expose-Headers")).toContain("X-VGI-Content-Encoding");
  });
});
