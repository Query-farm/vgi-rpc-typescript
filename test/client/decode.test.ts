// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Client-side response decoding — the counterpart to the server's
 * `X-VGI-Content-Encoding` stamping (see `test/http/handler.test.ts`).
 *
 * The asymmetry under test is the whole point of the module: which header the
 * codec is named in decides whether the fetch layer already undid the encoding.
 *
 *  - `Content-Encoding: gzip` — fetch decoded it before `arrayBuffer()`
 *    returned and may have left the header behind. Decoding again corrupts the
 *    body, so the client must NOT touch it.
 *  - `Content-Encoding: zstd` — some runtimes decode this transparently and
 *    some do not (Bun 1.3 does, and leaves the header behind), so the bytes
 *    decide: a body still carrying a zstd frame magic is the client's to undo,
 *    and one that is not has already been undone.
 *  - `X-VGI-Content-Encoding: <anything>` — nothing in the transport knows this
 *    name, so the body is still encoded and the client must always undo it.
 *
 * A worker on Cloudflare answers with the third form, which is why a TS client
 * that only ever looked at `Content-Encoding` fed gzip bytes to the Arrow
 * reader and failed with an unintelligible "expected to read N metadata bytes".
 */

import { describe, expect, test } from "bun:test";
import { decodeResponseBody, looksZstdEncoded, resolveResponseEncoding } from "../../src/client/decode.js";
import { RpcError } from "../../src/errors.js";
import { gzipCompress } from "../../src/util/gzip.js";
import { isZstdCompressAvailable, zstdCompress, zstdDecompress } from "../../src/util/zstd.js";

const PAYLOAD = new TextEncoder().encode("arrow-ipc-would-go-here".repeat(64));

const headersOf = (init: Record<string, string>): Headers => new Headers(init);

// ---------------------------------------------------------------------------
// resolveResponseEncoding
// ---------------------------------------------------------------------------

describe("looksZstdEncoded", () => {
  test.skipIf(!isZstdCompressAvailable())("recognises a real zstd frame", async () => {
    expect(looksZstdEncoded(await zstdCompress(PAYLOAD, 3))).toBe(true);
  });

  test("does not recognise a decoded Arrow stream, or a truncated magic", () => {
    // An Arrow IPC stream opens with the continuation marker, which is what a
    // transparently-decoded body looks like on this wire.
    expect(looksZstdEncoded(new Uint8Array([0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00]))).toBe(false);
    expect(looksZstdEncoded(new Uint8Array([0x28, 0xb5]))).toBe(false);
    expect(looksZstdEncoded(new Uint8Array())).toBe(false);
  });

  test("recognises a skippable frame", () => {
    // RFC 8878 §3.1.2: magic `5? 2A 4D 18`, little-endian.
    expect(looksZstdEncoded(new Uint8Array([0x5a, 0x2a, 0x4d, 0x18, 0, 0, 0, 0]))).toBe(true);
  });
});

describe("resolveResponseEncoding", () => {
  test("no encoding headers leaves the body alone", () => {
    expect(resolveResponseEncoding(headersOf({}))).toEqual({ codec: null, custom: false });
  });

  test("standard gzip is already undone by fetch, so nothing is left to do", () => {
    expect(resolveResponseEncoding(headersOf({ "Content-Encoding": "gzip" }))).toEqual({
      codec: null,
      custom: false,
    });
  });

  test("standard zstd survives fetch and is the client's to undo", () => {
    expect(resolveResponseEncoding(headersOf({ "Content-Encoding": "zstd" }))).toEqual({
      codec: "zstd",
      custom: false,
    });
  });

  test("the custom header is always the client's to undo", () => {
    expect(resolveResponseEncoding(headersOf({ "X-VGI-Content-Encoding": "gzip" }))).toEqual({
      codec: "gzip",
      custom: true,
    });
  });

  test("the custom header wins over the standard one", () => {
    const headers = headersOf({ "X-VGI-Content-Encoding": "gzip", "Content-Encoding": "zstd" });
    expect(resolveResponseEncoding(headers)).toEqual({ codec: "gzip", custom: true });
  });

  test("identity means plain, in either header", () => {
    expect(resolveResponseEncoding(headersOf({ "X-VGI-Content-Encoding": "identity" }))).toEqual({
      codec: null,
      custom: false,
    });
    expect(resolveResponseEncoding(headersOf({ "Content-Encoding": "identity" }))).toEqual({
      codec: null,
      custom: false,
    });
  });

  test("header values are matched case- and whitespace-insensitively", () => {
    expect(resolveResponseEncoding(headersOf({ "X-VGI-Content-Encoding": "  GZip " }))).toEqual({
      codec: "gzip",
      custom: true,
    });
    expect(resolveResponseEncoding(headersOf({ "Content-Encoding": "ZSTD" }))).toEqual({
      codec: "zstd",
      custom: false,
    });
  });
});

// ---------------------------------------------------------------------------
// decodeResponseBody
// ---------------------------------------------------------------------------

describe("decodeResponseBody", () => {
  test("gunzips a body stamped X-VGI-Content-Encoding — the Cloudflare path", async () => {
    const compressed = await gzipCompress(PAYLOAD);
    expect(compressed[0]).toBe(0x1f); // really gzip, not a pass-through
    expect(compressed[1]).toBe(0x8b);

    const out = await decodeResponseBody(headersOf({ "X-VGI-Content-Encoding": "gzip" }), compressed);
    expect(out).toEqual(PAYLOAD);
  });

  test("does NOT re-decode a standard Content-Encoding: gzip body", async () => {
    // fetch already decoded it; the header may linger. Touching it again would
    // throw (the bytes are not gzip) or, worse, corrupt a body that happened to
    // decode. The body must come back byte-identical.
    const out = await decodeResponseBody(headersOf({ "Content-Encoding": "gzip" }), PAYLOAD);
    expect(out).toEqual(PAYLOAD);
  });

  test("passes an unencoded body through untouched", async () => {
    expect(await decodeResponseBody(headersOf({}), PAYLOAD)).toEqual(PAYLOAD);
  });

  test("gzip needs no injected decompressor", async () => {
    // The Cloudflare path negotiates gzip precisely because workerd exposes no
    // zstd encoder; a client that never opted into compression has no
    // decompressFn, and must still be able to read that response.
    const compressed = await gzipCompress(PAYLOAD);
    const out = await decodeResponseBody(headersOf({ "X-VGI-Content-Encoding": "gzip" }), compressed, undefined);
    expect(out).toEqual(PAYLOAD);
  });

  test.skipIf(!isZstdCompressAvailable())("undoes zstd in either header", async () => {
    const compressed = await zstdCompress(PAYLOAD, 3);
    for (const header of ["Content-Encoding", "X-VGI-Content-Encoding"]) {
      const out = await decodeResponseBody(headersOf({ [header]: "zstd" }), compressed, zstdDecompress);
      expect(out).toEqual(PAYLOAD);
    }
  });

  test("names the problem when zstd arrives with no decoder available", async () => {
    // Under the custom header the body is always still encoded, whatever it
    // looks like: nothing in the transport knows that header's name.
    const attempt = decodeResponseBody(headersOf({ "X-VGI-Content-Encoding": "zstd" }), PAYLOAD, undefined);
    await expect(attempt).rejects.toThrow(RpcError);
    await expect(attempt).rejects.toThrow(/no zstd decoder/i);
  });

  test.skipIf(!isZstdCompressAvailable())(
    "names the missing decoder for a standard-header body that really is zstd",
    async () => {
      const compressed = await zstdCompress(PAYLOAD, 3);
      const attempt = decodeResponseBody(headersOf({ "Content-Encoding": "zstd" }), compressed, undefined);
      await expect(attempt).rejects.toThrow(RpcError);
      await expect(attempt).rejects.toThrow(/no zstd decoder/i);
    },
  );

  test("does NOT re-decode a standard Content-Encoding: zstd body the runtime already decoded", async () => {
    // Bun's fetch decodes `Content-Encoding: zstd` transparently and leaves
    // the header in place — even when the caller set `Accept-Encoding`
    // itself, so a client cannot opt out. Decoding it a second time threw
    // `InvalidZstdData` and made every zstd-compressing server unreachable
    // from this client on Bun.
    expect(await decodeResponseBody(headersOf({ "Content-Encoding": "zstd" }), PAYLOAD, zstdDecompress)).toEqual(
      PAYLOAD,
    );
  });

  test("names an unsupported codec instead of handing garbage to the Arrow reader", async () => {
    const attempt = decodeResponseBody(headersOf({ "X-VGI-Content-Encoding": "br" }), PAYLOAD);
    await expect(attempt).rejects.toThrow(RpcError);
    await expect(attempt).rejects.toThrow(/Unsupported response encoding 'br'/);
    await expect(attempt).rejects.toThrow(/X-VGI-Content-Encoding/);
  });
});

// ---------------------------------------------------------------------------
// End-to-end against a server that stamps the custom header, as workerd does.
// ---------------------------------------------------------------------------

describe("httpConnect against a custom-header server", () => {
  test("introspect and call both decode a gzip body under X-VGI-Content-Encoding", async () => {
    const { float, Protocol, createHttpHandler } = await import("../../src/index.js");
    const { httpConnect } = await import("../../src/client/connect.js");

    const protocol = new Protocol("decode.Test.v1");
    protocol.unary("double", {
      params: { x: float },
      result: { y: float },
      handler: async ({ x }) => ({ y: x * 2 }),
    });

    const handler = createHttpHandler(protocol, { serverId: "decode.Test.v1" });

    // Mimic the edge-safe workerd path exactly: the body is compressed once,
    // and the codec is named in the custom header only. Where the handler
    // already compressed, that means *relabelling* — re-compressing here would
    // double-encode and merely reproduce the bug under a different cause.
    // Content-Length is dropped so the runtime recomputes it for the body it
    // actually sends (a stale one truncates the response).
    const server = Bun.serve({
      port: 0,
      fetch: async (req) => {
        const resp = await handler(req);
        const body = new Uint8Array(await resp.arrayBuffer());
        const headers = new Headers(resp.headers);
        headers.delete("Content-Length");
        if (body.byteLength === 0) {
          return new Response(body, { status: resp.status, headers });
        }
        const already = headers.get("Content-Encoding");
        headers.delete("Content-Encoding");
        if (already) {
          headers.set("X-VGI-Content-Encoding", already);
          return new Response(body as unknown as BodyInit, { status: resp.status, headers });
        }
        headers.set("X-VGI-Content-Encoding", "gzip");
        return new Response((await gzipCompress(body)) as unknown as BodyInit, {
          status: resp.status,
          headers,
        });
      },
    });

    try {
      const client = httpConnect(`http://localhost:${server.port}`);
      // Exercises httpIntrospect (reflection) and then the unary call path,
      // both of which decode independently.
      const desc = await client.describe();
      expect(desc.methods.map((m) => m.name)).toContain("double");

      const result = await client.call("double", { x: 21 });
      expect(result?.y).toBe(42);
      client.close();
    } finally {
      server.stop(true);
    }
  });
});
