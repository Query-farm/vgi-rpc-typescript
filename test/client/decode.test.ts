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
 *  - `Content-Encoding: zstd` — no runtime decodes zstd transparently, so the
 *    client must undo it.
 *  - `X-VGI-Content-Encoding: <anything>` — nothing in the transport knows this
 *    name, so the body is still encoded and the client must always undo it.
 *
 * A worker on Cloudflare answers with the third form, which is why a TS client
 * that only ever looked at `Content-Encoding` fed gzip bytes to the Arrow
 * reader and failed with an unintelligible "expected to read N metadata bytes".
 */

import { describe, expect, test } from "bun:test";
import { decodeResponseBody, resolveResponseEncoding } from "../../src/client/decode.js";
import { RpcError } from "../../src/errors.js";
import { gzipCompress } from "../../src/util/gzip.js";
import { isZstdCompressAvailable, zstdCompress, zstdDecompress } from "../../src/util/zstd.js";

const PAYLOAD = new TextEncoder().encode("arrow-ipc-would-go-here".repeat(64));

const headersOf = (init: Record<string, string>): Headers => new Headers(init);

// ---------------------------------------------------------------------------
// resolveResponseEncoding
// ---------------------------------------------------------------------------

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
    const attempt = decodeResponseBody(headersOf({ "Content-Encoding": "zstd" }), PAYLOAD, undefined);
    await expect(attempt).rejects.toThrow(RpcError);
    await expect(attempt).rejects.toThrow(/no zstd decoder/i);
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

    const protocol = new Protocol("decode-test");
    protocol.unary("double", {
      params: { x: float },
      result: { y: float },
      handler: async ({ x }) => ({ y: x * 2 }),
    });

    const handler = createHttpHandler(protocol, { serverId: "decode-test" });

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
      // Exercises httpIntrospect (__describe__) and then the unary call path,
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
