// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Upload-URL protocol v2 (`VGI-Upload-URL-Support: 2`), end to end and in
 * process, on whichever Arrow backend the run selected.
 *
 * - The server's `__upload_url__/init` takes `uploads: list<struct<size,
 *   sha256>>`, returns `upload_headers`, and refuses the retired `count`
 *   shape, bad entry counts, sizes and hashes, and sizes over
 *   `VGI-Max-Upload-Bytes` before minting anything.
 * - A request pointer must carry `vgi_rpc.location.sha256`, and the fetched
 *   bytes must match it.
 * - The client refuses a v1 server, hashes the body, PUTs exactly those bytes
 *   with the returned headers, and stamps the digest on the pointer.
 * - The fake store verifies like R2: a body whose length or SHA-256 differs
 *   from the signed values is refused (`BadDigest` / `IncompleteBody`).
 */

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { RecordBatchReader, type Schema } from "@query-farm/apache-arrow";
import { emptyBatchWithMetadata, field, int64, schema, utf8 } from "../src/arrow/index.js";
import { httpConnect } from "../src/client/index.js";
import { buildRequestIpc } from "../src/client/ipc.js";
import { externalizeRequestBody, requestUploadUrls } from "../src/client/uploadUrl.js";
import { LOCATION_KEY, LOCATION_SHA256_KEY } from "../src/constants.js";
import type { UploadUrl, UploadUrlProvider } from "../src/external.js";
import { sha256Hex } from "../src/external.js";
import { ARROW_CONTENT_TYPE, serializeIpcStream, UPLOAD_URL_PARAMS_SCHEMA } from "../src/http/common.js";
import { createHttpHandler } from "../src/http/handler.js";
import { Protocol } from "../src/protocol.js";
import { int, str } from "../src/schema.js";

// ---------------------------------------------------------------------------
// A tiny R2-like store: sized, checksummed pre-signed PUTs.
// ---------------------------------------------------------------------------

interface Grant {
  size: number;
  shaB64: string;
}

const store = new Map<string, Uint8Array>();
const grants = new Map<string, Grant>();
const storeStats = { puts: 0, refused: 0, gets: 0 };
let corruptNextStoredObject = false;

function b64(bytes: Uint8Array): string {
  let s = "";
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s);
}

function hexToBytes(hex: string): Uint8Array {
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return out;
}

function s3Error(code: string): Response {
  storeStats.refused++;
  return new Response(`<?xml version="1.0"?><Error><Code>${code}</Code></Error>`, {
    status: 400,
    headers: { "Content-Type": "application/xml" },
  });
}

let storeServer: ReturnType<typeof Bun.serve>;
let storeBase: string;

async function storeFetch(req: Request): Promise<Response> {
  const url = new URL(req.url);
  const id = url.pathname.slice("/obj/".length);
  if (req.method === "PUT") {
    const grant = grants.get(id);
    if (!grant) return new Response("no grant", { status: 403 });
    const body = new Uint8Array(await req.arrayBuffer());
    if (body.byteLength !== grant.size) return s3Error("IncompleteBody");
    const claimed = req.headers.get("x-amz-checksum-sha256");
    if (claimed !== grant.shaB64) return s3Error("BadDigest");
    const actual = b64(new Uint8Array(await crypto.subtle.digest("SHA-256", body)));
    if (actual !== grant.shaB64) return s3Error("BadDigest");
    storeStats.puts++;
    const kept = new Uint8Array(body);
    if (corruptNextStoredObject) {
      corruptNextStoredObject = false;
      kept[kept.length - 1] ^= 0xff;
    }
    store.set(id, kept);
    return new Response(null, { status: 200 });
  }
  if (req.method === "GET") {
    const data = store.get(id);
    if (!data) return new Response("missing", { status: 404 });
    storeStats.gets++;
    return new Response(data, { status: 200 });
  }
  return new Response("method", { status: 405 });
}

/** Provider recording what it was asked for. */
const minted: { size: number; sha256: string }[] = [];
const provider: UploadUrlProvider = {
  generateUploadUrl(size: number, sha256Hex: string): UploadUrl {
    minted.push({ size, sha256: sha256Hex });
    const id = crypto.randomUUID();
    const shaB64 = b64(hexToBytes(sha256Hex));
    grants.set(id, { size, shaB64 });
    return {
      uploadUrl: `${storeBase}/obj/${id}`,
      downloadUrl: `${storeBase}/obj/${id}`,
      expiresAt: new Date(Date.now() + 600_000),
      // Content-Length is stripped by the server before it reaches the client.
      headers: { "x-amz-checksum-sha256": shaB64, "Content-Length": String(size) },
    };
  },
};

// ---------------------------------------------------------------------------
// The RPC server
// ---------------------------------------------------------------------------

const protocol = new Protocol("UploadV2").unary("measure", {
  params: { payload: str },
  result: { length: int },
  handler: ({ payload }) => ({ length: payload.length }),
});

const MAX_REQUEST = 4096;
const MAX_UPLOAD = 1_000_000;

let rpcServer: ReturnType<typeof Bun.serve>;
let rpcBase: string;
let v1Server: ReturnType<typeof Bun.serve>;
let v1Base: string;

const handler = createHttpHandler(protocol, {
  maxRequestBytes: MAX_REQUEST,
  maxUploadBytes: MAX_UPLOAD,
  uploadUrlProvider: provider,
  externalLocation: {
    storage: { upload: async () => "" },
    externalizeThresholdBytes: Number.MAX_SAFE_INTEGER,
    urlValidator: null,
  },
});

beforeAll(() => {
  storeServer = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: storeFetch });
  storeBase = `http://127.0.0.1:${storeServer.port}`;
  rpcServer = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: handler });
  rpcBase = `http://127.0.0.1:${rpcServer.port}`;
  // A server still advertising the retired v1 (`true`): same handler, header rewritten.
  v1Server = Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    fetch: async (req) => {
      const resp = await handler(req);
      const headers = new Headers(resp.headers);
      if (headers.has("VGI-Upload-URL-Support")) headers.set("VGI-Upload-URL-Support", "true");
      return new Response(resp.body, { status: resp.status, headers });
    },
  });
  v1Base = `http://127.0.0.1:${v1Server.port}`;
});

afterAll(() => {
  storeServer?.stop(true);
  rpcServer?.stop(true);
  v1Server?.stop(true);
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Same server without a general request cap: 100 entries (~8 KiB) exceed
 *  the 4 KiB `maxRequestBytes` the main handler sets to force uploads. */
const uncappedHandler = createHttpHandler(protocol, { maxUploadBytes: MAX_UPLOAD, uploadUrlProvider: provider });

async function postInit(body: Uint8Array, h = handler): Promise<Response> {
  return h(
    new Request(`${rpcBase}/__upload_url__/init`, {
      method: "POST",
      headers: { "Content-Type": ARROW_CONTENT_TYPE },
      body,
    }),
  );
}

function uploadsBody(entries: { size: bigint | number; sha256: string }[]): Uint8Array {
  return buildRequestIpc(
    UPLOAD_URL_PARAMS_SCHEMA as unknown as Schema,
    { uploads: entries.map((e) => ({ size: BigInt(e.size), sha256: e.sha256 })) },
    "__upload_url__",
  );
}

async function errorMessage(resp: Response): Promise<string> {
  const reader = await RecordBatchReader.from(new Uint8Array(await resp.arrayBuffer()));
  await reader.open();
  for (const batch of reader.readAll()) {
    const m = batch.metadata?.get("vgi_rpc.log_message");
    if (m) return m;
  }
  return "";
}

const HEX_A = "a".repeat(64);
const PAYLOAD_SCHEMA_FACADE = schema([field("payload", utf8(), false)]);
const PAYLOAD_SCHEMA = PAYLOAD_SCHEMA_FACADE as unknown as Schema;

// ---------------------------------------------------------------------------
// Server: __upload_url__/init
// ---------------------------------------------------------------------------

describe("__upload_url__/init v2 (server)", () => {
  test("capability header value is 2", async () => {
    const resp = await handler(new Request(`${rpcBase}/health`, { method: "OPTIONS" }));
    expect(resp.headers.get("VGI-Upload-URL-Support")).toBe("2");
    expect(resp.headers.get("VGI-Max-Upload-Bytes")).toBe(String(MAX_UPLOAD));
  });

  test("round-trips one URL per entry, in order, with upload_headers", async () => {
    minted.length = 0;
    const entries = [
      { size: 10, sha256: HEX_A },
      { size: 20, sha256: "b".repeat(64) },
      { size: 30, sha256: "c".repeat(64) },
    ];
    const resp = await postInit(uploadsBody(entries));
    expect(resp.status).toBe(200);
    const reader = await RecordBatchReader.from(new Uint8Array(await resp.arrayBuffer()));
    await reader.open();
    expect(reader.schema.fields.map((f) => f.name)).toEqual([
      "upload_url",
      "download_url",
      "expires_at",
      "upload_headers",
    ]);
    expect(reader.schema.fields.every((f) => !f.nullable)).toBe(true);
    const rows: { headers: Record<string, string> }[] = [];
    for (const batch of reader.readAll()) {
      for (let r = 0; r < batch.numRows; r++) {
        rows.push({ headers: batch.getChild("upload_headers")!.get(r).toJSON() });
      }
    }
    expect(rows.length).toBe(3);
    expect(minted).toEqual(entries);
    for (let i = 0; i < 3; i++) {
      expect(rows[i]!.headers["x-amz-checksum-sha256"]).toBe(b64(hexToBytes(entries[i]!.sha256)));
      // Never Content-Length.
      expect(Object.keys(rows[i]!.headers).map((k) => k.toLowerCase())).not.toContain("content-length");
    }
  });

  test("refuses the retired count shape", async () => {
    minted.length = 0;
    const body = buildRequestIpc(
      schema([field("count", int64(), true)]) as unknown as Schema,
      { count: 1n },
      "__upload_url__",
    );
    const resp = await postInit(body);
    expect(resp.status).toBe(400);
    const message = await errorMessage(resp);
    expect(message).toContain("retired count shape");
    expect(message).toContain("VGI-Upload-URL-Support: 2");
    expect(minted.length).toBe(0);
  });

  test("refuses 0 and more than 100 entries", async () => {
    for (const n of [0, 101]) {
      const entries = Array.from({ length: n }, () => ({ size: 1, sha256: HEX_A }));
      const resp = await postInit(uploadsBody(entries), uncappedHandler);
      expect(resp.status).toBe(400);
      expect(await errorMessage(resp)).toContain("1 to 100 entries");
    }
    // 100 is fine, and fits the endpoint's own 32 KiB cap.
    const ok = await postInit(
      uploadsBody(Array.from({ length: 100 }, () => ({ size: 1, sha256: HEX_A }))),
      uncappedHandler,
    );
    expect(ok.status).toBe(200);
  });

  test("refuses size < 1", async () => {
    for (const size of [0, -1]) {
      const resp = await postInit(uploadsBody([{ size, sha256: HEX_A }]));
      expect(resp.status).toBe(400);
      expect(await errorMessage(resp)).toContain("size");
    }
  });

  test("refuses a sha256 that is not 64 lowercase hex", async () => {
    for (const sha256 of ["A".repeat(64), "a".repeat(63), "g".repeat(64), ""]) {
      const resp = await postInit(uploadsBody([{ size: 5, sha256 }]));
      expect(resp.status).toBe(400);
      expect(await errorMessage(resp)).toContain("sha256");
    }
  });

  test("refuses a size over VGI-Max-Upload-Bytes", async () => {
    minted.length = 0;
    const resp = await postInit(uploadsBody([{ size: MAX_UPLOAD + 1, sha256: HEX_A }]));
    expect(resp.status).toBe(400);
    expect(await errorMessage(resp)).toContain("exceeds VGI-Max-Upload-Bytes");
    expect(minted.length).toBe(0);
  });

  test("the request schema is uploads: list<struct<size int64 not null, sha256 utf8 not null>> not null", () => {
    const f = UPLOAD_URL_PARAMS_SCHEMA.fields[0]! as any;
    expect(f.name).toBe("uploads");
    expect(f.nullable).toBe(false);
    const entry = (f.type as any).children[0].type;
    const [size, sha] = entry.children;
    expect([size.name, size.nullable, sha.name, sha.nullable]).toEqual(["size", false, "sha256", false]);
    expect(size.type.bitWidth ?? 64).toBe(64);
  });
});

// ---------------------------------------------------------------------------
// Server: request pointers
// ---------------------------------------------------------------------------

describe("request pointer resolution (server)", () => {
  async function pointerCall(withSha: boolean, corrupt: boolean): Promise<Response> {
    const payload = "x".repeat(10_000);
    const inner = buildRequestIpc(PAYLOAD_SCHEMA, { payload }, "measure", {
      protocol: "UploadV2",
    });
    const sha = await sha256Hex(inner);
    const [pair] = await requestUploadUrls(rpcBase, "", [{ size: inner.byteLength, sha256: sha }]);
    corruptNextStoredObject = corrupt;
    const put = await fetch(pair!.uploadUrl, { method: "PUT", headers: pair!.uploadHeaders, body: inner });
    expect(put.status).toBe(200);
    // Hand-build the pointer so the sha can be left off.
    const meta = new Map<string, string>([
      ["vgi_rpc.method", "measure"],
      ["vgi_rpc.protocol", "UploadV2"],
      ["vgi_rpc.request_version", "1"],
      [LOCATION_KEY, pair!.downloadUrl],
    ]);
    if (withSha) meta.set(LOCATION_SHA256_KEY, sha);
    const pointer = serializeIpcStream(PAYLOAD_SCHEMA_FACADE, [emptyBatchWithMetadata(PAYLOAD_SCHEMA_FACADE, meta)]);
    return handler(
      new Request(`${rpcBase}/UploadV2/measure`, {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body: pointer,
      }),
    );
  }

  test("a pointer with the right sha256 dispatches", async () => {
    const resp = await pointerCall(true, false);
    expect(resp.status).toBe(200);
  });

  test("a pointer without vgi_rpc.location.sha256 is refused", async () => {
    // Delivered as the call's RPC error batch, like any other resolution failure.
    const resp = await pointerCall(false, false);
    expect(await errorMessage(resp)).toContain("missing vgi_rpc.location.sha256");
  });

  test("bytes that do not match the pointer's sha256 are refused", async () => {
    const resp = await pointerCall(true, true);
    expect(await errorMessage(resp)).toContain("sha256 mismatch");
  });
});

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

describe("client upload (v2)", () => {
  test("an oversized request uploads (size, sha256), PUTs with the headers, and round-trips", async () => {
    minted.length = 0;
    const before = { ...storeStats };
    const client = httpConnect(rpcBase, { protocol: "UploadV2" } as any);
    try {
      const payload = "y".repeat(50_000);
      const result = await client.call("measure", { payload });
      expect(result?.length).toBe(50_000);
    } finally {
      client.close();
    }
    expect(minted.length).toBe(1);
    expect(minted[0]!.size).toBeGreaterThan(MAX_REQUEST);
    expect(minted[0]!.sha256).toMatch(/^[0-9a-f]{64}$/);
    expect(storeStats.puts).toBe(before.puts + 1);
    expect(storeStats.gets).toBe(before.gets + 1);
    expect(storeStats.refused).toBe(before.refused);
  });

  test("a small request stays inline", async () => {
    minted.length = 0;
    const client = httpConnect(rpcBase, { protocol: "UploadV2" } as any);
    try {
      expect((await client.call("measure", { payload: "small" }))?.length).toBe(5);
    } finally {
      client.close();
    }
    expect(minted.length).toBe(0);
  });

  test("the pointer carries the body's sha256", async () => {
    const body = buildRequestIpc(PAYLOAD_SCHEMA, { payload: "z".repeat(9000) }, "measure");
    const pointer = await externalizeRequestBody(body, { baseUrl: rpcBase, prefix: "" });
    const reader = await RecordBatchReader.from(pointer);
    await reader.open();
    const [batch] = reader.readAll();
    expect(batch!.metadata.get(LOCATION_SHA256_KEY)).toBe(await sha256Hex(body));
    expect(batch!.metadata.get(LOCATION_KEY)).toContain(storeBase);
  });

  test("a v1 server is refused: upload protocol is too old", async () => {
    minted.length = 0;
    const client = httpConnect(v1Base, { protocol: "UploadV2" } as any);
    try {
      await expect(client.call("measure", { payload: "q".repeat(50_000) })).rejects.toThrow(
        "upload protocol is too old",
      );
    } finally {
      client.close();
    }
    expect(minted.length).toBe(0);
  });

  test("storage refuses a PUT whose bytes differ from the signed sha256", async () => {
    const body = new TextEncoder().encode("the real bytes");
    const sha = await sha256Hex(body);
    const [pair] = await requestUploadUrls(rpcBase, "", [{ size: body.byteLength, sha256: sha }]);
    const wrong = new TextEncoder().encode("the fake bytes");
    const put = await fetch(pair!.uploadUrl, { method: "PUT", headers: pair!.uploadHeaders, body: wrong });
    expect(put.status).toBe(400);
    expect(await put.text()).toContain("BadDigest");
    const short = await fetch(pair!.uploadUrl, { method: "PUT", headers: pair!.uploadHeaders, body: body.slice(1) });
    expect(short.status).toBe(400);
    expect(await short.text()).toContain("IncompleteBody");
  });

  test("malformed entries go to the server, whose refusal surfaces", async () => {
    await expect(requestUploadUrls(rpcBase, "", [{ size: 0, sha256: HEX_A }])).rejects.toThrow("size");
    await expect(requestUploadUrls(rpcBase, "", [{ size: 1, sha256: "XYZ" }])).rejects.toThrow("sha256");
    await expect(requestUploadUrls(rpcBase, "", [])).rejects.toThrow("1 to 100");
  });
});
