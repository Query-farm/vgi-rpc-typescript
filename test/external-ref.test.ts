// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Pre-published `ExternalRef` results: the value type, `publishExternal`, and
// the unary dispatchers (byte stream and HTTP) writing a ref's pointer as-is.

import { describe, expect, test } from "bun:test";
import { RecordBatchReader } from "@query-farm/apache-arrow";
import { batchFromColumns, deserializeBatches, singleRowBatch } from "../src/arrow/index.js";
import { buildRequestIpc } from "../src/client/ipc.js";
import { LOCATION_KEY, LOCATION_SHA256_KEY, LOG_LEVEL_KEY, SERVER_ID_KEY } from "../src/constants.js";
import {
  type ExternalLocationConfig,
  ExternalRef,
  type ExternalStorage,
  isExternalLocationBatch,
  isExternalRef,
  maybeExternalizeBatch,
  publishExternal,
  publishExternalResult,
  resolveExternalLocation,
} from "../src/external.js";
import { ARROW_CONTENT_TYPE } from "../src/http/common.js";
import { createHttpHandler, type DispatchInfo, Protocol, str } from "../src/index.js";
import { VgiRpcServer } from "../src/server.js";
import { zstdDecompress } from "../src/util/zstd.js";
import { buildEmptyBatch } from "../src/wire/response.js";

/** In-memory storage that counts uploads. */
class MockStorage implements ExternalStorage {
  objects = new Map<string, { data: Uint8Array; contentEncoding: string }>();
  uploads = 0;

  async upload(data: Uint8Array, contentEncoding: string): Promise<string> {
    this.uploads++;
    const url = `https://mock.storage/${this.uploads}`;
    this.objects.set(url, { data: new Uint8Array(data), contentEncoding });
    return url;
  }

  /** A `fetch` that serves the stored objects, for resolveExternalLocation. */
  fetch = (async (input: string | URL | Request) => {
    const obj = this.objects.get(String(input));
    if (!obj) return new Response(null, { status: 404 });
    const headers: Record<string, string> = {};
    if (obj.contentEncoding) headers["Content-Encoding"] = obj.contentEncoding;
    return new Response(obj.data, { status: 200, headers });
  }) as typeof globalThis.fetch;
}

/** Storage that must never be touched. */
class ForbiddenStorage implements ExternalStorage {
  async upload(): Promise<string> {
    throw new Error("a pre-published ref must not upload");
  }
}

async function sha256Hex(data: Uint8Array): Promise<string> {
  const hash = await crypto.subtle.digest("SHA-256", data);
  return Array.from(new Uint8Array(hash))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

const DIGEST = "a".repeat(64);

function makeProtocol(ref: () => ExternalRef): Protocol {
  const protocol = new Protocol("RefService");
  protocol.unary("catalog", {
    params: { name: str },
    result: { result: str },
    handler: () => ref(),
  });
  return protocol;
}

// ===========================================================================
// ExternalRef
// ===========================================================================

describe("ExternalRef", () => {
  test("holds url and optional digest", () => {
    const withDigest = new ExternalRef("https://x/1", DIGEST);
    expect(withDigest.url).toBe("https://x/1");
    expect(withDigest.sha256).toBe(DIGEST);
    const without = new ExternalRef("https://x/2");
    expect(without.sha256).toBeUndefined();
    expect(new ExternalRef("https://x/3", null).sha256).toBeUndefined();
  });

  test("rejects an empty url", () => {
    expect(() => new ExternalRef("")).toThrow("url must be non-empty");
  });

  test.each([
    ["too short", "abc"],
    ["uppercase", "A".repeat(64)],
    ["non-hex", "g".repeat(64)],
    ["too long", "a".repeat(65)],
    ["empty", ""],
  ])("rejects a malformed digest (%s)", (_label, digest) => {
    expect(() => new ExternalRef("https://x/1", digest)).toThrow("64 lowercase hex");
  });

  test("is immutable", () => {
    const ref = new ExternalRef("https://x/1", DIGEST);
    expect(() => {
      (ref as any).url = "https://evil/";
    }).toThrow();
    expect(ref.url).toBe("https://x/1");
  });

  test("isExternalRef recognises refs only", () => {
    expect(isExternalRef(new ExternalRef("https://x/1"))).toBe(true);
    expect(isExternalRef({ url: "https://x/1", sha256: DIGEST })).toBe(false);
    expect(isExternalRef({ result: "x" })).toBe(false);
    expect(isExternalRef(null)).toBe(false);
  });

  test("pointerBatch carries location, and the digest only when present", () => {
    const schema = makeProtocol(() => new ExternalRef("https://x"))
      .getMethods()
      .get("catalog")!.resultSchema;
    const withDigest = new ExternalRef("https://x/1", DIGEST).pointerBatch(schema);
    expect(withDigest.numRows).toBe(0);
    expect(isExternalLocationBatch(withDigest)).toBe(true);
    expect(withDigest.metadata?.get(LOCATION_KEY)).toBe("https://x/1");
    expect(withDigest.metadata?.get(LOCATION_SHA256_KEY)).toBe(DIGEST);
    const without = new ExternalRef("https://x/2").pointerBatch(schema);
    expect(without.metadata?.has(LOCATION_SHA256_KEY)).toBe(false);
  });
});

// ===========================================================================
// publishExternal
// ===========================================================================

describe("publishExternal", () => {
  const schema = makeProtocol(() => new ExternalRef("https://x"))
    .getMethods()
    .get("catalog")!.resultSchema;

  test("uploads once and returns a ref whose digest covers the raw IPC bytes", async () => {
    const storage = new MockStorage();
    const ref = await publishExternal(singleRowBatch(schema, { result: "hello" }), storage);
    expect(storage.uploads).toBe(1);
    const obj = storage.objects.get(ref.url)!;
    expect(obj.contentEncoding).toBe("");
    expect(ref.sha256).toBe(await sha256Hex(obj.data));
    const batches = deserializeBatches(obj.data);
    expect(batches.length).toBe(1);
    expect(batches[0].numRows).toBe(1);
    expect(batches[0].getChildAt(0)?.get(0)).toBe("hello");
  });

  test("serializes byte-for-byte like the per-call externalizer", async () => {
    const batch = singleRowBatch(schema, { result: "same bytes" });
    const published = new MockStorage();
    await publishExternal(batch, published);
    const perCall = new MockStorage();
    await maybeExternalizeBatch(batch, { storage: perCall, externalizeThresholdBytes: 1 });
    expect(published.objects.get("https://mock.storage/1")!.data).toEqual(
      perCall.objects.get("https://mock.storage/1")!.data,
    );
  });

  test("includeSha256: false omits the digest", async () => {
    const ref = await publishExternal(singleRowBatch(schema, { result: "x" }), new MockStorage(), {
      includeSha256: false,
    });
    expect(ref.sha256).toBeUndefined();
  });

  test("compresses with zstd; the digest is of the uncompressed bytes", async () => {
    const storage = new MockStorage();
    const ref = await publishExternal(singleRowBatch(schema, { result: "zz".repeat(500) }), storage, {
      compression: { algorithm: "zstd" },
    });
    const obj = storage.objects.get(ref.url)!;
    expect(obj.contentEncoding).toBe("zstd");
    const raw = new Uint8Array(await zstdDecompress(obj.data));
    expect(ref.sha256).toBe(await sha256Hex(raw));
  });

  test("requires exactly one row", async () => {
    const storage = new MockStorage();
    await expect(publishExternal(batchFromColumns(schema, { result: ["a", "b"] }), storage)).rejects.toThrow(
      "1-row result batch, got 2 rows",
    );
    await expect(publishExternal(buildEmptyBatch(schema), storage)).rejects.toThrow("1-row result batch, got 0 rows");
    expect(storage.uploads).toBe(0);
  });

  test("publishExternalResult builds the batch from values and resolves to them", async () => {
    const storage = new MockStorage();
    const ref = await publishExternalResult(schema, { result: "via values" }, storage);
    const pointer = ref.pointerBatch(schema);
    const config: ExternalLocationConfig = { storage, urlValidator: null, fetch: storage.fetch };
    const resolved = await resolveExternalLocation(pointer, config);
    expect(resolved.numRows).toBe(1);
    expect(resolved.getChildAt(0)?.get(0)).toBe("via values");
  });

  test("publishExternalResult refuses a missing required field without uploading", async () => {
    const storage = new MockStorage();
    await expect(publishExternalResult(schema, {}, storage)).rejects.toThrow("missing required field 'result'");
    expect(storage.uploads).toBe(0);
  });
});

// ===========================================================================
// Dispatch: byte-stream (pipe / unix / tcp) unary path
// ===========================================================================

async function callOverStream(
  protocol: Protocol,
  externalLocation?: ExternalLocationConfig,
): Promise<{ batch: any; info: DispatchInfo | undefined }> {
  const method = protocol.getMethod("catalog")!;
  const request = buildRequestIpc(method.paramsSchema as any, { name: "n" }, "catalog", { protocol: protocol.name });
  const readable = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(request);
      controller.close();
    },
  });
  const chunks: Uint8Array[] = [];
  let info: DispatchInfo | undefined;
  await new VgiRpcServer(protocol, {
    externalLocation,
    dispatchHook: {
      onDispatchStart: () => undefined,
      onDispatchEnd: (_t, i) => {
        info = i;
      },
    },
  }).serveConnection(readable, {
    write(bytes) {
      chunks.push(new Uint8Array(bytes));
    },
  });
  const total = chunks.reduce((n, c) => n + c.byteLength, 0);
  const body = new Uint8Array(total);
  let offset = 0;
  for (const c of chunks) {
    body.set(c, offset);
    offset += c.byteLength;
  }
  const reader = await RecordBatchReader.from(body);
  await reader.open();
  const batches = reader.readAll().filter((b) => !b.metadata.get(LOG_LEVEL_KEY));
  expect(batches.length).toBe(1);
  return { batch: batches[0], info };
}

describe("unary dispatch over a byte stream", () => {
  test("writes the ref's pointer with no storage configured", async () => {
    const { batch } = await callOverStream(makeProtocol(() => new ExternalRef("https://pub/1", DIGEST)));
    expect(batch.numRows).toBe(0);
    expect(batch.schema.fields.map((f: any) => f.name)).toEqual(["result"]);
    expect(batch.metadata.get(LOCATION_KEY)).toBe("https://pub/1");
    expect(batch.metadata.get(LOCATION_SHA256_KEY)).toBe(DIGEST);
  });

  test("omits the digest key for a ref without one", async () => {
    const { batch } = await callOverStream(makeProtocol(() => new ExternalRef("https://pub/2")));
    expect(batch.metadata.get(LOCATION_KEY)).toBe("https://pub/2");
    expect(batch.metadata.has(LOCATION_SHA256_KEY)).toBe(false);
  });

  test("never re-uploads or inlines, whatever the storage threshold", async () => {
    for (const threshold of [1, 1 << 30]) {
      const { batch } = await callOverStream(
        makeProtocol(() => new ExternalRef("https://pub/3", DIGEST)),
        {
          storage: new ForbiddenStorage(),
          externalizeThresholdBytes: threshold,
        },
      );
      expect(batch.numRows).toBe(0);
      expect(batch.metadata.get(LOCATION_KEY)).toBe("https://pub/3");
    }
  });

  test("an ordinary result still takes the normal path", async () => {
    const protocol = new Protocol("RefService");
    protocol.unary("catalog", { params: { name: str }, result: { result: str }, handler: (p) => ({ result: p.name }) });
    const { batch } = await callOverStream(protocol);
    expect(batch.numRows).toBe(1);
    expect(batch.metadata.has(LOCATION_KEY)).toBe(false);
  });
});

// ===========================================================================
// Dispatch: HTTP unary path
// ===========================================================================

async function callOverHttp(
  protocol: Protocol,
  options: Parameters<typeof createHttpHandler>[1] = {},
): Promise<{ status: number; batch: any; info: DispatchInfo | undefined }> {
  let info: DispatchInfo | undefined;
  const handler = createHttpHandler(protocol, {
    prefix: "/vgi",
    ...options,
    dispatchHook: {
      onDispatchStart: () => undefined,
      onDispatchEnd: (_t, i) => {
        info = i;
      },
    },
  });
  const method = protocol.getMethod("catalog")!;
  const response = await handler(
    new Request(`http://localhost/vgi/${protocol.name}/catalog`, {
      method: "POST",
      headers: { "Content-Type": ARROW_CONTENT_TYPE },
      body: buildRequestIpc(method.paramsSchema as any, { name: "n" }, "catalog", { protocol: protocol.name }),
    }),
  );
  const reader = await RecordBatchReader.from(new Uint8Array(await response.arrayBuffer()));
  await reader.open();
  const batches = reader.readAll().filter((b) => !b.metadata.get(LOG_LEVEL_KEY));
  expect(batches.length).toBe(1);
  return { status: response.status, batch: batches[0], info };
}

describe("unary dispatch over HTTP", () => {
  test("writes the ref's pointer with no storage configured", async () => {
    const { status, batch } = await callOverHttp(makeProtocol(() => new ExternalRef("https://pub/h1", DIGEST)));
    expect(status).toBe(200);
    expect(batch.numRows).toBe(0);
    expect(batch.metadata.get(LOCATION_KEY)).toBe("https://pub/h1");
    expect(batch.metadata.get(LOCATION_SHA256_KEY)).toBe(DIGEST);
    expect(batch.metadata.has(SERVER_ID_KEY)).toBe(false);
  });

  test("omits the digest key for a ref without one", async () => {
    const { batch } = await callOverHttp(makeProtocol(() => new ExternalRef("https://pub/h2")));
    expect(batch.metadata.has(LOCATION_SHA256_KEY)).toBe(false);
  });

  test("bypasses threshold and the externalized-bytes cap, and tallies nothing", async () => {
    const { status, batch, info } = await callOverHttp(
      makeProtocol(() => new ExternalRef("https://pub/h3", DIGEST)),
      {
        externalLocation: { storage: new ForbiddenStorage(), externalizeThresholdBytes: 1 << 30 },
        maxExternalizedResponseBytes: 1,
      },
    );
    expect(status).toBe(200);
    expect(batch.metadata.get(LOCATION_KEY)).toBe("https://pub/h3");
    expect(info?.externalizedBytes ?? 0).toBe(0);
  });

  test("a client resolves the published object end to end", async () => {
    const storage = new MockStorage();
    const protocol = new Protocol("RefService");
    let cached: Promise<ExternalRef> | undefined;
    let calls = 0;
    protocol.unary("catalog", {
      params: { name: str },
      result: { result: str },
      handler: () => {
        calls++;
        cached ??= publishExternalResult(protocol.getMethod("catalog")!.resultSchema, { result: "catalog!" }, storage);
        return cached;
      },
    });
    const first = await callOverHttp(protocol, { externalLocation: { storage } });
    const second = await callOverHttp(protocol, { externalLocation: { storage } });
    expect(calls).toBe(2);
    expect(storage.uploads).toBe(1);
    expect(second.batch.metadata.get(LOCATION_KEY)).toBe(first.batch.metadata.get(LOCATION_KEY));
    const resolved = await resolveExternalLocation(first.batch, { storage, urlValidator: null, fetch: storage.fetch });
    expect(resolved.getChildAt(0)?.get(0)).toBe("catalog!");
  });
});
