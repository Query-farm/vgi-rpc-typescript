// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import { Field, Int64, RecordBatch, RecordBatchStreamWriter, Schema, vectorFromArray } from "@query-farm/apache-arrow";
import { LOCATION_KEY, LOCATION_SHA256_KEY, LOG_LEVEL_KEY } from "../src/constants.js";
import {
  type ExternalLocationConfig,
  type ExternalStorage,
  httpsOnlyValidator,
  isExternalLocationBatch,
  makeExternalLocationBatch,
  maybeExternalizeBatch,
  redactExternalUrl,
  resolveExternalLocation,
} from "../src/external.js";
import { zstdCompress } from "../src/util/zstd.js";
import { buildEmptyBatch } from "../src/wire/response.js";

const TEST_SCHEMA = new Schema([new Field("value", new Int64(), false)]);

/** In-memory mock storage. */
class MockStorage implements ExternalStorage {
  data = new Map<string, Uint8Array>();
  counter = 0;
  lastContentEncoding = "";

  async upload(data: Uint8Array, contentEncoding: string): Promise<string> {
    this.counter++;
    this.lastContentEncoding = contentEncoding;
    const url = `https://mock.storage/${this.counter}`;
    this.data.set(url, new Uint8Array(data));
    return url;
  }
}

/** Create a test batch with n rows of int64 values. */
function makeBatch(n: number): RecordBatch {
  const values = Array.from({ length: n }, (_, i) => BigInt(i));
  const col = vectorFromArray(values, new Int64());
  return new RecordBatch(TEST_SCHEMA, col.data[0]);
}

/** Compute SHA-256 hex of data. */
async function sha256Hex(data: Uint8Array): Promise<string> {
  const hash = await crypto.subtle.digest("SHA-256", data);
  return Array.from(new Uint8Array(hash))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

/** Serialize a batch to IPC bytes. */
function serializeIpc(batch: RecordBatch): Uint8Array {
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, batch.schema);
  writer.write(batch);
  writer.close();
  return writer.toUint8Array(true);
}

// ===========================================================================
// Detection tests
// ===========================================================================

describe("isExternalLocationBatch", () => {
  test("positive: zero-row batch with LOCATION_KEY", () => {
    const batch = makeExternalLocationBatch(TEST_SCHEMA, "https://mock/test");
    expect(isExternalLocationBatch(batch)).toBe(true);
  });

  test("negative: non-zero-row batch", () => {
    const batch = makeBatch(1);
    expect(isExternalLocationBatch(batch)).toBe(false);
  });

  test("negative: log batch with LOCATION_KEY", () => {
    const meta = new Map<string, string>();
    meta.set(LOCATION_KEY, "https://mock/test");
    meta.set(LOG_LEVEL_KEY, "INFO");
    const batch = buildEmptyBatch(TEST_SCHEMA, meta);
    expect(isExternalLocationBatch(batch)).toBe(false);
  });

  test("negative: zero-row batch without LOCATION_KEY", () => {
    const batch = buildEmptyBatch(TEST_SCHEMA);
    expect(isExternalLocationBatch(batch)).toBe(false);
  });
});

// ===========================================================================
// Creation tests
// ===========================================================================

describe("makeExternalLocationBatch", () => {
  test("creates zero-row batch", () => {
    const batch = makeExternalLocationBatch(TEST_SCHEMA, "https://mock/test");
    expect(batch.numRows).toBe(0);
  });

  test("has LOCATION_KEY metadata", () => {
    const batch = makeExternalLocationBatch(TEST_SCHEMA, "https://mock/test");
    expect(batch.metadata?.get(LOCATION_KEY)).toBe("https://mock/test");
  });

  test("includes SHA-256 when provided", () => {
    const batch = makeExternalLocationBatch(TEST_SCHEMA, "https://mock/test", "abc123");
    expect(batch.metadata?.get(LOCATION_SHA256_KEY)).toBe("abc123");
  });

  test("no SHA-256 when not provided", () => {
    const batch = makeExternalLocationBatch(TEST_SCHEMA, "https://mock/test");
    expect(batch.metadata?.has(LOCATION_SHA256_KEY)).toBe(false);
  });
});

// ===========================================================================
// Externalization tests
// ===========================================================================

describe("maybeExternalizeBatch", () => {
  test("above threshold → pointer batch with SHA-256", async () => {
    const storage = new MockStorage();
    const config: ExternalLocationConfig = { storage, externalizeThresholdBytes: 10 };
    const batch = makeBatch(100);

    const result = await maybeExternalizeBatch(batch, config);
    expect(result.numRows).toBe(0);
    expect(result.metadata?.has(LOCATION_KEY)).toBe(true);
    expect(result.metadata?.has(LOCATION_SHA256_KEY)).toBe(true);
    expect(result.metadata?.get(LOCATION_SHA256_KEY)?.length).toBe(64);
    expect(storage.data.size).toBe(1);
  });

  test("below threshold → pass through", async () => {
    const storage = new MockStorage();
    const config: ExternalLocationConfig = { storage, externalizeThresholdBytes: 10_000_000 };
    const batch = makeBatch(1);

    const result = await maybeExternalizeBatch(batch, config);
    expect(result.numRows).toBe(1);
    expect(storage.data.size).toBe(0);
  });

  test("no config → pass through", async () => {
    const batch = makeBatch(100);
    const result = await maybeExternalizeBatch(batch, null);
    expect(result.numRows).toBe(100);
  });

  test("zero-row batch → pass through", async () => {
    const storage = new MockStorage();
    const config: ExternalLocationConfig = { storage, externalizeThresholdBytes: 0 };
    const batch = buildEmptyBatch(TEST_SCHEMA);

    const result = await maybeExternalizeBatch(batch, config);
    expect(result.numRows).toBe(0);
    expect(storage.data.size).toBe(0);
  });
});

// ===========================================================================
// Resolution tests
// ===========================================================================

describe("resolveExternalLocation", () => {
  test("basic resolution", async () => {
    const dataBatch = makeBatch(5);
    const ipcBytes = serializeIpc(dataBatch);
    const checksum = await sha256Hex(ipcBytes);

    // Mock fetch
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async () =>
      new Response(ipcBytes, { headers: { "Content-Type": "application/octet-stream" } })) as typeof fetch;

    try {
      const pointer = makeExternalLocationBatch(TEST_SCHEMA, "https://mock.storage/1", checksum);
      const config: ExternalLocationConfig = { storage: new MockStorage(), urlValidator: null };
      const resolved = await resolveExternalLocation(pointer, config);
      expect(resolved.numRows).toBe(5);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("non-pointer passes through", async () => {
    const batch = makeBatch(5);
    const config: ExternalLocationConfig = { storage: new MockStorage(), urlValidator: null };
    const result = await resolveExternalLocation(batch, config);
    expect(result.numRows).toBe(5);
  });

  test("null config passes through", async () => {
    const pointer = makeExternalLocationBatch(TEST_SCHEMA, "https://mock/test");
    const result = await resolveExternalLocation(pointer, null);
    expect(result.numRows).toBe(0); // pointer unchanged
  });
});

// ===========================================================================
// URL validation tests
// ===========================================================================

describe("httpsOnlyValidator", () => {
  test("accepts HTTPS", () => {
    expect(() => httpsOnlyValidator("https://example.com/data")).not.toThrow();
  });

  test("rejects HTTP", () => {
    expect(() => httpsOnlyValidator("http://example.com/data")).toThrow("HTTPS");
  });

  test("rejects FTP", () => {
    expect(() => httpsOnlyValidator("ftp://example.com/data")).toThrow("HTTPS");
  });
});

describe("resolveExternalLocation URL validation", () => {
  test("default rejects HTTP", async () => {
    const pointer = makeExternalLocationBatch(TEST_SCHEMA, "http://insecure.com/data");
    const config: ExternalLocationConfig = { storage: new MockStorage() };
    await expect(resolveExternalLocation(pointer, config)).rejects.toThrow("HTTPS");
  });

  test("revalidates every manual redirect target", async () => {
    const originalFetch = globalThis.fetch;
    const visited: string[] = [];
    globalThis.fetch = (async (input: string | URL | Request) => {
      const target = String(input);
      visited.push(target);
      return new Response(null, { status: 302, headers: { Location: "http://internal.example/secret" } });
    }) as typeof fetch;
    try {
      const pointer = makeExternalLocationBatch(TEST_SCHEMA, "https://public.example/start");
      await expect(resolveExternalLocation(pointer, { storage: new MockStorage() })).rejects.toThrow("URL rejected");
      expect(visited).toEqual(["https://public.example/start"]);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("stops streaming when the compressed-byte cap is crossed", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async () => {
      const body = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new Uint8Array(4));
          controller.enqueue(new Uint8Array(5));
          controller.close();
        },
      });
      return new Response(body);
    }) as typeof fetch;
    try {
      const pointer = makeExternalLocationBatch(TEST_SCHEMA, "https://storage.example/data");
      await expect(resolveExternalLocation(pointer, { storage: new MockStorage(), maxFetchBytes: 8 })).rejects.toThrow(
        "max_fetch_bytes",
      );
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("enforces the decompressed-byte cap", async () => {
    const raw = serializeIpc(makeBatch(100));
    const compressed = await zstdCompress(raw, 1);
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async () =>
      new Response(compressed, { headers: { "Content-Encoding": "zstd" } })) as typeof fetch;
    try {
      const pointer = makeExternalLocationBatch(TEST_SCHEMA, "https://storage.example/data");
      await expect(
        resolveExternalLocation(pointer, {
          storage: new MockStorage(),
          maxFetchBytes: compressed.byteLength + 1,
          maxDecompressedBytes: raw.byteLength - 1,
        }),
      ).rejects.toThrow();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("does not disclose signed URL components in fetch failures", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async () => new Response(null, { status: 403, statusText: "Forbidden" })) as typeof fetch;
    const secret = "TOP-SECRET-SIGNATURE";
    try {
      const pointer = makeExternalLocationBatch(
        TEST_SCHEMA,
        `https://alice:password@storage.example/object?signature=${secret}#credential`,
      );
      const error = await resolveExternalLocation(pointer, { storage: new MockStorage() }).catch((e) => e as Error);
      expect(error.message).not.toContain(secret);
      expect(error.message).not.toContain("password");
      expect(error.message).not.toContain("credential");
      expect(error.message).toContain("https://storage.example/object");
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("redacts URL credentials, query strings, and fragments in diagnostics", () => {
    expect(redactExternalUrl("https://alice:secret@example.com/path?X-Amz-Signature=bearer#token")).toBe(
      "https://example.com/path",
    );
  });
});

// ===========================================================================
// SHA-256 checksum tests
// ===========================================================================

describe("SHA-256 checksums", () => {
  test("present on externalize", async () => {
    const storage = new MockStorage();
    const config: ExternalLocationConfig = { storage, externalizeThresholdBytes: 10 };
    const batch = makeBatch(100);

    const result = await maybeExternalizeBatch(batch, config);
    const sha = result.metadata?.get(LOCATION_SHA256_KEY);
    expect(sha).toBeDefined();
    expect(sha?.length).toBe(64);
  });

  test("matches raw IPC bytes", async () => {
    const storage = new MockStorage();
    const config: ExternalLocationConfig = { storage, externalizeThresholdBytes: 10 };
    const batch = makeBatch(100);

    const result = await maybeExternalizeBatch(batch, config);
    const sha = result.metadata!.get(LOCATION_SHA256_KEY)!;
    const url = result.metadata!.get(LOCATION_KEY)!;
    const uploaded = storage.data.get(url)!;
    const computed = await sha256Hex(uploaded);
    expect(sha).toBe(computed);
  });

  test("verified on fetch (correct data)", async () => {
    const dataBatch = makeBatch(10);
    const ipcBytes = serializeIpc(dataBatch);
    const checksum = await sha256Hex(ipcBytes);

    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async () => new Response(ipcBytes)) as typeof fetch;

    try {
      const pointer = makeExternalLocationBatch(TEST_SCHEMA, "https://mock.storage/ok", checksum);
      const config: ExternalLocationConfig = { storage: new MockStorage(), urlValidator: null };
      const resolved = await resolveExternalLocation(pointer, config);
      expect(resolved.numRows).toBe(10);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("mismatch raises error", async () => {
    const dataBatch = makeBatch(10);
    const ipcBytes = serializeIpc(dataBatch);
    const wrongSha = "0".repeat(64);

    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async () => new Response(ipcBytes)) as typeof fetch;

    try {
      const pointer = makeExternalLocationBatch(TEST_SCHEMA, "https://mock.storage/bad", wrongSha);
      const config: ExternalLocationConfig = { storage: new MockStorage(), urlValidator: null };
      await expect(resolveExternalLocation(pointer, config)).rejects.toThrow("SHA-256 checksum mismatch");
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("absent skips verification", async () => {
    const dataBatch = makeBatch(10);
    const ipcBytes = serializeIpc(dataBatch);

    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async () => new Response(ipcBytes)) as typeof fetch;

    try {
      // No SHA-256 on pointer
      const pointer = makeExternalLocationBatch(TEST_SCHEMA, "https://mock.storage/nosig");
      expect(pointer.metadata?.has(LOCATION_SHA256_KEY)).toBe(false);

      const config: ExternalLocationConfig = { storage: new MockStorage(), urlValidator: null };
      const resolved = await resolveExternalLocation(pointer, config);
      expect(resolved.numRows).toBe(10);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("full externalize → serve → resolve roundtrip", async () => {
    const storage = new MockStorage();
    const config: ExternalLocationConfig = { storage, externalizeThresholdBytes: 10, urlValidator: null };

    // Externalize
    const batch = makeBatch(50);
    const pointer = await maybeExternalizeBatch(batch, config);
    expect(pointer.numRows).toBe(0);

    // Mock fetch to serve from storage
    const url = pointer.metadata!.get(LOCATION_KEY)!;
    const storedData = storage.data.get(url)!;
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async () => new Response(storedData)) as typeof fetch;

    try {
      const resolved = await resolveExternalLocation(pointer, config);
      expect(resolved.numRows).toBe(50);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("mismatch on full roundtrip with tampered hash", async () => {
    const storage = new MockStorage();
    const config: ExternalLocationConfig = { storage, externalizeThresholdBytes: 10, urlValidator: null };

    const batch = makeBatch(50);
    const pointer = await maybeExternalizeBatch(batch, config);

    // Tamper: create new pointer with wrong SHA-256
    const url = pointer.metadata!.get(LOCATION_KEY)!;
    const storedData = storage.data.get(url)!;
    const tampered = makeExternalLocationBatch(TEST_SCHEMA, url, "0".repeat(64));

    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async () => new Response(storedData)) as typeof fetch;

    try {
      await expect(resolveExternalLocation(tampered, config)).rejects.toThrow("SHA-256 checksum mismatch");
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
});
