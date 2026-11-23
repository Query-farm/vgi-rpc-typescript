// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Blob-vendor client (`src/vendor.ts`).
 *
 * The first groups need no network. The rest run against the reference fake
 * vendor (`vgi_rpc.conformance.fake_vendor`, vgi-rpc-python >= 0.56), started
 * here with `--test-routes`; they skip, loudly, when the reference is not
 * importable. Set `VGI_VENDOR_TESTS_REQUIRED=1` (CI does) to fail instead.
 */

import { afterAll, beforeAll, beforeEach, describe, expect, test } from "bun:test";
import type { Subprocess } from "bun";
import { batchFromColumns, field, int64, schema } from "../src/arrow/index.js";
import { LOCATION_KEY, LOCATION_SHA256_KEY } from "../src/constants.js";
import { maybeExternalizeBatch, ObjectMissingError, resolveExternalLocation } from "../src/external.js";
import {
  CLIENT_UPLOAD_ORIGIN,
  externalConfigFromEnv,
  maxUploadBytesFromEnv,
  pinnedHostsValidator,
  VendorBlobClient,
  VendorError,
  VendorStorage,
} from "../src/vendor.js";
import { PYTHON_BIN } from "./reference.js";

const TOKEN = "test-token";
const SCHEMA = schema([field("v", int64())]);

// ---------------------------------------------------------------------------
// Pure
// ---------------------------------------------------------------------------

describe("pinnedHostsValidator", () => {
  test("accepts exact host on the default https port", () => {
    const v = pinnedHostsValidator(["storage.example.com"]);
    v("https://storage.example.com/bucket/key?X-Amz-Signature=abc");
    expect(() => v("https://storage.example.com:8443/k")).toThrow(/not a pinned storage host/);
    expect(() => v("https://evil.storage.example.com/k")).toThrow(/not a pinned storage host/);
    expect(() => v("http://storage.example.com/k")).toThrow(/scheme 'http' not allowed/);
  });

  test("an explicit port matches only that port", () => {
    const v = pinnedHostsValidator(["127.0.0.1:9000"], { allowHttp: true });
    v("http://127.0.0.1:9000/r2/x");
    expect(() => v("http://127.0.0.1:9001/r2/x")).toThrow(/not a pinned storage host/);
    expect(() => v("http://127.0.0.1/r2/x")).toThrow(/not a pinned storage host/);
  });

  test("refuses userinfo, malformed URLs and paths outside the prefixes", () => {
    const v = pinnedHostsValidator(["h.example"], { prefixes: ["https://h.example/r2/"] });
    v("https://h.example/r2/a/b");
    expect(() => v("https://u:p@h.example/r2/a")).toThrow(/userinfo/);
    expect(() => v("not a url")).toThrow(/malformed/);
    expect(() => v("https://h.example/other/a")).toThrow(/outside the pinned storage prefixes/);
    expect(() => v("https://h.example/r2/../other/a")).toThrow(/outside the pinned storage prefixes/);
    expect(() => v("https://h.example/r2/%2e%2e/other/a")).toThrow(/outside the pinned storage prefixes/);
  });

  test("diagnostics never carry the signed query", () => {
    const v = pinnedHostsValidator(["h.example"]);
    try {
      v("https://other.example/k?X-Amz-Signature=SECRETSIG");
      throw new Error("expected a refusal");
    } catch (error) {
      expect((error as Error).message).not.toContain("SECRETSIG");
    }
  });

  test("refuses bad entries and an empty list", () => {
    expect(() => pinnedHostsValidator([])).toThrow(/at least one host/);
    expect(() => pinnedHostsValidator(["h.example/path"])).toThrow(/Invalid pinned host/);
    expect(() => pinnedHostsValidator(["u@h.example"])).toThrow(/Invalid pinned host/);
  });

  test("IPv6 entries", () => {
    const v = pinnedHostsValidator(["[::1]:9000"], { allowHttp: true });
    v("http://[::1]:9000/x");
    expect(() => v("http://[::2]:9000/x")).toThrow(/not a pinned storage host/);
  });
});

describe("vendor client construction", () => {
  test("http only for loopback, and a token is required", () => {
    expect(() => new VendorBlobClient("http://vendor.example.com", TOKEN)).toThrow(/https/);
    expect(() => new VendorBlobClient("ftp://vendor.example.com", TOKEN)).toThrow(/https/);
    expect(() => new VendorBlobClient("https://vendor.example.com", "")).toThrow(/token is empty/);
    new VendorBlobClient("http://127.0.0.1:1", TOKEN);
    new VendorBlobClient("http://localhost:1", TOKEN);
    new VendorBlobClient("http://[::1]:1", TOKEN);
  });

  test("externalConfigFromEnv", () => {
    expect(externalConfigFromEnv({})).toBeNull();
    expect(() => externalConfigFromEnv({ VGI_BLOB_VENDOR_URL: "https://v.example" })).toThrow(/VGI_BLOB_VENDOR_TOKEN/);
    const fallback = externalConfigFromEnv({ VGI_BLOB_VENDOR_URL: "https://v.example", VGI_CF_DO_TOKEN: "t" });
    expect(fallback?.externalizeThresholdBytes).toBe(1_048_576);
    expect(fallback?.compression).toBeUndefined();
    const cfg = externalConfigFromEnv({
      VGI_BLOB_VENDOR_URL: "https://v.example",
      VGI_BLOB_VENDOR_TOKEN: "t",
      VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES: "4096",
      VGI_RPC_EXTERNAL_COMPRESSION: "zstd:5",
    });
    expect(cfg?.storage).toBeInstanceOf(VendorStorage);
    expect(cfg?.externalizeThresholdBytes).toBe(4096);
    expect(cfg?.compression).toEqual({ algorithm: "zstd", level: 5 });
    expect(typeof cfg?.urlValidator).toBe("function");
    const base = { VGI_BLOB_VENDOR_URL: "https://v.example", VGI_BLOB_VENDOR_TOKEN: "t" };
    expect(() => externalConfigFromEnv({ ...base, VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES: "0" })).toThrow(/positive/);
    expect(() => externalConfigFromEnv({ ...base, VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES: "1k" })).toThrow(/integer/);
    expect(() => externalConfigFromEnv({ ...base, VGI_RPC_EXTERNAL_COMPRESSION: "brotli" })).toThrow(/none or zstd/);
  });

  test("generateUploadUrl marks its grant request origin=client_upload", async () => {
    const bodies: Record<string, unknown>[] = [];
    const fakeFetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
      expect(String(input)).toBe("https://v.example/v1/blobs/put");
      bodies.push(JSON.parse(String(init?.body)));
      return new Response(
        JSON.stringify({
          object_id: "o1",
          upload_url: "https://storage.example/o1?put",
          upload_method: "PUT",
          required_headers: { "content-length": "3", "x-amz-checksum-sha256": "c2hh" },
          download_url: "https://storage.example/o1?get",
          expires_at: new Date(Date.now() + 60_000).toISOString(),
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    }) as typeof globalThis.fetch;
    const storage = new VendorStorage("https://v.example", TOKEN, { fetch: fakeFetch, pinHosts: false });
    const pair = await storage.generateUploadUrl(3, "ab".repeat(32));
    expect(CLIENT_UPLOAD_ORIGIN).toBe("client_upload");
    expect(bodies).toHaveLength(1);
    expect(bodies[0]).toMatchObject({ size: 3, purpose: "rpc", origin: "client_upload" });
    expect(pair.headers).toEqual({ "x-amz-checksum-sha256": "c2hh" });
  });

  test("maxUploadBytesFromEnv", () => {
    expect(maxUploadBytesFromEnv({})).toBeNull();
    expect(maxUploadBytesFromEnv({ VGI_RPC_MAX_UPLOAD_BYTES: "268435456" })).toBe(268435456);
    expect(() => maxUploadBytesFromEnv({ VGI_RPC_MAX_UPLOAD_BYTES: "-1" })).toThrow(/positive/);
  });
});

// ---------------------------------------------------------------------------
// Against the reference fake vendor
// ---------------------------------------------------------------------------

const required = process.env.VGI_VENDOR_TESTS_REQUIRED === "1";
let hasFake = false;
try {
  hasFake = Bun.spawnSync([PYTHON_BIN, "-c", "import vgi_rpc.conformance.fake_vendor"]).exitCode === 0;
} catch {}
if (!hasFake) {
  const why = `vgi_rpc.conformance.fake_vendor is not importable with ${PYTHON_BIN}`;
  if (required) throw new Error(`VGI_VENDOR_TESTS_REQUIRED=1 but ${why}`);
  console.warn(`SKIP vendor tests against the fake vendor: ${why}`);
}

let proc: Subprocess | undefined;
let base = "";

async function readUrl(p: Subprocess): Promise<string> {
  const reader = (p.stdout as ReadableStream<Uint8Array>).getReader();
  const decoder = new TextDecoder();
  let text = "";
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    const { done, value } = await reader.read();
    if (done) break;
    text += decoder.decode(value);
    const match = /URL:(\S+)/.exec(text);
    if (match) {
      reader.releaseLock();
      return match[1]!;
    }
  }
  throw new Error(`fake vendor did not report a URL: ${text}`);
}

async function stats(): Promise<Record<string, number>> {
  return JSON.parse(await (await fetch(`${base}/__stats__`)).text());
}

async function admin(path: string): Promise<void> {
  const resp = await fetch(`${base}/__admin__/${path}`);
  expect(resp.status).toBe(200);
  await resp.text();
}

function bytes(n: number, seed = 1): Uint8Array {
  const out = new Uint8Array(n);
  for (let i = 0; i < n; i++) out[i] = (i * 31 + seed) & 0xff;
  return out;
}

describe.skipIf(!hasFake)("against the fake vendor", () => {
  beforeAll(async () => {
    proc = Bun.spawn(
      [
        PYTHON_BIN,
        "-c",
        "from vgi_rpc.conformance.fake_vendor import main; main()",
        "--test-routes",
        "--token",
        `${TOKEN}=tenant-a`,
      ],
      { stdout: "pipe", stderr: "inherit" },
    );
    base = await readUrl(proc);
  });

  afterAll(() => {
    proc?.kill();
  });

  beforeEach(async () => {
    await admin("reset");
  });

  test("VendorBlobClient round-trips bytes and counts one put and one get", async () => {
    const client = new VendorBlobClient(base, TOKEN);
    const before = await stats();
    const data = bytes(5000);
    const ref = await client.putBytes(data, { scope: { shard: "att-ab", executionId: "s.00" } });
    expect(ref.size).toBe(5000);
    expect(ref.sha256).toMatch(/^[0-9a-f]{64}$/);
    expect(ref.objectId).toMatch(/^[0-9a-f]{32}$/);
    const back = await client.getBytes(ref);
    expect(back).toEqual(data);
    const after = await stats();
    expect(after.puts! - before.puts!).toBe(1);
    expect(after.gets! - before.gets!).toBe(1);
    expect(after.storage_puts! - before.storage_puts!).toBe(1);
    expect(after.bytes_uploaded! - before.bytes_uploaded!).toBe(5000);
  });

  test("an empty blob round-trips", async () => {
    const client = new VendorBlobClient(base, TOKEN);
    const ref = await client.putBytes(new Uint8Array());
    expect(await client.getBytes(ref)).toEqual(new Uint8Array());
  });

  test("idempotency: same key and bytes reuse the object; other bytes conflict", async () => {
    const client = new VendorBlobClient(base, TOKEN);
    const data = bytes(100, 7);
    const a = await client.putBytes(data, { idempotencyKey: "abc.0" });
    const b = await client.putBytes(data, { idempotencyKey: "abc.0" });
    expect(b.objectId).toBe(a.objectId);
    const err = await client.putBytes(bytes(100, 8), { idempotencyKey: "abc.0" }).catch((e) => e);
    expect(err).toBeInstanceOf(VendorError);
    expect(err.status).toBe(409);
    expect(err.code).toBe("idempotency_conflict");
  });

  test("a corrupted download is a sha256_mismatch against the ref", async () => {
    const client = new VendorBlobClient(base, TOKEN);
    const ref = await client.putBytes(bytes(2048, 3));
    await admin("fault?purpose=fs&action=corrupt&count=1");
    const err = await client.getBytes(ref).catch((e) => e);
    expect(err).toBeInstanceOf(VendorError);
    expect(err.code).toBe("sha256_mismatch");
    await admin("reset");
    expect(await client.getBytes(ref)).toEqual(bytes(2048, 3));
  });

  test("a ref whose size disagrees is a size_mismatch", async () => {
    const client = new VendorBlobClient(base, TOKEN);
    const ref = await client.putBytes(bytes(64));
    const err = await client.getBytes({ ...ref, size: 65 }).catch((e) => e);
    expect(err.code).toBe("size_mismatch");
  });

  test("a ref whose sha256 disagrees is a sha256_mismatch", async () => {
    const client = new VendorBlobClient(base, TOKEN);
    const ref = await client.putBytes(bytes(64));
    const err = await client.getBytes({ ...ref, sha256: "0".repeat(64) }).catch((e) => e);
    expect(err.code).toBe("sha256_mismatch");
  });

  test("a missing download is a 404; an unknown object is a vendor 404", async () => {
    const client = new VendorBlobClient(base, TOKEN);
    const ref = await client.putBytes(bytes(300));
    await admin("fault?purpose=fs&action=missing&count=1");
    const err = await client.getBytes(ref).catch((e) => e);
    expect(err).toBeInstanceOf(VendorError);
    expect(err.status).toBe(404);
    const unknown = await client
      .getBytes({ objectId: "f".repeat(32), size: 1, sha256: "0".repeat(64) })
      .catch((e) => e);
    expect(unknown.status).toBe(404);
    expect(["not_found", "not_uploaded"]).toContain(unknown.code);
  });

  test("delete and deleteScope", async () => {
    const client = new VendorBlobClient(base, TOKEN);
    const a = await client.putBytes(bytes(10), { scope: { shard: "loc-anon", executionId: "q.01" } });
    const b = await client.putBytes(bytes(11), { scope: { shard: "loc-anon", executionId: "q.01" } });
    const c = await client.putBytes(bytes(12));
    expect(await client.deleteScope("loc-anon", "q.01")).toBe(2);
    expect((await client.getBytes(a).catch((e) => e)).status).toBe(404);
    expect((await client.getBytes(b).catch((e) => e)).status).toBe(404);
    expect(await client.delete([c, "f".repeat(32)])).toBe(1);
  });

  test("a wrong token is a 401 and the token never appears in the error", async () => {
    const client = new VendorBlobClient(base, "WRONG-SECRET-TOKEN");
    const err = await client.putBytes(bytes(10)).catch((e) => e);
    expect(err).toBeInstanceOf(VendorError);
    expect(err.status).toBe(401);
    expect(String(err.message)).not.toContain("WRONG-SECRET-TOKEN");
  });

  test("urlValidator pins to the vendor's own hosts", async () => {
    const client = new VendorBlobClient(base, TOKEN);
    await client.urlValidator(`${base}/r2/anything`);
    await expect(Promise.resolve(client.urlValidator("https://elsewhere.example/r2/x"))).rejects.toThrow();
    await expect(Promise.resolve(client.urlValidator(`${base}/not-r2/x`))).rejects.toThrow(/prefixes/);
    const info = await client.info();
    expect(info.tenant).toBe("tenant-a");
    expect(info.uploadHosts.length).toBeGreaterThan(0);
  });

  test("VendorStorage.upload returns a pinned rpc download URL holding the bytes", async () => {
    const storage = new VendorStorage(base, TOKEN);
    const data = bytes(9000, 9);
    const before = await stats();
    const url = await storage.upload(data, "");
    await storage.urlValidator(url);
    const back = new Uint8Array(await (await fetch(url)).arrayBuffer());
    expect(back).toEqual(data);
    // A server write is not marked as a client upload.
    const after = await stats();
    expect(after.puts_rpc! - before.puts_rpc!).toBe(1);
    expect(after.puts_upload! - before.puts_upload!).toBe(0);
    expect(after.client_uploads! - before.client_uploads!).toBe(0);
  });

  test("VendorStorage.generateUploadUrl vends a sized PUT/GET pair the store verifies", async () => {
    const storage = new VendorStorage(base, TOKEN);
    const before = await stats();
    const data = bytes(1234, 5);
    const sha = Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", data)))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
    const pair = await storage.generateUploadUrl(data.byteLength, sha);
    expect(pair.expiresAt).toBeInstanceOf(Date);
    // The signed checksum header comes back for the client; Content-Length never does.
    expect(Object.keys(pair.headers ?? {}).map((k) => k.toLowerCase())).toContain("x-amz-checksum-sha256");
    expect(Object.keys(pair.headers ?? {}).map((k) => k.toLowerCase())).not.toContain("content-length");

    // Wrong bytes of the right length: BadDigest, nothing stored.
    const wrong = bytes(1234, 6);
    const bad = await fetch(pair.uploadUrl, { method: "PUT", headers: pair.headers, body: wrong });
    expect(bad.status).toBe(400);
    expect(await bad.text()).toContain("BadDigest");
    // Wrong length: IncompleteBody.
    const short = await fetch(pair.uploadUrl, { method: "PUT", headers: pair.headers, body: data.slice(1) });
    expect(short.status).toBe(400);
    expect(await short.text()).toContain("IncompleteBody");

    const put = await fetch(pair.uploadUrl, { method: "PUT", headers: pair.headers, body: data });
    expect(put.status).toBeLessThan(300);
    const back = new Uint8Array(await (await fetch(pair.downloadUrl)).arrayBuffer());
    expect(back).toEqual(data);
    const after = await stats();
    // The sized /v1/blobs/put path, never the retired unsized /v1/upload_urls.
    expect(after.puts! - before.puts!).toBe(1);
    expect(after.upload_urls! - before.upload_urls!).toBe(0);
    expect(after.upload_refusals! - before.upload_refusals!).toBe(2);
    // The grant was marked origin=client_upload (fake vendor >= vgi-rpc-python f0fb675).
    expect(after.puts_upload! - before.puts_upload!).toBe(1);
    expect(after.puts_rpc! - before.puts_rpc!).toBe(0);
    expect(after.client_uploads! - before.client_uploads!).toBe(1);
    expect(after.client_upload_bytes! - before.client_upload_bytes!).toBe(data.byteLength);
    expect(after.client_upload_refusals! - before.client_upload_refusals!).toBe(2);
  });

  test("VendorStorage object API: putObject marks origin=state_ref, get fetches by id, a deleted object is missing", async () => {
    const storage = new VendorStorage(base, TOKEN);
    const before = await stats();
    const data = bytes(5000, 3);
    const objectId = await storage.putObject(data);
    expect(objectId.length).toBeGreaterThan(0);
    expect(await storage.get(objectId)).toEqual(data);
    await expect(storage.get(objectId, { maxBytes: 100 })).rejects.toThrow("max_fetch_bytes");
    const after = await stats();
    // A StateRef write is a server rpc write too (STATE_REF_V1.md decision 9).
    expect(after.puts_state_ref! - before.puts_state_ref!).toBe(1);
    expect(after.puts_rpc! - before.puts_rpc!).toBe(1);
    expect(after.gets_state_ref! - before.gets_state_ref!).toBeGreaterThanOrEqual(1);
    expect(after.upload_urls).toBe(0);
    await admin("delete?origin=state_ref");
    await expect(storage.get(objectId)).rejects.toBeInstanceOf(ObjectMissingError);
  });

  test("generateUploadUrl reports the grant's objectId", async () => {
    const storage = new VendorStorage(base, TOKEN);
    const pair = await storage.generateUploadUrl(3, "ab".repeat(32));
    expect(typeof pair.objectId).toBe("string");
    expect(pair.objectId!.length).toBeGreaterThan(0);
  });

  test("the upload fault corrupts a client PUT in transit and storage refuses it", async () => {
    const storage = new VendorStorage(base, TOKEN);
    const data = bytes(4096, 7);
    const sha = Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", data)))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
    const pair = await storage.generateUploadUrl(data.byteLength, sha);
    await admin("fault?purpose=upload&action=corrupt&count=1");
    const before = await stats();
    const put = await fetch(pair.uploadUrl, { method: "PUT", headers: pair.headers, body: data });
    expect(put.status).toBe(400);
    expect(await put.text()).toContain("BadDigest");
    const after = await stats();
    expect(after.uploads_corrupted! - before.uploads_corrupted!).toBe(1);
    expect(after.upload_bad_digest! - before.upload_bad_digest!).toBe(1);
  });

  test("externalConfigFromEnv: a result batch round-trips through a pointer with sha256", async () => {
    const config = externalConfigFromEnv({
      VGI_BLOB_VENDOR_URL: base,
      VGI_BLOB_VENDOR_TOKEN: TOKEN,
      VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES: "64",
    })!;
    const values = Array.from({ length: 1000 }, (_, i) => BigInt(i));
    const batch = batchFromColumns(SCHEMA, { v: values });
    const pointer = await maybeExternalizeBatch(batch, config);
    expect(pointer.numRows).toBe(0);
    expect(pointer.metadata?.get(LOCATION_KEY)).toStartWith(`${base}/`);
    expect(pointer.metadata?.get(LOCATION_SHA256_KEY)).toMatch(/^[0-9a-f]{64}$/);
    const resolved = await resolveExternalLocation(pointer, config);
    expect(resolved.numRows).toBe(1000);

    // The same pointer read through a corrupting download fails the digest.
    await admin("fault?purpose=rpc&action=corrupt&count=1");
    await expect(resolveExternalLocation(pointer, config)).rejects.toThrow(/SHA-256 checksum mismatch/);
  });

  test("zstd compression round-trips", async () => {
    const config = externalConfigFromEnv({
      VGI_BLOB_VENDOR_URL: base,
      VGI_BLOB_VENDOR_TOKEN: TOKEN,
      VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES: "1",
      VGI_RPC_EXTERNAL_COMPRESSION: "zstd",
    })!;
    const batch = batchFromColumns(SCHEMA, { v: [1n, 2n, 3n] });
    const pointer = await maybeExternalizeBatch(batch, config);
    expect(pointer.numRows).toBe(0);
    const resolved = await resolveExternalLocation(pointer, config);
    expect(resolved.numRows).toBe(3);
  });
});
