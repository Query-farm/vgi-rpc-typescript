// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// createGCSStorage on V4 signed URLs: the real @google-cloud/storage signer
// (a generated service-account key), against an in-process stand-in for the
// GCS XML API that enforces what GCS enforces on these URLs: the signed
// headers are present, Content-MD5 matches, x-goog-content-length-range
// holds. (fake-gcs-server 1.54 does not serve XML-API PUTs on signed URLs,
// so it cannot stand in here; a run against real GCS is pending.)

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { createHash, generateKeyPairSync } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ObjectMissingError } from "../src/external.js";
import { createGCSStorage, GCS_ENV, gcsStorageConfigFromEnv } from "../src/gcs.js";

const BUCKET = "vgi-gcs-test";
const objects = new Map<string, Uint8Array>();
const seen: { method: string; signedHeaders: string; headers: Record<string, string> }[] = [];
let stallGets = false;
let server: ReturnType<typeof Bun.serve>;
let endpoint = "";
let keyFile = "";
const dir = mkdtempSync(join(tmpdir(), "vgi-gcs-"));

beforeAll(() => {
  const { privateKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
  keyFile = join(dir, "sa.json");
  writeFileSync(
    keyFile,
    JSON.stringify({
      type: "service_account",
      project_id: "vgi-test",
      private_key_id: "k1",
      private_key: privateKey.export({ type: "pkcs8", format: "pem" }),
      client_email: "signer@vgi-test.iam.gserviceaccount.com",
      client_id: "1",
      token_uri: "https://oauth2.googleapis.com/token",
    }),
  );
  server = Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    async fetch(req) {
      const url = new URL(req.url);
      const signedHeaders = url.searchParams.get("X-Goog-SignedHeaders") ?? "";
      const headers: Record<string, string> = {};
      req.headers.forEach((v, k) => {
        headers[k] = v;
      });
      seen.push({ method: req.method, signedHeaders, headers });
      // The JSON API's bucket lookup (Bucket.exists), unauthenticated here.
      if (url.pathname === `/storage/v1/b/${BUCKET}`) return Response.json({ name: BUCKET });
      if (url.pathname.startsWith("/storage/v1/b/")) return Response.json({ error: { code: 404 } }, { status: 404 });
      if (
        !url.searchParams.get("X-Goog-Signature") ||
        url.searchParams.get("X-Goog-Algorithm") !== "GOOG4-RSA-SHA256"
      ) {
        return new Response("<Error><Code>AccessDenied</Code></Error>", { status: 403 });
      }
      const key = decodeURIComponent(url.pathname.slice(1));
      if (!key.startsWith(`${BUCKET}/`))
        return new Response("<Error><Code>NoSuchBucket</Code></Error>", { status: 404 });
      if (req.method === "PUT") {
        // Every signed header must be sent.
        for (const h of signedHeaders.split(";")) {
          if (h && h !== "host" && !(h in headers)) {
            return new Response("<Error><Code>SignatureDoesNotMatch</Code></Error>", { status: 403 });
          }
        }
        const body = new Uint8Array(await req.arrayBuffer());
        const md5 = headers["content-md5"];
        if (md5 && createHash("md5").update(body).digest("base64") !== md5) {
          return new Response("<Error><Code>BadDigest</Code></Error>", { status: 400 });
        }
        const range = headers["x-goog-content-length-range"];
        if (range) {
          const [lo, hi] = range.split(",").map(Number) as [number, number];
          if (body.byteLength < lo || body.byteLength > hi) {
            return new Response("<Error><Code>EntityTooLarge</Code></Error>", { status: 400 });
          }
        }
        objects.set(key, body);
        return new Response(null, { status: 200 });
      }
      if (req.method === "DELETE") {
        objects.delete(key);
        return new Response(null, { status: 204 });
      }
      if (stallGets) return new Promise<Response>(() => {});
      const body = objects.get(key);
      if (!body) return new Response("<Error><Code>NoSuchKey</Code></Error>", { status: 404 });
      return new Response(body);
    },
  });
  endpoint = `http://127.0.0.1:${server.port}`;
});

afterAll(() => {
  server.stop(true);
  rmSync(dir, { recursive: true, force: true });
});

const make = (extra: Record<string, unknown> = {}) =>
  createGCSStorage({ bucket: BUCKET, prefix: "t/", endpointUrl: endpoint, keyFilename: keyFile, ...extra });

describe("createGCSStorage (V4 signed URLs on the storage HTTP layer)", () => {
  test("results: signed PUT with Content-MD5, then a signed GET URL on the endpoint", async () => {
    const storage = make();
    const data = new Uint8Array([1, 2, 3, 4, 5]);
    const url = await storage.upload(data, "zstd");
    expect(new URL(url).origin).toBe(endpoint);
    const put = seen.findLast((s) => s.method === "PUT")!;
    expect(put.signedHeaders).toContain("content-md5");
    expect(put.headers["content-encoding"]).toBe("zstd");
    expect([...new Uint8Array(await (await fetch(url)).arrayBuffer())]).toEqual([1, 2, 3, 4, 5]);
    await storage.urlValidator(url);
    await expect(storage.urlValidator("http://evil.example/vgi-gcs-test/t/x")).rejects.toThrow();
    await expect(storage.urlValidator(`${endpoint}/other-bucket/x`)).rejects.toThrow();
  });

  test("objects: putObject / get round-trip, a missing object is ObjectMissingError, the cap holds", async () => {
    const storage = make();
    const id = await storage.putObject(new Uint8Array(1000).fill(7));
    expect((await storage.get(id)).byteLength).toBe(1000);
    await expect(storage.get("t/obj/nope")).rejects.toBeInstanceOf(ObjectMissingError);
    await expect(storage.get(id, { maxBytes: 10 })).rejects.toThrow("max_fetch_bytes");
  });

  test("upload grants: the size is pinned by x-goog-content-length-range", async () => {
    const storage = make();
    const body = new Uint8Array(64).fill(1);
    const sha = createHash("sha256").update(body).digest("hex");
    const grant = await storage.generateUploadUrl(64, sha);
    expect(grant.headers).toEqual({ "x-goog-content-length-range": "64,64" });
    expect((await fetch(grant.uploadUrl, { method: "PUT", body: body.slice(1), headers: grant.headers })).status).toBe(
      400,
    );
    expect((await fetch(grant.uploadUrl, { method: "PUT", body, headers: grant.headers })).status).toBe(200);
    expect((await storage.get(grant.objectId!)).byteLength).toBe(64);
  });

  test("verify() and best-effort delete", async () => {
    await make().verify();
    await expect(make({ bucket: "no-such-bucket" }).verify()).rejects.toThrow("does not exist");
    const storage = make();
    const id = await storage.putObject(new Uint8Array([1, 2]));
    await storage.deleteObject(id);
    await expect(storage.get(id)).rejects.toBeInstanceOf(ObjectMissingError);
  });

  test("a GET that never answers fails within the stall timeout", async () => {
    const storage = make({ stallTimeoutMs: 1000, maxAttempts: 1 });
    const id = await storage.putObject(new Uint8Array([9]));
    stallGets = true;
    const t0 = performance.now();
    try {
      await expect(storage.get(id)).rejects.toThrow("stalled");
    } finally {
      stallGets = false;
    }
    expect(performance.now() - t0).toBeLessThan(3000);
  });

  test("configuration from VGI_STORAGE_GCS_*", () => {
    expect(() => gcsStorageConfigFromEnv({})).toThrow(GCS_ENV.BUCKET);
    expect(
      gcsStorageConfigFromEnv({
        VGI_STORAGE_GCS_BUCKET: "b",
        VGI_STORAGE_GCS_PREFIX: "vgi/",
        VGI_STORAGE_GCS_ENDPOINT: "http://localhost:4443",
        VGI_STORAGE_GCS_CREDENTIALS_FILE: "/k.json",
        VGI_STORAGE_URL_TTL_SECONDS: "600",
        VGI_STORAGE_ALLOW_PLAIN_HTTP: "1",
      }),
    ).toMatchObject({
      bucket: "b",
      prefix: "vgi/rpc/",
      endpointUrl: "http://localhost:4443",
      keyFilename: "/k.json",
      presignExpirySeconds: 600,
    });
    expect(() =>
      gcsStorageConfigFromEnv({ VGI_STORAGE_GCS_BUCKET: "b", VGI_STORAGE_GCS_ENDPOINT: "ftp://x" }),
    ).toThrow();
    expect(() =>
      gcsStorageConfigFromEnv({ VGI_STORAGE_GCS_BUCKET: "b", VGI_STORAGE_GCS_ENDPOINT: "http://x" }),
    ).toThrow("VGI_STORAGE_ALLOW_PLAIN_HTTP");
    expect(gcsStorageConfigFromEnv({ VGI_STORAGE_GCS_BUCKET: "b" }, { role: "fs" }).prefix).toBe("fs/");
  });
});
