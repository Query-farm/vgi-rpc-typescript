// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// createS3Storage as an UploadUrlProvider (upload URLs v2): the PUT URL must
// sign Content-Length and x-amz-checksum-sha256 so storage refuses a body of
// the wrong size or digest. The unit tests presign offline; the live tests run
// against an S3-compatible endpoint (e.g. MinIO) when VGI_S3_TEST_ENDPOINT and
// VGI_S3_TEST_BUCKET are set, with AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY.

import { beforeAll, describe, expect, it } from "bun:test";
import { createHash, randomBytes } from "node:crypto";
import { createS3Storage } from "../src/s3.js";

const sha256Hex = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");

describe("createS3Storage.generateUploadUrl (offline presign)", () => {
  beforeAll(() => {
    process.env.AWS_ACCESS_KEY_ID ??= "AKIDEXAMPLE";
    process.env.AWS_SECRET_ACCESS_KEY ??= "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
  });

  it("signs content-length and the sha256 checksum header, and names the object", async () => {
    const storage = createS3Storage({
      bucket: "b",
      prefix: "p/",
      region: "us-east-1",
      endpointUrl: "http://127.0.0.1:9",
      verifyOnFirstUse: false,
    });
    const body = new TextEncoder().encode("hello upload");
    const grant = await storage.generateUploadUrl(body.byteLength, sha256Hex(body));
    const put = new URL(grant.uploadUrl);
    const signed = (put.searchParams.get("X-Amz-SignedHeaders") ?? "").split(";");
    expect(signed).toContain("content-length");
    expect(signed).toContain("x-amz-checksum-sha256");
    // The checksum is a header the client sends, not a hoisted query parameter.
    expect(put.searchParams.has("x-amz-checksum-sha256")).toBe(false);
    expect(grant.headers).toEqual({ "x-amz-checksum-sha256": createHash("sha256").update(body).digest("base64") });
    expect(grant.objectId).toMatch(/^p\/[0-9a-f-]{36}\.arrow$/);
    expect(new URL(grant.uploadUrl).pathname).toBe(new URL(grant.downloadUrl).pathname);
    expect(grant.expiresAt.getTime()).toBeGreaterThan(Date.now());
  });

  it("refuses a malformed digest or size", async () => {
    const storage = createS3Storage({
      bucket: "b",
      region: "us-east-1",
      endpointUrl: "http://127.0.0.1:9",
      verifyOnFirstUse: false,
    });
    await expect(storage.generateUploadUrl(10, "ABC")).rejects.toThrow("sha256");
    await expect(storage.generateUploadUrl(0, "0".repeat(64))).rejects.toThrow("size");
  });
});

const endpoint = process.env.VGI_S3_TEST_ENDPOINT;
const bucket = process.env.VGI_S3_TEST_BUCKET;
describe.skipIf(!endpoint || !bucket)("createS3Storage.generateUploadUrl (live S3-compatible endpoint)", () => {
  const make = () =>
    createS3Storage({
      bucket: bucket!,
      prefix: "vgi-rpc-ts-test/",
      region: process.env.AWS_REGION ?? "us-east-1",
      endpointUrl: endpoint!,
    });

  it("accepts the declared body; the object reads back by id and by the GET URL", async () => {
    const storage = make();
    const body = new Uint8Array(randomBytes(100_000));
    const grant = await storage.generateUploadUrl(body.byteLength, sha256Hex(body));
    const r = await fetch(grant.uploadUrl, { method: "PUT", body, headers: { ...grant.headers } });
    expect(r.status).toBe(200);
    expect(sha256Hex(await storage.get(grant.objectId!))).toBe(sha256Hex(body));
    const g = await fetch(grant.downloadUrl);
    expect(sha256Hex(new Uint8Array(await g.arrayBuffer()))).toBe(sha256Hex(body));
  });

  it("refuses a corrupted body of the declared length", async () => {
    const storage = make();
    const body = new Uint8Array(randomBytes(100_000));
    const grant = await storage.generateUploadUrl(body.byteLength, sha256Hex(body));
    const bad = body.slice();
    bad[0] ^= 0xff;
    const r = await fetch(grant.uploadUrl, { method: "PUT", body: bad, headers: { ...grant.headers } });
    expect(r.status).toBeGreaterThanOrEqual(400);
  });

  it("refuses a body of another length", async () => {
    const storage = make();
    const body = new Uint8Array(randomBytes(100_000));
    const grant = await storage.generateUploadUrl(body.byteLength, sha256Hex(body));
    const r = await fetch(grant.uploadUrl, { method: "PUT", body: body.slice(1), headers: { ...grant.headers } });
    expect(r.status).toBeGreaterThanOrEqual(400);
  });
});
