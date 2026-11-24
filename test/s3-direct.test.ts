// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Direct S3 storage (contract v1): configuration from VGI_STORAGE_S3_*,
// explicit keys and session tokens, the public endpoint for handed-out URLs,
// the URL lifetime under temporary credentials, CHECKSUM=none and the bucket
// check. The live part runs against an S3-compatible endpoint with STS
// (RustFS in CI) when VGI_S3_TEST_ENDPOINT / VGI_S3_TEST_BUCKET are set and
// AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY hold its root keys.

import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { Sha256 } from "@aws-crypto/sha256-js";
import { SignatureV4 } from "@smithy/signature-v4";
import { createS3Storage, S3_ENV, s3StorageConfigFromEnv } from "../src/s3.js";

const endpoint = process.env.VGI_S3_TEST_ENDPOINT;
const bucket = process.env.VGI_S3_TEST_BUCKET;
const rootKey = process.env.AWS_ACCESS_KEY_ID ?? "";
const rootSecret = process.env.AWS_SECRET_ACCESS_KEY ?? "";

describe("s3StorageConfigFromEnv", () => {
  test("defaults, and every variable", async () => {
    expect(() => s3StorageConfigFromEnv({})).toThrow(S3_ENV.BUCKET);
    expect(s3StorageConfigFromEnv({ VGI_STORAGE_S3_BUCKET: "b" })).toMatchObject({
      bucket: "b",
      prefix: "rpc/",
      region: "us-east-1",
    });
    expect(
      s3StorageConfigFromEnv({ VGI_STORAGE_S3_BUCKET: "b", VGI_STORAGE_S3_PREFIX: "p/" }, { role: "fs" }).prefix,
    ).toBe("p/fs/");
    expect(s3StorageConfigFromEnv({ VGI_STORAGE_S3_BUCKET: "b", AWS_DEFAULT_REGION: "eu-west-1" }).region).toBe(
      "eu-west-1",
    );
    const c = s3StorageConfigFromEnv({
      VGI_STORAGE_S3_BUCKET: "b",
      VGI_STORAGE_S3_PREFIX: "vgi/",
      VGI_STORAGE_S3_ENDPOINT: "http://minio:9000/",
      VGI_STORAGE_S3_PUBLIC_ENDPOINT: "https://s3.example.com",
      VGI_STORAGE_S3_REGION: "auto",
      VGI_STORAGE_S3_FORCE_PATH_STYLE: "false",
      VGI_STORAGE_S3_CHECKSUM: "none",
      VGI_STORAGE_S3_SDK_CHECKSUM: "when_supported",
      VGI_STORAGE_S3_ACCESS_KEY_ID: "AK",
      VGI_STORAGE_S3_SECRET_ACCESS_KEY: "SK",
      VGI_STORAGE_S3_SESSION_TOKEN: "TOK",
      VGI_STORAGE_URL_TTL_SECONDS: "600",
      VGI_STORAGE_S3_SESSION_EXPIRES: "2030-01-01T00:00:00Z",
      VGI_STORAGE_ALLOW_PLAIN_HTTP: "1",
      AWS_REGION: "ignored",
    });
    expect(await (c.credentials as () => Promise<unknown>)()).toEqual({
      accessKeyId: "AK",
      secretAccessKey: "SK",
      sessionToken: "TOK",
      expiration: new Date("2030-01-01T00:00:00Z"),
    });
    expect(c).toMatchObject({
      prefix: "vgi/rpc/",
      endpointUrl: "http://minio:9000",
      publicEndpointUrl: "https://s3.example.com",
      region: "auto",
      forcePathStyle: false,
      checksum: "none",
      sdkChecksum: "when_supported",
      presignExpirySeconds: 600,
    });
  });

  test("plain http needs VGI_STORAGE_ALLOW_PLAIN_HTTP; a token without an expiry warns", () => {
    const b = { VGI_STORAGE_S3_BUCKET: "b" };
    expect(() => s3StorageConfigFromEnv({ ...b, VGI_STORAGE_S3_ENDPOINT: "http://minio:9000" })).toThrow(
      "VGI_STORAGE_ALLOW_PLAIN_HTTP",
    );
    expect(() =>
      s3StorageConfigFromEnv({
        ...b,
        VGI_STORAGE_S3_ENDPOINT: "https://s3.example.com",
        VGI_STORAGE_S3_PUBLIC_ENDPOINT: "http://s3.example.com",
      }),
    ).toThrow("VGI_STORAGE_S3_PUBLIC_ENDPOINT");
    const logs: string[] = [];
    s3StorageConfigFromEnv(
      {
        ...b,
        VGI_STORAGE_S3_ACCESS_KEY_ID: "A",
        VGI_STORAGE_S3_SECRET_ACCESS_KEY: "S",
        VGI_STORAGE_S3_SESSION_TOKEN: "T",
      },
      { log: (m) => logs.push(m) },
    );
    expect(logs.join()).toContain("VGI_STORAGE_S3_SESSION_EXPIRES");
    expect(logs.join()).not.toContain("T,");
    expect(() =>
      s3StorageConfigFromEnv({
        ...b,
        VGI_STORAGE_S3_ACCESS_KEY_ID: "A",
        VGI_STORAGE_S3_SECRET_ACCESS_KEY: "S",
        VGI_STORAGE_S3_SESSION_TOKEN: "T",
        VGI_STORAGE_S3_SESSION_EXPIRES: "soon",
      }),
    ).toThrow("VGI_STORAGE_S3_SESSION_EXPIRES");
  });

  test("refuses half-set keys, a token without keys, and junk values without echoing secrets", () => {
    const b = { VGI_STORAGE_S3_BUCKET: "b" };
    expect(() => s3StorageConfigFromEnv({ ...b, VGI_STORAGE_S3_ACCESS_KEY_ID: "AK" })).toThrow("set together");
    expect(() => s3StorageConfigFromEnv({ ...b, VGI_STORAGE_S3_SESSION_TOKEN: "very-secret-token" })).toThrow(/needs/);
    try {
      s3StorageConfigFromEnv({ ...b, VGI_STORAGE_S3_SESSION_TOKEN: "very-secret-token" });
    } catch (e) {
      expect((e as Error).message).not.toContain("very-secret-token");
    }
    expect(() => s3StorageConfigFromEnv({ ...b, VGI_STORAGE_S3_CHECKSUM: "md5" })).toThrow(S3_ENV.CHECKSUM);
    expect(() => s3StorageConfigFromEnv({ ...b, VGI_STORAGE_S3_ENDPOINT: "minio:9000" })).toThrow(S3_ENV.ENDPOINT);
    expect(() => s3StorageConfigFromEnv({ ...b, VGI_STORAGE_S3_FORCE_PATH_STYLE: "maybe" })).toThrow();
    expect(() => s3StorageConfigFromEnv({ ...b, VGI_STORAGE_URL_TTL_SECONDS: "0" })).toThrow();
  });

  test("checksum none: no upload grants", () => {
    expect(createS3Storage({ bucket: "b", checksum: "none" }).generateUploadUrl).toBeUndefined();
    expect(typeof createS3Storage({ bucket: "b" }).generateUploadUrl).toBe("function");
  });
});

/** STS AssumeRole against the test endpoint (RustFS and MinIO serve it at /). */
async function assumeRole(
  seconds: number,
): Promise<{ accessKeyId: string; secretAccessKey: string; sessionToken: string }> {
  const url = new URL(endpoint!);
  const body = new URLSearchParams({
    Action: "AssumeRole",
    Version: "2011-06-15",
    RoleArn: "arn:aws:iam::000000000000:role/vgi-test",
    RoleSessionName: "vgi-rpc-ts",
    DurationSeconds: String(seconds),
  }).toString();
  const signer = new SignatureV4({
    service: "sts",
    region: "us-east-1",
    credentials: { accessKeyId: rootKey, secretAccessKey: rootSecret },
    sha256: Sha256,
  });
  const signed = await signer.sign({
    method: "POST",
    protocol: url.protocol,
    hostname: url.hostname,
    port: url.port ? Number(url.port) : undefined,
    path: "/",
    headers: { host: url.host, "content-type": "application/x-www-form-urlencoded" },
    body,
  });
  const r = await fetch(`${endpoint}/`, { method: "POST", headers: signed.headers as Record<string, string>, body });
  const xml = await r.text();
  const pick = (tag: string) => new RegExp(`<${tag}>([^<]+)</${tag}>`).exec(xml)?.[1];
  const creds = {
    accessKeyId: pick("AccessKeyId")!,
    secretAccessKey: pick("SecretAccessKey")!,
    sessionToken: pick("SessionToken")!,
  };
  if (!r.ok || !creds.sessionToken) throw new Error(`AssumeRole failed: HTTP ${r.status}`);
  return creds;
}

const sha256Hex = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");

describe.skipIf(!endpoint || !bucket)("direct S3 storage (live S3-compatible endpoint with STS)", () => {
  const base = {
    VGI_STORAGE_S3_BUCKET: bucket!,
    VGI_STORAGE_S3_ENDPOINT: endpoint!,
    VGI_STORAGE_S3_PREFIX: "direct/",
    VGI_STORAGE_ALLOW_PLAIN_HTTP: "1",
  };

  async function roundTrip(storage: ReturnType<typeof createS3Storage>) {
    const data = new Uint8Array(5000).fill(3);
    const url = await storage.upload(data, "");
    expect((await fetch(url)).status).toBe(200);
    const id = await storage.putObject(data);
    expect((await storage.get(id)).byteLength).toBe(5000);
    if (storage.generateUploadUrl) {
      const grant = await storage.generateUploadUrl(data.byteLength, sha256Hex(data));
      expect((await fetch(grant.uploadUrl, { method: "PUT", body: data, headers: grant.headers })).status).toBe(200);
      expect((await fetch(grant.downloadUrl)).status).toBe(200);
    }
    return url;
  }

  test("explicit keys from VGI_STORAGE_S3_*, with no AWS_* in the environment", async () => {
    const storage = createS3Storage(
      s3StorageConfigFromEnv({
        ...base,
        VGI_STORAGE_S3_ACCESS_KEY_ID: rootKey,
        VGI_STORAGE_S3_SECRET_ACCESS_KEY: rootSecret,
      }),
    );
    await storage.verify();
    await roundTrip(storage);
  });

  test("a session token (STS AssumeRole) is used and required", async () => {
    const creds = await assumeRole(900);
    const env = {
      ...base,
      VGI_STORAGE_S3_ACCESS_KEY_ID: creds.accessKeyId,
      VGI_STORAGE_S3_SECRET_ACCESS_KEY: creds.secretAccessKey,
      VGI_STORAGE_S3_SESSION_TOKEN: creds.sessionToken,
    };
    const url = await roundTrip(createS3Storage(s3StorageConfigFromEnv(env)));
    expect(new URL(url).searchParams.get("X-Amz-Security-Token")).toBe(creds.sessionToken);
    // Without the token the same key pair is refused.
    const noToken = createS3Storage(s3StorageConfigFromEnv({ ...env, VGI_STORAGE_S3_SESSION_TOKEN: "" }));
    await expect(noToken.verify()).rejects.toThrow();
  });

  test("VGI_STORAGE_S3_SESSION_EXPIRES caps URL lifetimes; a session too short for a grant warns and signs what is left", async () => {
    const creds = await assumeRole(900);
    const logs: string[] = [];
    const env = {
      ...base,
      VGI_STORAGE_S3_ACCESS_KEY_ID: creds.accessKeyId,
      VGI_STORAGE_S3_SECRET_ACCESS_KEY: creds.secretAccessKey,
      VGI_STORAGE_S3_SESSION_TOKEN: creds.sessionToken,
      VGI_STORAGE_S3_SESSION_EXPIRES: String(Math.floor(Date.now() / 1000) + 600),
    };
    const storage = createS3Storage(s3StorageConfigFromEnv(env, { log: (m) => logs.push(m) }));
    const data = new Uint8Array([4, 5, 6]);
    const grant = await storage.generateUploadUrl!(3, sha256Hex(data));
    expect(Number(new URL(grant.uploadUrl).searchParams.get("X-Amz-Expires"))).toBeLessThanOrEqual(540);
    expect(logs.join()).toContain("signing for the remaining lifetime");
    expect((await fetch(grant.uploadUrl, { method: "PUT", body: data, headers: grant.headers })).status).toBe(200);
  });

  test("best-effort delete removes the object and ignores a missing one", async () => {
    const storage = createS3Storage(
      s3StorageConfigFromEnv({
        ...base,
        VGI_STORAGE_S3_ACCESS_KEY_ID: rootKey,
        VGI_STORAGE_S3_SECRET_ACCESS_KEY: rootSecret,
      }),
    );
    const id = await storage.putObject(new Uint8Array([1]));
    await storage.deleteObject(id);
    await expect(storage.get(id)).rejects.toThrow();
    await storage.deleteObject(id);
    await storage.deleteObject("direct/rpc/never-existed");
  });

  test("URL lifetime under temporary credentials: capped at expiry - 60 s, refreshed below 15 min", async () => {
    const creds = await assumeRole(900);
    let refreshes = 0;
    // Expires in 10 minutes: below the upload-grant minimum, so the provider
    // is asked to refresh; the refreshed identity lasts 20 minutes.
    const provider = async (options?: { forceRefresh?: boolean }) => {
      if (options?.forceRefresh) refreshes++;
      const minutes = refreshes > 0 ? 20 : 10;
      return { ...creds, expiration: new Date(Date.now() + minutes * 60_000) };
    };
    const storage = createS3Storage({
      bucket: bucket!,
      prefix: "direct/",
      endpointUrl: endpoint!,
      region: "us-east-1",
      credentials: provider,
    });
    const data = new Uint8Array([1, 2, 3]);
    const result = new URL(await storage.upload(data, ""));
    expect(Number(result.searchParams.get("X-Amz-Expires"))).toBeLessThanOrEqual(10 * 60 - 60);
    expect(refreshes).toBe(0);
    const grant = await storage.generateUploadUrl(3, sha256Hex(data));
    expect(refreshes).toBe(1);
    const expires = Number(new URL(grant.uploadUrl).searchParams.get("X-Amz-Expires"));
    expect(expires).toBeGreaterThanOrEqual(15 * 60);
    expect(expires).toBeLessThanOrEqual(20 * 60 - 60);
  });

  test("VGI_STORAGE_S3_PUBLIC_ENDPOINT: handed-out URLs use it, requests use the endpoint", async () => {
    const internal = new URL(endpoint!);
    const publicEndpoint = `${internal.protocol}//localhost:${internal.port}`;
    const storage = createS3Storage(
      s3StorageConfigFromEnv({
        ...base,
        VGI_STORAGE_S3_PUBLIC_ENDPOINT: publicEndpoint,
        VGI_STORAGE_S3_ACCESS_KEY_ID: rootKey,
        VGI_STORAGE_S3_SECRET_ACCESS_KEY: rootSecret,
      }),
    );
    const url = await roundTrip(storage);
    expect(new URL(url).host).toBe(`localhost:${internal.port}`);
    await storage.urlValidator(url);
    await expect(storage.urlValidator(url.replace("localhost", "127.0.0.1"))).rejects.toThrow();
  });

  test("VGI_STORAGE_S3_CHECKSUM=none: results and objects, no upload grants", async () => {
    const storage = createS3Storage(
      s3StorageConfigFromEnv({
        ...base,
        VGI_STORAGE_S3_CHECKSUM: "none",
        VGI_STORAGE_S3_ACCESS_KEY_ID: rootKey,
        VGI_STORAGE_S3_SECRET_ACCESS_KEY: rootSecret,
      }),
    );
    expect(storage.generateUploadUrl).toBeUndefined();
    await roundTrip(storage);
  });

  test("verify(): a missing bucket and denied access say so", async () => {
    const keys = { VGI_STORAGE_S3_ACCESS_KEY_ID: rootKey, VGI_STORAGE_S3_SECRET_ACCESS_KEY: rootSecret };
    await expect(
      createS3Storage(
        s3StorageConfigFromEnv({ ...base, ...keys, VGI_STORAGE_S3_BUCKET: "no-such-bucket-vgi" }),
      ).verify(),
    ).rejects.toThrow("does not exist");
    await expect(
      createS3Storage(
        s3StorageConfigFromEnv({
          ...base,
          VGI_STORAGE_S3_ACCESS_KEY_ID: rootKey,
          VGI_STORAGE_S3_SECRET_ACCESS_KEY: "wrong-secret",
        }),
      ).verify(),
    ).rejects.toThrow(/access denied|check failed/);
  });
});
