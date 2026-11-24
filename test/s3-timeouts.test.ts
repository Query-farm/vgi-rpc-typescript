// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// createS3Storage uses stall (socket idle) timeouts, never a whole-request
// deadline. Against a live S3-compatible endpoint (VGI_S3_TEST_ENDPOINT /
// VGI_S3_TEST_BUCKET, e.g. MinIO), through an in-process TCP proxy that
// throttles client-to-server bytes to a fixed rate or stops forwarding.

import { describe, expect, it } from "bun:test";
import net from "node:net";
import { createS3Storage } from "../src/s3.js";

const endpoint = process.env.VGI_S3_TEST_ENDPOINT;
const bucket = process.env.VGI_S3_TEST_BUCKET;
// Bun's fetch pulls a request body in ~1.5 MB batches, so at 2 MiB/s pulls are
// up to ~1 s apart: a 3 s window leaves room while staying far below the run.
const STALL_MS = 3000;
const MiB = 1024 * 1024;

/** A TCP proxy to `target` forwarding client bytes at `rate` bytes/s (all of
 *  them, or only the first `stallAfter`); server bytes pass freely. */
function throttlingProxy(target: URL, rate: number, stallAfter?: number) {
  let forwarded = 0;
  let connections = 0;
  const sockets = new Set<net.Socket>();
  const server = net.createServer((client) => {
    connections++;
    const upstream = net.connect(Number(target.port), target.hostname);
    for (const s of [client, upstream]) {
      sockets.add(s);
      s.on("error", () => {});
      s.on("close", () => {
        sockets.delete(s);
        client.destroy();
        upstream.destroy();
      });
    }
    upstream.on("data", (d) => client.write(d));
    client.on("data", (d) => {
      if (stallAfter !== undefined && forwarded >= stallAfter) {
        client.pause();
        return;
      }
      forwarded += d.byteLength;
      upstream.write(d);
      client.pause();
      setTimeout(() => client.resume(), (d.byteLength / rate) * 1000);
    });
  });
  return new Promise<{ url: string; connections: () => number; close: () => void }>((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const port = (server.address() as net.AddressInfo).port;
      resolve({
        url: `http://127.0.0.1:${port}`,
        connections: () => connections,
        close: () => {
          for (const s of sockets) s.destroy();
          server.close();
        },
      });
    });
  });
}

describe.skipIf(!endpoint || !bucket)("createS3Storage timeouts (live S3-compatible endpoint)", () => {
  const make = (url: string, maxAttempts = 3) => {
    const storage = createS3Storage({
      bucket: bucket!,
      prefix: "vgi-rpc-ts-test/",
      region: process.env.AWS_REGION ?? "us-east-1",
      endpointUrl: url,
      stallTimeoutMs: STALL_MS,
    });
    process.env.AWS_MAX_ATTEMPTS = String(maxAttempts);
    return storage;
  };

  it("a slow putObject that keeps moving succeeds on its first attempt", async () => {
    // 24 MiB at 2 MiB/s is ~12 s against a 3 s stall timeout.
    const proxy = await throttlingProxy(new URL(endpoint!), 2 * MiB);
    try {
      const storage = make(proxy.url);
      const t0 = performance.now();
      const id = await storage.putObject(new Uint8Array(24 * MiB));
      const ms = performance.now() - t0;
      expect(ms).toBeGreaterThan(3 * STALL_MS);
      expect(proxy.connections()).toBe(1);
      const back = await make(endpoint!).get(id, { maxBytes: 32 * MiB });
      expect(back.byteLength).toBe(24 * MiB);
    } finally {
      proxy.close();
    }
  }, 60_000);

  it("a putObject that stops moving fails within the stall timeout per attempt", async () => {
    const proxy = await throttlingProxy(new URL(endpoint!), 8 * MiB, 512 * 1024);
    try {
      const storage = make(proxy.url, 1);
      const t0 = performance.now();
      let error: unknown = null;
      try {
        await storage.putObject(new Uint8Array(16 * MiB));
      } catch (e) {
        error = e;
      }
      const ms = performance.now() - t0;
      expect(error).not.toBeNull();
      expect(ms).toBeLessThan(STALL_MS + 2500);
    } finally {
      proxy.close();
      delete process.env.AWS_MAX_ATTEMPTS;
    }
  }, 60_000);
});
