// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Storage HTTP timeouts are stall (idle) timeouts, never a whole-transfer
// deadline: a slow transfer that keeps moving completes with exactly one
// attempt, and a transfer that stops moving fails within the stall timeout.
//
// The storage side is a raw TCP (or TLS) server that throttles: it reads a
// PUT body at a fixed rate (pausing the socket, so TCP backpressure reaches the
// client), trickles a GET body, or stops moving bytes on request.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import net from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import tls from "node:tls";
import { Field, Int64, Schema } from "@query-farm/apache-arrow";
import { makeExternalLocationBatch, resolveExternalLocation } from "../src/external.js";
import {
  ENV_STORAGE_STALL_TIMEOUT,
  readStorageBody,
  StorageStallError,
  storageFetch,
  storageStallTimeoutMsFromEnv,
} from "../src/storage-http.js";
import { VendorBlobClient, VendorStorage } from "../src/vendor.js";

// Bun's fetch pulls a request body in ~1.5 MB batches, so pulls can be ~1 s
// apart at 2 MiB/s: a 2 s window leaves room, and every slow transfer below
// runs for several windows.
const STALL_MS = 2000;
const SLACK_MS = 1500;

interface Attempt {
  method: string;
  bodyBytes: number;
}

interface SlowServerOptions {
  /** Bytes/s for reading a PUT body and for writing a GET body. */
  rate: number;
  /** Stop moving bytes after this many body bytes (read or written). */
  stallAfter?: number;
  /** GET body size. */
  getSize?: number;
  /** Read the request, never answer. */
  noResponse?: boolean;
  tls?: { key: string; cert: string };
}

interface SlowServer {
  base: string;
  hostPort: string;
  attempts: Attempt[];
  close(): void;
}

function startSlowServer(opts: SlowServerOptions): Promise<SlowServer> {
  const attempts: Attempt[] = [];
  const sockets = new Set<net.Socket>();
  const onConnection = (socket: net.Socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    socket.on("error", () => {});
    let head = Buffer.alloc(0);
    let attempt: Attempt | null = null;
    let contentLength = 0;
    let stalled = false;
    const onData = (chunk: Buffer) => {
      if (stalled) return;
      if (attempt === null) {
        head = Buffer.concat([head, chunk]);
        const end = head.indexOf("\r\n\r\n");
        if (end < 0) return;
        const lines = head.subarray(0, end).toString("latin1").split("\r\n");
        const method = lines[0]!.split(" ")[0]!;
        for (const line of lines.slice(1)) {
          const [k, v] = line.split(/:\s*/, 2) as [string, string];
          if (k.toLowerCase() === "content-length") contentLength = Number(v);
        }
        attempt = { method, bodyBytes: 0 };
        attempts.push(attempt);
        chunk = head.subarray(end + 4);
        if (method === "GET") {
          if (!opts.noResponse) void sendGet(socket, attempt);
          return;
        }
      }
      attempt.bodyBytes += chunk.byteLength;
      if (opts.stallAfter !== undefined && attempt.bodyBytes >= opts.stallAfter) {
        stalled = true;
        socket.pause(); // never resumed: the client's writes back up
        return;
      }
      if (attempt.bodyBytes >= contentLength) {
        if (!opts.noResponse) socket.end("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        return;
      }
      socket.pause();
      setTimeout(() => socket.resume(), (chunk.byteLength / opts.rate) * 1000);
    };
    socket.on("data", onData);
  };
  const sendGet = async (socket: net.Socket, attempt: Attempt) => {
    const size = opts.getSize ?? 0;
    socket.write(`HTTP/1.1 200 OK\r\nContent-Length: ${size}\r\nConnection: close\r\n\r\n`);
    const block = Math.max(1, Math.floor(opts.rate / 20));
    let sent = 0;
    while (sent < size && !socket.destroyed) {
      if (opts.stallAfter !== undefined && sent >= opts.stallAfter) return; // stop sending, keep open
      const n = Math.min(block, size - sent);
      socket.write(Buffer.alloc(n, 0x61));
      sent += n;
      attempt.bodyBytes = sent;
      await Bun.sleep((n / opts.rate) * 1000);
    }
    socket.end();
  };
  const server = opts.tls
    ? tls.createServer({ key: opts.tls.key, cert: opts.tls.cert }, onConnection)
    : net.createServer(onConnection);
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const port = (server.address() as net.AddressInfo).port;
      resolve({
        base: `${opts.tls ? "https" : "http"}://127.0.0.1:${port}`,
        hostPort: `127.0.0.1:${port}`,
        attempts,
        close() {
          for (const s of sockets) s.destroy();
          server.close();
        },
      });
    });
  });
}

/** A minimal blob vendor whose grants point at `storage`. */
function startVendor(storage: SlowServer, objectSize: number) {
  const server = Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    async fetch(req) {
      const path = new URL(req.url).pathname;
      const expires = new Date(Date.now() + 3_600_000).toISOString();
      if (path === "/v1/info") {
        return Response.json({ region: "test", upload_hosts: [storage.hostPort], download_hosts: [storage.hostPort] });
      }
      if (path === "/v1/blobs/put") {
        const body = (await req.json()) as { size: number; sha256_b64: string };
        return Response.json({
          object_id: "obj-1",
          upload_url: `${storage.base}/obj-1?X-Amz-Signature=secret`,
          required_headers: { "Content-Length": String(body.size), "x-amz-checksum-sha256": body.sha256_b64 },
          download_url: `${storage.base}/obj-1?X-Amz-Signature=secret`,
          expires_at: expires,
        });
      }
      if (path === "/v1/blobs/get") {
        return Response.json({
          download_url: `${storage.base}/obj-1?X-Amz-Signature=secret`,
          size: objectSize,
          sha256_b64: "AAAA",
          content_encoding: null,
          expires_at: expires,
        });
      }
      return new Response("not found", { status: 404 });
    },
  });
  return { url: `http://127.0.0.1:${server.port}`, stop: () => server.stop(true) };
}

async function timed<T>(fn: () => Promise<T>): Promise<[T | Error, number]> {
  const t0 = performance.now();
  try {
    return [await fn(), performance.now() - t0];
  } catch (error) {
    return [error as Error, performance.now() - t0];
  }
}

const MiB = 1024 * 1024;

describe("stall timeout from the environment", () => {
  test("default 30 s, seconds parsed, junk refused", () => {
    expect(ENV_STORAGE_STALL_TIMEOUT).toBe("VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS");
    expect(storageStallTimeoutMsFromEnv({})).toBe(30_000);
    expect(storageStallTimeoutMsFromEnv({ [ENV_STORAGE_STALL_TIMEOUT]: "120" })).toBe(120_000);
    expect(storageStallTimeoutMsFromEnv({ [ENV_STORAGE_STALL_TIMEOUT]: "2.5" })).toBe(2_500);
    for (const bad of ["0", "-1", "abc", "1e3x", "Infinity"]) {
      expect(() => storageStallTimeoutMsFromEnv({ [ENV_STORAGE_STALL_TIMEOUT]: bad })).toThrow(
        ENV_STORAGE_STALL_TIMEOUT,
      );
    }
  });

  test("the vendor client reads it from its env", async () => {
    const storage = await startSlowServer({ rate: 1 });
    try {
      const vendor = VendorStorage.fromEnv({
        env: {
          VGI_BLOB_VENDOR_URL: "http://127.0.0.1:9",
          VGI_BLOB_VENDOR_TOKEN: "t",
          [ENV_STORAGE_STALL_TIMEOUT]: "7",
        },
      });
      expect((vendor as unknown as { api: { stallTimeoutMs: number } }).api.stallTimeoutMs).toBe(7_000);
    } finally {
      storage.close();
    }
  });
});

describe("vendor storage transfers: stall timeout, no whole-transfer deadline", () => {
  test("a slow upload that keeps moving succeeds with exactly one PUT", async () => {
    // 3 MiB at 512 KiB/s is ~6 s: three times the stall timeout, and three
    // times the whole-request timeout (timeoutMs), which the PUT must not use.
    const storage = await startSlowServer({ rate: 512 * 1024 });
    const vendor = startVendor(storage, 3 * MiB);
    try {
      const client = new VendorStorage(vendor.url, "token", { stallTimeoutMs: STALL_MS, timeoutMs: STALL_MS });
      const [result, ms] = await timed(() => client.upload(new Uint8Array(3 * MiB), ""));
      expect(result).toBeString();
      expect(ms).toBeGreaterThan(2 * STALL_MS);
      expect(storage.attempts.filter((a) => a.method === "PUT")).toEqual([{ method: "PUT", bodyBytes: 3 * MiB }]);
    } finally {
      vendor.stop();
      storage.close();
    }
  }, 30_000);

  test("an upload far larger than the socket buffers keeps moving to the end with one PUT", async () => {
    // 24 MiB at 2 MiB/s is ~12 s against a 2 s stall timeout: progress has to
    // come from chunks as the wire takes them, and the final buffer drain must
    // not read as a stall.
    const storage = await startSlowServer({ rate: 2 * MiB });
    const vendor = startVendor(storage, 24 * MiB);
    try {
      const client = new VendorStorage(vendor.url, "token", { stallTimeoutMs: STALL_MS, timeoutMs: STALL_MS });
      const [result, ms] = await timed(() => client.upload(new Uint8Array(24 * MiB), ""));
      expect(result).toBeString();
      expect(ms).toBeGreaterThan(4 * STALL_MS);
      expect(storage.attempts.filter((a) => a.method === "PUT")).toEqual([{ method: "PUT", bodyBytes: 24 * MiB }]);
    } finally {
      vendor.stop();
      storage.close();
    }
  }, 60_000);

  test("an upload whose storage stops reading fails within the stall timeout, once per attempt", async () => {
    const storage = await startSlowServer({ rate: 1024 * 1024, stallAfter: 256 * 1024 });
    // 64 MiB: far more than the socket buffers hold, so the stall shows up
    // while chunks are still being pulled (a body small enough to vanish into
    // the buffers is only caught by the end-of-body drain window).
    const vendor = startVendor(storage, 64 * MiB);
    try {
      const client = new VendorStorage(vendor.url, "token", {
        stallTimeoutMs: STALL_MS,
        maxAttempts: 2,
        backoffMs: 10,
      });
      const [result, ms] = await timed(() => client.upload(new Uint8Array(64 * MiB), ""));
      expect(result).toBeInstanceOf(Error);
      expect((result as Error).message).toContain("failed after 2 attempts (stalled)");
      expect((result as Error).message).not.toContain("secret");
      expect(storage.attempts.filter((a) => a.method === "PUT")).toHaveLength(2);
      expect(ms).toBeLessThan(2 * STALL_MS + SLACK_MS);
    } finally {
      vendor.stop();
      storage.close();
    }
  }, 30_000);

  test("a slow download that keeps moving succeeds with exactly one GET", async () => {
    const storage = await startSlowServer({ rate: 512 * 1024, getSize: 3 * MiB });
    const vendor = startVendor(storage, 3 * MiB);
    try {
      const client = new VendorStorage(vendor.url, "token", { stallTimeoutMs: STALL_MS, timeoutMs: STALL_MS });
      const [result, ms] = await timed(() => client.get("obj-1"));
      expect(result).toBeInstanceOf(Uint8Array);
      expect((result as Uint8Array).byteLength).toBe(3 * MiB);
      expect(ms).toBeGreaterThan(2 * STALL_MS);
      expect(storage.attempts.map((a) => a.method)).toEqual(["GET"]);
    } finally {
      vendor.stop();
      storage.close();
    }
  }, 30_000);

  test("a download that stalls mid-body is retried, then fails within the stall timeout", async () => {
    const storage = await startSlowServer({ rate: 4 * MiB, getSize: 3 * MiB, stallAfter: MiB });
    const vendor = startVendor(storage, 3 * MiB);
    try {
      const client = new VendorBlobClient(vendor.url, "token", {
        stallTimeoutMs: STALL_MS,
        maxAttempts: 2,
        backoffMs: 10,
      });
      const [result, ms] = await timed(() =>
        client.getBytes({ objectId: "obj-1", size: 3 * MiB, sha256: "0".repeat(64) }),
      );
      expect(result).toBeInstanceOf(Error);
      expect((result as Error).message).toContain("failed after 2 attempts (stalled)");
      expect(storage.attempts.map((a) => a.method)).toEqual(["GET", "GET"]);
      expect(ms).toBeLessThan(2 * STALL_MS + 2 * 250 + SLACK_MS);
    } finally {
      vendor.stop();
      storage.close();
    }
  }, 30_000);

  test("storage that never answers fails within the stall timeout", async () => {
    const storage = await startSlowServer({ rate: MiB, noResponse: true });
    const vendor = startVendor(storage, 10);
    try {
      const client = new VendorStorage(vendor.url, "token", { stallTimeoutMs: STALL_MS, maxAttempts: 1 });
      const [result, ms] = await timed(() => client.get("obj-1"));
      expect(result).toBeInstanceOf(Error);
      expect((result as Error).message).toContain("(stalled)");
      expect(ms).toBeLessThan(STALL_MS + SLACK_MS);
    } finally {
      vendor.stop();
      storage.close();
    }
  }, 30_000);
});

describe("inbound pointer fetch: stall timeout, no whole-fetch deadline", () => {
  const schema = new Schema([new Field("x", new Int64(), true)]);
  const config = { storage: { upload: async () => "" }, urlValidator: null, stallTimeoutMs: STALL_MS };

  test("a slow pointer fetch is not cut off", async () => {
    // The body is not Arrow IPC, so a fetch that completed fails in decoding,
    // never with a fetch error: the ~6 s fetch was not cut off.
    const slow = await startSlowServer({ rate: 256 * 1024, getSize: 1536 * 1024 });
    try {
      const [result, ms] = await timed(() =>
        resolveExternalLocation(makeExternalLocationBatch(schema, `${slow.base}/x`), config),
      );
      expect(result).toBeInstanceOf(Error);
      expect((result as Error).message).not.toContain("External location fetch");
      expect(ms).toBeGreaterThan(2 * STALL_MS);
      expect(slow.attempts).toEqual([{ method: "GET", bodyBytes: 1536 * 1024 }]);
    } finally {
      slow.close();
    }
  }, 30_000);

  test("a stalled pointer fetch fails within the stall timeout", async () => {
    const stuck = await startSlowServer({ rate: MiB, getSize: MiB, stallAfter: 64 * 1024 });
    try {
      const [result, ms] = await timed(() =>
        resolveExternalLocation(makeExternalLocationBatch(schema, `${stuck.base}/x`), config),
      );
      expect(result).toBeInstanceOf(Error);
      expect((result as Error).message).toContain("stalled");
      expect(ms).toBeLessThan(STALL_MS + SLACK_MS);
    } finally {
      stuck.close();
    }
  }, 30_000);

  test("a pointer server that never answers fails within the stall timeout", async () => {
    const mute = await startSlowServer({ rate: MiB, noResponse: true });
    try {
      const [result, ms] = await timed(() =>
        resolveExternalLocation(makeExternalLocationBatch(schema, `${mute.base}/x`), config),
      );
      expect((result as Error).message).toContain("stalled");
      expect(ms).toBeLessThan(STALL_MS + SLACK_MS);
    } finally {
      mute.close();
    }
  }, 30_000);
});

describe("storageFetch", () => {
  test("slow GET body completes; stalled GET body fails within the stall timeout", async () => {
    const slow = await startSlowServer({ rate: 256 * 1024, getSize: 1536 * 1024 });
    const stuck = await startSlowServer({ rate: MiB, getSize: MiB, stallAfter: 64 * 1024 });
    try {
      const [ok, okMs] = await timed(async () =>
        readStorageBody(await storageFetch(`${slow.base}/x`, { method: "GET", stallTimeoutMs: STALL_MS }), 2 * MiB),
      );
      expect((ok as Uint8Array).byteLength).toBe(1536 * 1024);
      expect(okMs).toBeGreaterThan(2 * STALL_MS);
      const [bad, badMs] = await timed(async () =>
        readStorageBody(await storageFetch(`${stuck.base}/x`, { method: "GET", stallTimeoutMs: STALL_MS }), 2 * MiB),
      );
      expect(bad).toBeInstanceOf(StorageStallError);
      expect(badMs).toBeLessThan(STALL_MS + SLACK_MS);
    } finally {
      slow.close();
      stuck.close();
    }
  }, 30_000);

  // Over TLS a single large write can get one deadline for the whole buffer;
  // the body must go in chunks so every chunk restarts the stall window.
  const certDir = mkdtempSync(join(tmpdir(), "vgi-stall-tls-"));
  let tlsCert: { key: string; cert: string } | null = null;
  beforeAll(() => {
    const r = Bun.spawnSync([
      "openssl",
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-keyout",
      join(certDir, "k.pem"),
      "-out",
      join(certDir, "c.pem"),
      "-days",
      "1",
      "-subj",
      "/CN=127.0.0.1",
    ]);
    if (r.exitCode === 0) {
      tlsCert = {
        key: readFileSync(join(certDir, "k.pem"), "utf8"),
        cert: readFileSync(join(certDir, "c.pem"), "utf8"),
      };
    }
  });
  afterAll(() => rmSync(certDir, { recursive: true, force: true }));

  test("a slow PUT over TLS keeps moving past many stall windows", async () => {
    if (!tlsCert) return; // no openssl
    const storage = await startSlowServer({ rate: 512 * 1024, tls: tlsCert });
    try {
      const body = new Uint8Array(3 * MiB);
      const [r, ms] = await timed(async () => {
        const resp = await storageFetch(`${storage.base}/x`, {
          method: "PUT",
          body,
          stallTimeoutMs: STALL_MS,
          extra: { tls: { rejectUnauthorized: false } },
        });
        await readStorageBody(resp, 1024);
        return resp.response.status;
      });
      expect(r).toBe(200);
      expect(ms).toBeGreaterThan(2 * STALL_MS);
      expect(storage.attempts).toEqual([{ method: "PUT", bodyBytes: 3 * MiB }]);
    } finally {
      storage.close();
    }
  }, 30_000);
});
