// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Cross-runtime zstd compression/decompression.
 *
 * Uses Bun.zstd* when running on Bun, otherwise falls back to node:zlib
 * (available on Node.js 22.15+ and Deno 2.6.9+). On Cloudflare workerd
 * `nodejs_compat_v2` polyfills node:zlib's gzip/deflate APIs but **not**
 * zstd; the functions below throw on workerd. zstd is opt-in (only enabled
 * when `compressionLevel` is configured), so the bundle stays valid even
 * when zstd is unreachable.
 */

// Resolve node:zlib via indirect-string require so esbuild/wrangler can't
// trace it statically. workerd has neither bun:zstd nor node:zlib zstd APIs;
// throwing at call time keeps the bundle valid.
const _NODE_ZLIB_MOD = "node:zlib";
const isBun = typeof globalThis.Bun !== "undefined";
function _loadZlib(): any {
  const req: any = (globalThis as any).require ?? null;
  if (!req) {
    throw new Error(
      "zstd is not available in this runtime. " +
        "Requires Bun, Node.js >= 22.15, or Deno >= 2.6.9.",
    );
  }
  return req(_NODE_ZLIB_MOD);
}

/** Compress data with zstd at the given level (1-22). */
export async function zstdCompress(data: Uint8Array, level: number): Promise<Uint8Array<ArrayBuffer>> {
  if (isBun) {
    return new Uint8Array(Bun.zstdCompressSync(data, { level }));
  }
  const zlib = _loadZlib();
  const fn = zlib.zstdCompressSync;
  if (typeof fn !== "function") {
    throw new Error("zstd is not available in this runtime. " + "Requires Bun, Node.js >= 22.15, or Deno >= 2.6.9.");
  }
  return new Uint8Array(
    fn(data, {
      params: {
        [zlib.constants.ZSTD_c_compressionLevel]: level,
      },
    }),
  );
}

/** Decompress zstd-compressed data. */
export async function zstdDecompress(data: Uint8Array): Promise<Uint8Array<ArrayBuffer>> {
  if (isBun) {
    return new Uint8Array(Bun.zstdDecompressSync(data));
  }
  const zlib = _loadZlib();
  const fn = zlib.zstdDecompressSync;
  if (typeof fn !== "function") {
    throw new Error("zstd is not available in this runtime. " + "Requires Bun, Node.js >= 22.15, or Deno >= 2.6.9.");
  }
  return new Uint8Array(fn(data));
}
