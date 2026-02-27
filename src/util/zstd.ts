// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Cross-runtime zstd compression/decompression.
 *
 * Uses Bun.zstd* when running on Bun, otherwise falls back to node:zlib
 * (available on Node.js 22.15+ and Deno 2.6.9+).
 */

import * as zlib from "node:zlib";

const isBun = typeof globalThis.Bun !== "undefined";

/** Compress data with zstd at the given level (1-22). */
export function zstdCompress(data: Uint8Array, level: number): Uint8Array {
  if (isBun) {
    return new Uint8Array(Bun.zstdCompressSync(data, { level }));
  }
  const fn = (zlib as any).zstdCompressSync;
  if (typeof fn !== "function") {
    throw new Error(
      "zstd is not available in this runtime. " +
        "Requires Bun, Node.js >= 22.15, or Deno >= 2.6.9.",
    );
  }
  return new Uint8Array(
    fn(data, {
      params: {
        [(zlib.constants as any).ZSTD_c_compressionLevel]: level,
      },
    }),
  );
}

/** Decompress zstd-compressed data. */
export function zstdDecompress(data: Uint8Array): Uint8Array {
  if (isBun) {
    return new Uint8Array(Bun.zstdDecompressSync(data));
  }
  const fn = (zlib as any).zstdDecompressSync;
  if (typeof fn !== "function") {
    throw new Error(
      "zstd is not available in this runtime. " +
        "Requires Bun, Node.js >= 22.15, or Deno >= 2.6.9.",
    );
  }
  return new Uint8Array(fn(data));
}
