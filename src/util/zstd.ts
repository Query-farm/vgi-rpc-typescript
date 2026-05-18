// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Cross-runtime zstd compression/decompression.
 *
 * Decompression order of preference: Bun.zstd → node:zlib zstd (Node 22.15+,
 * Deno 2.6.9+) → fzstd pure-JS fallback. The fzstd fallback exists so
 * Cloudflare workerd — which has no native zstd — can still decode
 * `Content-Encoding: zstd` request bodies (the DuckDB VGI extension always
 * sends them). fzstd is decompression-only, so compression on workerd still
 * throws.
 */

import { decompress as fzstdDecompress } from "fzstd";

// Resolve node:zlib via indirect-string require so esbuild/wrangler can't
// trace it statically. On workerd we want the fzstd path, not a node:zlib
// import that wouldn't have zstd anyway.
const _NODE_ZLIB_MOD = "node:zlib";
const isBun = typeof globalThis.Bun !== "undefined";
function _loadZlibOrNull(): any | null {
  const req: any = (import.meta as any).require ?? (globalThis as any).require ?? null;
  if (!req) return null;
  try {
    return req(_NODE_ZLIB_MOD);
  } catch {
    return null;
  }
}

/** Return true when the current runtime can produce zstd-compressed output.
 *
 *  Bun has `Bun.zstdCompressSync`; Node ≥22.15 / Deno ≥2.6.9 expose it via
 *  `node:zlib`. Other runtimes (workerd, older Node) have no encoder. The
 *  fzstd fallback is decompress-only so it doesn't count.
 */
export function isZstdCompressAvailable(): boolean {
  if (isBun) return true;
  const zlib = _loadZlibOrNull();
  return typeof zlib?.zstdCompressSync === "function";
}

/** Compress data with zstd at the given level (1-22). */
export async function zstdCompress(data: Uint8Array, level: number): Promise<Uint8Array<ArrayBuffer>> {
  if (isBun) {
    return new Uint8Array(Bun.zstdCompressSync(data, { level }));
  }
  const zlib = _loadZlibOrNull();
  const fn = zlib?.zstdCompressSync;
  if (typeof fn !== "function") {
    throw new Error(
      "zstd compression is not available in this runtime. " +
        "Requires Bun or Node.js >= 22.15 / Deno >= 2.6.9. " +
        "(workerd has no native zstd encoder; fzstd is decompress-only.)",
    );
  }
  return new Uint8Array(
    fn(data, {
      params: {
        [zlib.constants.ZSTD_c_compressionLevel]: level,
      },
    }),
  );
}

/**
 * Decompress zstd-compressed data, optionally bounding the output size.
 *
 * Zstd frames carry the decompressed size in the header and decompressors
 * trust it eagerly: a ~3 KB compressed body claiming 100 MB output would
 * allocate 100 MB. When `maxOutputSize` is supplied, this helper:
 *
 * 1. Reads `Frame_Content_Size` from the frame header. If declared and
 *    above the cap, refuses *before* allocation with a clear error.
 * 2. Decompresses, then asserts the actual output size is also under the
 *    cap (covers frames whose size is not in the header — a streaming
 *    cap would be tighter, but neither Bun.zstdDecompressSync nor
 *    node:zlib's sync API exposes one, so we use the post-check).
 *
 * Mirrors the Python server-side fix in `_decompress_body` and the
 * client-side fix in `external_fetch.fetch_url`.
 */
export async function zstdDecompress(data: Uint8Array, maxOutputSize?: number): Promise<Uint8Array<ArrayBuffer>> {
  if (maxOutputSize != null) {
    const declared = readZstdFrameContentSize(data);
    if (declared !== null && declared > maxOutputSize) {
      throw new Error(`zstd decompressed size (${declared}) would exceed cap (${maxOutputSize})`);
    }
  }

  let out: Uint8Array<ArrayBuffer>;
  if (isBun) {
    out = new Uint8Array(Bun.zstdDecompressSync(data));
  } else {
    const zlib = _loadZlibOrNull();
    const fn = zlib?.zstdDecompressSync;
    if (typeof fn === "function") {
      out = new Uint8Array(fn(data));
    } else {
      // workerd path: no native zstd, fall back to the pure-JS decoder.
      // fzstd is decompress-only and synchronous; cap-checking already ran
      // above against the frame header, but pure-JS decode of large inputs
      // is slow — keep the upstream maxOutputSize tight.
      //
      // CRITICAL: copy into a freshly-allocated ArrayBuffer so byteOffset is
      // 0. fzstd internally returns subarray views with arbitrary byteOffset
      // (often not 8-aligned), and downstream Arrow IPC readers create
      // BigInt64Array views relative to the buffer's byteOffset — those
      // throw `start offset of BigInt64Array should be a multiple of 8` if
      // the underlying offset isn't 8-aligned.
      const decoded = fzstdDecompress(data);
      out = new Uint8Array(decoded.byteLength);
      out.set(decoded);
    }
  }

  if (maxOutputSize != null && out.byteLength > maxOutputSize) {
    throw new Error(`zstd decompressed size (${out.byteLength}) exceeds cap (${maxOutputSize})`);
  }
  return out;
}

/**
 * Parse `Frame_Content_Size` from a zstd frame header.
 *
 * Returns the declared decompressed size, or `null` if the frame header
 * does not include it (frames may omit it for streaming compression) or
 * the input is too short / not a valid zstd frame magic.
 *
 * Frame format (RFC 8478): magic(4) | FHD(1) | window_desc(0|1) |
 * dict_id(0|1|2|4) | frame_content_size(0|1|2|4|8). FCS_size depends on
 * FCS_field_size (FHD bits 6-7) and Single_Segment_flag (FHD bit 5).
 */
function readZstdFrameContentSize(data: Uint8Array): number | null {
  if (data.length < 6) return null;
  // Magic: 0xFD2FB528 little-endian.
  if (data[0] !== 0x28 || data[1] !== 0xb5 || data[2] !== 0x2f || data[3] !== 0xfd) {
    return null;
  }
  const fhd = data[4];
  const fcsFieldSize = (fhd >> 6) & 0x3;
  const singleSegment = ((fhd >> 5) & 0x1) === 1;
  const dictIdFlag = fhd & 0x3;
  // Per spec: FCS_size = 0 → 0 unless Single_Segment_flag is set, then 1.
  const fcsSize = fcsFieldSize === 0 ? (singleSegment ? 1 : 0) : fcsFieldSize === 1 ? 2 : fcsFieldSize === 2 ? 4 : 8;
  if (fcsSize === 0) return null;

  const windowDescSize = singleSegment ? 0 : 1;
  const dictIdSize = dictIdFlag === 0 ? 0 : dictIdFlag === 1 ? 1 : dictIdFlag === 2 ? 2 : 4;
  const fcsOffset = 5 + windowDescSize + dictIdSize;
  if (data.length < fcsOffset + fcsSize) return null;

  let fcs = 0n;
  for (let i = 0; i < fcsSize; i++) {
    fcs |= BigInt(data[fcsOffset + i]) << BigInt(i * 8);
  }
  // FCS_field_size == 1 (size 2) carries an offset of 256.
  if (fcsSize === 2) fcs += 256n;
  if (fcs > BigInt(Number.MAX_SAFE_INTEGER)) return Number.MAX_SAFE_INTEGER;
  return Number(fcs);
}
