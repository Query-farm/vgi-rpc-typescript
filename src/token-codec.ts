// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * The standard token codec: compress, tag, then seal (`STATE_REF_V1.md` §7).
 *
 * Every sealed payload this framework mints -- the HTTP cursor and call
 * tokens, and the inline and stored `StateRef` payloads -- is compressed
 * *before* it is sealed: once sealed a token is ciphertext, and no outer codec
 * can find the redundancy in it.
 *
 * ```
 * sealed_payload := codec (1 byte) || body
 * ```
 *
 * | codec  | body                       | notes                                   |
 * |--------|----------------------------|-----------------------------------------|
 * | `0x00` | raw plaintext              | used when compression does not shrink   |
 * | `0x01` | one zstd frame             | the default; level 3                    |
 * | `0x02` | raw DEFLATE (RFC 1951)     | for runtimes without a zstd encoder     |
 * | other  | --                         | reserved; refused                       |
 *
 * The writer emits `0x01` where the runtime can encode zstd (Bun, Node >=
 * 22.15, Deno); on a runtime without a zstd *encoder* (Cloudflare workerd)
 * {@link packTokenAsync} emits `0x02` through the Web `CompressionStream`
 * (`"deflate-raw"`), like vgi-rpc-python's bare-install fallback. `0x00` is
 * used whenever compression does not shrink the payload. Every reader accepts
 * all three: {@link unpackTokenAsync} decodes DEFLATE with `node:zlib` or,
 * failing that, `DecompressionStream`; zstd decodes everywhere (fzstd on
 * workerd).
 *
 * The synchronous {@link packToken} cannot reach `CompressionStream`, so on
 * workerd it falls back to `0x00`; the framework's token mints use the async
 * variants.
 */

import { isZstdCompressAvailable, zstdCompressSyncOrNull, zstdDecompressSync } from "./util/zstd.js";

/** Body is the plaintext as-is. */
export const CODEC_RAW = 0x00;
/** Body is one zstd frame. */
export const CODEC_ZSTD = 0x01;
/** Body is a raw DEFLATE stream (RFC 1951, no zlib header). */
export const CODEC_DEFLATE = 0x02;

/** Decompression bound for tokens (64 MiB). */
export const MAX_TOKEN_PLAINTEXT_BYTES = 64 * 1024 * 1024;

/** Level 3: the same speed as level 1 on token-sized payloads and slightly smaller. */
const ZSTD_LEVEL = 3;

/** A payload carries an unknown codec tag or does not decompress. */
export class TokenCodecError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TokenCodecError";
  }
}

/**
 * Compress `plaintext` when that makes it smaller and tag the codec used.
 *
 * Returns `0x01 || zstd(plaintext)` when that is shorter than `plaintext`, else
 * `0x00 || plaintext` -- so a small payload never grows by more than the tag.
 */
export function packToken(plaintext: Uint8Array): Uint8Array {
  const compressed = zstdCompressSyncOrNull(plaintext, ZSTD_LEVEL);
  const useZstd = compressed !== null && compressed.byteLength < plaintext.byteLength;
  const body = useZstd ? compressed! : plaintext;
  const out = new Uint8Array(1 + body.byteLength);
  out[0] = useZstd ? CODEC_ZSTD : CODEC_RAW;
  out.set(body, 1);
  return out;
}

const _NODE_ZLIB_MOD = "node:zlib";
function inflateRawOrNull(): ((data: Uint8Array, opts: { maxOutputLength: number }) => Uint8Array) | null {
  const req: any = (import.meta as any).require ?? (globalThis as any).require ?? null;
  if (!req) return null;
  try {
    const fn = req(_NODE_ZLIB_MOD)?.inflateRawSync;
    return typeof fn === "function" ? fn : null;
  } catch {
    return null;
  }
}

function tagged(codec: number, body: Uint8Array): Uint8Array {
  const out = new Uint8Array(1 + body.byteLength);
  out[0] = codec;
  out.set(body, 1);
  return out;
}

async function streamThrough(
  data: Uint8Array,
  transform: TransformStream<Uint8Array, Uint8Array>,
  maxBytes: number,
): Promise<Uint8Array> {
  const copy = new Uint8Array(data.byteLength);
  copy.set(data);
  const reader = new Blob([copy]).stream().pipeThrough(transform).getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > maxBytes) throw new TokenCodecError("payload exceeds the decompression bound");
      chunks.push(value);
    }
  } finally {
    await reader.cancel().catch(() => {});
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}

/**
 * Raw-DEFLATE {@link packToken}: `0x02 || deflate-raw(plaintext)` when that is
 * shorter, else `0x00 || plaintext`. Uses the Web `CompressionStream`, which
 * every runtime (workerd included) provides.
 */
export async function packTokenDeflate(plaintext: Uint8Array): Promise<Uint8Array> {
  const compressed = await streamThrough(
    plaintext,
    new CompressionStream("deflate-raw" as CompressionFormat) as unknown as TransformStream<Uint8Array, Uint8Array>,
    Number.MAX_SAFE_INTEGER,
  );
  return compressed.byteLength < plaintext.byteLength
    ? tagged(CODEC_DEFLATE, compressed)
    : tagged(CODEC_RAW, plaintext);
}

/**
 * {@link packToken} on every runtime: zstd (`0x01`) where the runtime can
 * encode it, raw DEFLATE (`0x02`) where it cannot (workerd), raw (`0x00`)
 * when compression does not shrink the payload.
 */
export async function packTokenAsync(plaintext: Uint8Array): Promise<Uint8Array> {
  if (isZstdCompressAvailable()) return packToken(plaintext);
  return packTokenDeflate(plaintext);
}

/**
 * {@link unpackToken} on every runtime: also decodes DEFLATE (`0x02`) where
 * `node:zlib` is absent (workerd), through `DecompressionStream`, bounded
 * while it streams.
 */
export async function unpackTokenAsync(
  data: Uint8Array,
  maxOutputBytes: number = MAX_TOKEN_PLAINTEXT_BYTES,
): Promise<Uint8Array> {
  if (data.byteLength === 0 || data[0] !== CODEC_DEFLATE || inflateRawOrNull() !== null) {
    return unpackToken(data, maxOutputBytes);
  }
  return inflateRawStream(data.subarray(1), maxOutputBytes);
}

/** @internal Inflate a raw DEFLATE body with `DecompressionStream`, bounded. */
export async function inflateRawStream(body: Uint8Array, maxOutputBytes: number): Promise<Uint8Array> {
  try {
    return await streamThrough(
      body,
      new DecompressionStream("deflate-raw" as CompressionFormat) as unknown as TransformStream<Uint8Array, Uint8Array>,
      maxOutputBytes,
    );
  } catch (error) {
    if (error instanceof TokenCodecError) throw error;
    throw new TokenCodecError("deflate body does not decompress");
  }
}

/**
 * Reverse {@link packToken}, bounding the decompressed size.
 *
 * @throws TokenCodecError when the payload is empty, its codec tag is unknown,
 *   it does not decompress, or it decompresses past `maxOutputBytes`.
 */
export function unpackToken(data: Uint8Array, maxOutputBytes: number = MAX_TOKEN_PLAINTEXT_BYTES): Uint8Array {
  if (data.byteLength === 0) throw new TokenCodecError("empty payload");
  const tag = data[0];
  const body = data.subarray(1);
  if (tag === CODEC_RAW) {
    if (body.byteLength > maxOutputBytes) throw new TokenCodecError("payload exceeds the decompression bound");
    // A fresh buffer with byteOffset 0: Arrow readers build typed-array views
    // that must be aligned relative to the buffer.
    const out = new Uint8Array(body.byteLength);
    out.set(body);
    return out;
  }
  if (tag === CODEC_ZSTD) {
    try {
      return zstdDecompressSync(body, maxOutputBytes);
    } catch (error) {
      const message = error instanceof Error ? error.message : "";
      if (/exceed/.test(message)) throw new TokenCodecError("payload exceeds the decompression bound");
      throw new TokenCodecError("zstd body does not decompress");
    }
  }
  if (tag === CODEC_DEFLATE) {
    const inflate = inflateRawOrNull();
    if (inflate === null) throw new TokenCodecError("deflate is not available in this runtime");
    let out: Uint8Array;
    try {
      out = inflate(body, { maxOutputLength: maxOutputBytes });
    } catch {
      throw new TokenCodecError("deflate body does not decompress (or exceeds the decompression bound)");
    }
    const copy = new Uint8Array(out.byteLength);
    copy.set(out);
    return copy;
  }
  throw new TokenCodecError(`unknown codec 0x${tag.toString(16).padStart(2, "0")}`);
}
