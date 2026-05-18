// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Cross-runtime gzip compression/decompression.
 *
 * The Python `vgi-rpc[http]` client now defaults to `Content-Encoding: gzip`
 * on every request, so every HTTP server in the framework must decode it.
 * We use the Web platform `DecompressionStream`/`CompressionStream` APIs,
 * which Bun, Node 18+, Deno, and Cloudflare workerd all expose.
 */

async function streamThrough(
  data: Uint8Array,
  transform: ReadableWritablePair<Uint8Array, BufferSource>,
  maxOutputSize?: number,
): Promise<Uint8Array<ArrayBuffer>> {
  const ws = transform.writable.getWriter();
  const rs = transform.readable.getReader();
  // Copy into a freshly-allocated ArrayBuffer-backed view to satisfy TS lib
  // BufferSource narrowing (rules out SharedArrayBuffer-backed Uint8Array).
  const view = new Uint8Array(data.byteLength);
  view.set(data);
  const writePromise = (async () => {
    await ws.write(view as BufferSource);
    await ws.close();
  })();
  const chunks: Uint8Array[] = [];
  let total = 0;
  while (true) {
    const { value, done } = await rs.read();
    if (done) break;
    const chunk = value as Uint8Array;
    total += chunk.byteLength;
    if (maxOutputSize != null && total > maxOutputSize) {
      throw new Error(`gzip decompressed size (${total}) exceeds cap (${maxOutputSize})`);
    }
    chunks.push(chunk);
  }
  await writePromise;
  const out = new Uint8Array(total);
  let offset = 0;
  for (const c of chunks) {
    out.set(c, offset);
    offset += c.byteLength;
  }
  return out;
}

/**
 * Decompress gzip-encoded data, optionally bounded by `maxOutputSize`.
 *
 * The gzip footer's ISIZE field is mod 2^32 so it can't be trusted for a
 * pre-check — we bound output incrementally during streaming decode.
 */
export async function gzipDecompress(data: Uint8Array, maxOutputSize?: number): Promise<Uint8Array<ArrayBuffer>> {
  return streamThrough(data, new DecompressionStream("gzip"), maxOutputSize);
}

/** Compress data with gzip. `level` is accepted for API parity with zstd but ignored — the Web API doesn't expose a level. */
export async function gzipCompress(data: Uint8Array, _level?: number): Promise<Uint8Array<ArrayBuffer>> {
  return streamThrough(data, new CompressionStream("gzip"));
}
