// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Client-side response-body decoding.
 *
 * The server names the codec it used in one of two headers, and which one it
 * picked decides whether the fetch layer has already done the work for us:
 *
 * - **`Content-Encoding`** — the standard header. Every fetch implementation
 *   (undici/Node, Bun, Deno, browsers) transparently decodes the codecs it
 *   knows — gzip, deflate, br — before `arrayBuffer()` ever returns, and may
 *   leave the header in place while doing so. Decoding those a second time
 *   would corrupt the body. `zstd` is the exception: no runtime decodes it
 *   transparently, so that one is ours to undo.
 *
 * - **`X-VGI-Content-Encoding`** — VGI's own header, used when something
 *   between us and the server would mangle the standard one: a browser
 *   `fetch()` that cannot set `Accept-Encoding`, or a server on workerd, where
 *   the Cloudflare edge re-gzips an already-encoded body under the same header.
 *   Nothing in the transport understands this name, so the body arrives exactly
 *   as the server encoded it and **every** codec here is ours to undo.
 *
 * The custom header wins when both are present — that is the whole point of
 * stamping it, and it matches how `vgi-rpc-python` and the DuckDB VGI
 * extension resolve the pair.
 */

import { RpcError } from "../errors.js";
import { CONTENT_ENCODING_HEADER, VGI_CONTENT_ENCODING_HEADER } from "../http/codec.js";
import { gzipDecompress } from "../util/gzip.js";

/** Decompressor for a codec the platform will not undo for us. */
export type DecompressFn = (data: Uint8Array, maxOutputSize?: number) => Promise<Uint8Array>;

/** Independent cap for bytes that are still compressed/encoded after Fetch.
 * The accepted response budget applies to decoded Arrow bytes; this ceiling
 * only prevents an attacker from filling memory with an enormous compressed
 * representation before bounded decompression begins. */
export const DEFAULT_MAX_RESPONSE_REPRESENTATION_BYTES = 256 * 1024 * 1024;

/** Read a Fetch response incrementally. Identity/already-decoded bytes are
 * bounded by the advertised decoded limit. A custom-encoded or surviving zstd
 * representation uses the independent representation ceiling instead. */
export async function readResponseBodyBounded(
  response: Response,
  maxDecodedBytes: number,
  maxRepresentationBytes = DEFAULT_MAX_RESPONSE_REPRESENTATION_BYTES,
): Promise<Uint8Array> {
  const resolved = resolveResponseEncoding(response.headers);
  const custom = response.headers.get(VGI_CONTENT_ENCODING_HEADER)?.trim().toLowerCase();
  const standard = response.headers.get(CONTENT_ENCODING_HEADER)?.trim().toLowerCase();
  const encodedRepresentation = resolved.codec !== null;
  const fetchDecodedStandard =
    !custom && standard !== undefined && standard !== null && ["gzip", "deflate", "br"].includes(standard);
  const readLimit = encodedRepresentation ? maxRepresentationBytes : maxDecodedBytes;
  const declared = response.headers.get("Content-Length");
  // Fetch commonly leaves the encoded Content-Length after transparently
  // decoding standard gzip/deflate/br. It is not a decoded-size claim and
  // must not be compared to the accepted decoded budget.
  if (!fetchDecodedStandard && declared != null && /^[0-9]+$/.test(declared)) {
    const length = Number(declared);
    if (Number.isSafeInteger(length) && length > readLimit) {
      await response.body?.cancel("response limit exceeded");
      const kind = encodedRepresentation ? "representation safety" : "accepted";
      throw new RpcError("TransportError", `HTTP response exceeds ${kind} limit (${length} > ${readLimit})`, "");
    }
  }
  if (!response.body) return new Uint8Array();
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > readLimit) {
        await reader.cancel("response limit exceeded");
        const kind = encodedRepresentation ? "representation safety" : "accepted";
        throw new RpcError("TransportError", `HTTP response exceeds ${kind} limit (${total} > ${readLimit})`, "");
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const body = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return body;
}

/** How the body reached us, and therefore what is left for the client to do. */
export interface ResolvedResponseEncoding {
  /** Lower-cased codec name, or null when the body is already plain. */
  codec: string | null;
  /** True when the codec came from `X-VGI-Content-Encoding`. */
  custom: boolean;
}

/**
 * Resolve which codec, if any, the client still has to undo.
 *
 * Returns `{codec: null}` for an absent header, an explicit `identity`, and for
 * a standard-header codec the fetch layer has already handled — in every one of
 * those cases the caller must use the body as-is.
 */
export function resolveResponseEncoding(headers: Headers): ResolvedResponseEncoding {
  const custom = headers.get(VGI_CONTENT_ENCODING_HEADER)?.trim().toLowerCase();
  if (custom && custom !== "identity") {
    return { codec: custom, custom: true };
  }

  const standard = headers.get(CONTENT_ENCODING_HEADER)?.trim().toLowerCase();
  // Only zstd survives the fetch layer intact; gzip/deflate/br are already gone.
  if (standard === "zstd") {
    return { codec: "zstd", custom: false };
  }
  if (standard && standard !== "identity" && !["gzip", "deflate", "br"].includes(standard)) {
    return { codec: standard, custom: false };
  }
  return { codec: null, custom: false };
}

/**
 * Undo whatever encoding the response body still carries.
 *
 * `zstdDecompress` is injected because zstd rides on an optional dependency the
 * caller loads lazily; gzip needs no injection, being built on the Web
 * `DecompressionStream` that Bun, Node 18+, Deno, and workerd all expose.
 * Passing no zstd decompressor is fine on runtimes that never negotiate it.
 */
export async function decodeResponseBody(
  headers: Headers,
  body: Uint8Array,
  zstdDecompress?: DecompressFn,
  maxDecodedBytes?: number,
): Promise<Uint8Array> {
  const { codec, custom } = resolveResponseEncoding(headers);
  if (!codec) return body;

  if (codec === "gzip") {
    return new Uint8Array(await gzipDecompress(body, maxDecodedBytes));
  }
  if (codec === "zstd") {
    if (!zstdDecompress) {
      throw new RpcError(
        "ProtocolError",
        "Server sent a zstd-encoded response but this client has no zstd decoder. " +
          "Install the optional zstd dependency, or configure the server not to negotiate zstd.",
        "",
      );
    }
    const decoded = new Uint8Array(await zstdDecompress(body, maxDecodedBytes));
    if (maxDecodedBytes != null && decoded.byteLength > maxDecodedBytes) {
      throw new RpcError(
        "TransportError",
        `Decoded HTTP response exceeds accepted limit (${decoded.byteLength} > ${maxDecodedBytes})`,
        "",
      );
    }
    return decoded;
  }

  // An unknown codec is not recoverable, and handing the still-encoded bytes to
  // the Arrow reader would surface as an unintelligible "expected to read N
  // metadata bytes" far from the cause. Name it here instead.
  throw new RpcError(
    "ProtocolError",
    `Unsupported response encoding '${codec}'` +
      `${custom ? ` (${VGI_CONTENT_ENCODING_HEADER})` : ` (${CONTENT_ENCODING_HEADER})`}.`,
    "",
  );
}
