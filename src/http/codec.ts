// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Response-codec negotiation for the HTTP server.
 *
 * Mirrors the canonical implementation in `vgi-rpc-python`
 * (`vgi_rpc/_codec.py:parse_encoding_list` and
 * `vgi_rpc/http/server/_middleware.py:_CompressionMiddleware._pick_response_encoding`).
 *
 * Two request headers are consulted:
 *
 * - `X-VGI-Accept-Encoding` — VGI's own preference list. It exists because
 *   browsers cannot set `Accept-Encoding` (a forbidden header name for
 *   `fetch()`), so a WASM/browser client has *no other way* to ask for a
 *   compressed response; and because HTTP clients such as cpp-httplib (used by
 *   the DuckDB VGI extension) inject their own `Accept-Encoding: deflate,
 *   gzip, br, zstd`, listing gzip ahead of zstd.
 * - `Accept-Encoding` — the standard header.
 *
 * The client's stated order is honoured: the custom list is walked first, then
 * whatever the standard list adds. The first codec the server can actually
 * produce wins; if nothing overlaps the body is sent uncompressed.
 */

/**
 * zstd level used when `HttpHandlerOptions.compressionLevel` is left unset —
 * response compression is **on by default**.
 *
 * Level 1 rather than 3 because on Arrow IPC bodies it is not a speed/size
 * tradeoff: measured on an 8.41 MB body, level 1 took 5.77 ms and produced
 * 4.219 MB, level 3 took 27.39 ms and produced 4.384 MB — 4.7x faster *and*
 * smaller. All five VGI SDKs default to the same level so a mixed-language
 * fleet compresses identically (Python's `make_wsgi_app(compression_level=1)`
 * is the canonical reference).
 *
 * Pass `compressionLevel: null` to turn response compression off entirely.
 */
export const DEFAULT_COMPRESSION_LEVEL = 1;

/**
 * Codecs that actually compress. Deliberately not extended: no br, no deflate.
 * The tuple order is this server's own preference — informational only (it is
 * what `VGI-Response-Encodings` advertises); the client's order decides which
 * codec is used.
 */
export const COMPRESSION_ENCODINGS = ["zstd", "gzip"] as const;

/** A codec that compresses: `zstd` or `gzip`. */
export type CompressionEncoding = (typeof COMPRESSION_ENCODINGS)[number];

/**
 * A token accepted in the accept-encoding headers: a compressing codec or
 * `identity`. `identity` is an explicit "send it uncompressed" request — every
 * server can always produce it — so a client can turn response compression off
 * per request without relying on the accidental no-overlap path.
 */
export type Encoding = CompressionEncoding | "identity";

/** The explicit "no compression" token. */
export const IDENTITY_ENCODING = "identity";

const KNOWN_ENCODINGS: readonly string[] = [...COMPRESSION_ENCODINGS, IDENTITY_ENCODING];

/**
 * Parse a comma-separated `Accept-Encoding`-style header value into an
 * ordered, de-duplicated list of known codecs.
 *
 * Tokens that are not codecs we know are skipped silently (the caller
 * intersects with what the server can produce anyway). q-values are parsed
 * off and **ignored**, not honoured — matching Python's `parse_encoding_list`.
 * A missing/empty header parses to an empty list.
 */
export function parseEncodingList(headerValue: string | null | undefined): Encoding[] {
  if (!headerValue) return [];
  const out: Encoding[] = [];
  const seen = new Set<string>();
  for (const raw of headerValue.split(",")) {
    let token = raw.trim().toLowerCase();
    if (!token) continue;
    const semi = token.indexOf(";");
    if (semi >= 0) token = token.slice(0, semi).trim();
    if (!KNOWN_ENCODINGS.includes(token)) continue;
    if (seen.has(token)) continue;
    seen.add(token);
    out.push(token as Encoding);
  }
  return out;
}

/** Outcome of {@link pickResponseEncoding}. */
export interface NegotiatedEncoding {
  /**
   * The codec to compress the response with, or `null` to send it
   * uncompressed. `null` covers both "the client explicitly asked for
   * `identity`" and "nothing the client offered is producible" — both mean an
   * unencoded body with no content-encoding header, so they need not be
   * distinguished.
   */
  codec: CompressionEncoding | null;
  /**
   * True when the winning codec was offered *only* via `X-VGI-Accept-Encoding`.
   * Such a client's fetch/proxy layer would mangle or transparently decode a
   * standard `Content-Encoding`, so the response must be stamped
   * `X-VGI-Content-Encoding` instead.
   */
  usedCustom: boolean;
}

/**
 * Pick the response codec from the client's two accept headers.
 *
 * Walks `custom ++ [e for e in standard if e not in custom]` — the client's
 * stated order, custom header first — and returns the first codec for which
 * `canProduce` is true (runtime has an encoder *and* configuration enables
 * compression). `identity` is always producible, so reaching it before any
 * producible compressed codec ends the walk with "send it uncompressed". No
 * overlap yields `{ codec: null }` too, i.e. an uncompressed body.
 *
 * @param standardHeader raw `Accept-Encoding` request header value (or null)
 * @param customHeader   raw `X-VGI-Accept-Encoding` request header value (or null)
 * @param canProduce     predicate: can the server emit this codec right now?
 */
export function pickResponseEncoding(
  standardHeader: string | null | undefined,
  customHeader: string | null | undefined,
  canProduce: (encoding: CompressionEncoding) => boolean,
): NegotiatedEncoding {
  const standard = parseEncodingList(standardHeader);
  const custom = parseEncodingList(customHeader);
  const merged = [...custom, ...standard.filter((e) => !custom.includes(e))];
  for (const enc of merged) {
    // Explicit `identity` wins where it stands: uncompressed body, and no
    // content-encoding header at all — an identity body is just a body.
    if (enc === IDENTITY_ENCODING) {
      return { codec: null, usedCustom: false };
    }
    if (canProduce(enc)) {
      return { codec: enc, usedCustom: custom.includes(enc) && !standard.includes(enc) };
    }
  }
  // No overlap. `usedCustom` mirrors Python's `(None, bool(custom))`; it is
  // unused when `codec` is null.
  return { codec: null, usedCustom: custom.length > 0 };
}

/** Response header naming the codec when the client used the standard accept header. */
export const CONTENT_ENCODING_HEADER = "Content-Encoding";

/**
 * Response header naming the codec when the client could only state its
 * preference through `X-VGI-Accept-Encoding` (the browser/WASM path). Stamping
 * the standard header there would invite the fetch layer to double-decode.
 */
export const VGI_CONTENT_ENCODING_HEADER = "X-VGI-Content-Encoding";

/** VGI's own accept-encoding request header — settable from a browser `fetch()`. */
export const VGI_ACCEPT_ENCODING_HEADER = "X-VGI-Accept-Encoding";

/**
 * Capability header advertising the codecs this server speaks, in server-
 * preference order. The value is the **intersection** of what it can decode
 * on requests and what it can produce on responses — only codecs usable in
 * both directions. `identity` is omitted (always available, carries no
 * information).
 *
 * Emitted always, including with an empty value. Absent vs present-but-empty
 * is a real distinction: absent means "legacy server, assume zstd";
 * present-but-empty means "I speak no compression".
 *
 * Matches the header name used by `vgi-rpc-python`.
 */
export const SUPPORTED_ENCODINGS_HEADER = "VGI-Supported-Encodings";
