// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import type { ExternalLocationConfig, UploadUrlProvider } from "../external.js";
import type { DispatchHook, ServeStartHook } from "../types.js";
import type { AuthenticateFn, OAuthResourceMetadata } from "./auth.js";

/** Configuration options for createHttpHandler(). */
export interface HttpHandlerOptions {
  /** URL path prefix for all endpoints. Default: "" (root). */
  prefix?: string;
  /** XChaCha20-Poly1305 master key (32 bytes) used to seal stream state
   *  tokens.  A random 32-byte key is generated if omitted (tokens won't
   *  survive a restart or load-balance across workers). */
  tokenKey?: Uint8Array;
  /** State token time-to-live in seconds. Default: 3600 (1 hour). 0 disables TTL checks. */
  tokenTtl?: number;
  /** CORS allowed origins. If set, CORS headers are added to all responses. */
  corsOrigins?: string;
  /** Access-Control-Max-Age value in seconds for preflight OPTIONS responses. Default: 7200 (2 hours). null omits the header. */
  corsMaxAge?: number | null;
  /** Maximum request body size in bytes. Advertised via VGI-Max-Request-Bytes header. */
  maxRequestBytes?: number;
  /** Cap on the post-decompression size of a `Content-Encoding: zstd`
   *  request body, in bytes.  Defends against zstd decompression bombs:
   *  a tiny compressed frame can declare a huge decompressed size and
   *  blow up the server before {@link maxRequestBytes} ever sees the
   *  payload.  When omitted, defaults to `maxRequestBytes * 16` if that
   *  is set, otherwise unbounded. */
  maxDecompressedRequestBytes?: number;
  /** Maximum bytes before a producer stream emits a continuation token.
   *
   * @deprecated Use {@link maxResponseBytes} instead. The cap now governs all
   *  HTTP method responses (unary, exchange, producer), not just producer streams.
   */
  maxStreamResponseBytes?: number;
  /** HTTP body cap. Hard for unary and stream-exchange (overshoot surfaces
   *  as 200 + X-VGI-RPC-Error EXCEPTION batch). Soft for producer streams
   *  (overshoot mints a continuation token). Externalised payloads do not
   *  count toward this — they leave only tiny pointer batches on the wire.
   *  Advertised via VGI-Max-Response-Bytes.  Undefined = unbounded. */
  maxResponseBytes?: number;
  /** Cap on bytes uploaded to external storage during one HTTP response.
   *  Always hard — externalised uploads have no escape valve. Advertised via
   *  VGI-Max-Externalized-Response-Bytes.  Undefined = unbounded. */
  maxExternalizedResponseBytes?: number;
  /** Server ID included in response metadata. Random if omitted. */
  serverId?: string;
  /** Custom state serializer for stream state objects. Default: JSON with BigInt support. */
  stateSerializer?: StateSerializer;
  /** zstd compression level for responses (1-22). If set, responses are
   *  compressed when the client sends Accept-Encoding: zstd. */
  compressionLevel?: number;
  /** Optional authentication callback. Called for each request before dispatch. */
  authenticate?: AuthenticateFn;
  /** Optional RFC 9728 OAuth Protected Resource Metadata. Served at well-known endpoint. */
  oauthResourceMetadata?: OAuthResourceMetadata;
  /** Optional dispatch hook for observability (tracing, metrics). */
  dispatchHook?: DispatchHook;
  /** Optional lifecycle hook fired once on the first dispatched request.
   *  Mirrors Python's on_serve_start; lazy-firing keeps it fork-safe for
   *  pre-fork servers. */
  onServeStart?: ServeStartHook;
  /** Enable HTML landing page at GET {prefix}/. Default: true. */
  enableLandingPage?: boolean;
  /** Enable HTML describe/API reference page at GET {prefix}/describe. Default: true. */
  enableDescribePage?: boolean;
  /** Enable HTML 404 page for unmatched GET routes. Default: true. */
  enableNotFoundPage?: boolean;
  /** Enable JSON health endpoint at GET {prefix}/health. Default: true. */
  enableHealthEndpoint?: boolean;
  /** Protocol name shown in HTML pages. Defaults to the Protocol's name. */
  protocolName?: string;
  /** Operator-supplied protocol-contract version label, surfaced on every
   *  access-log record so dashboards and alerts can key off contract
   *  changes.  Mirrors the Python `RpcServer(..., protocol_version=...)`
   *  argument. */
  protocolVersion?: string;
  /** URL to service's source repository, shown in landing/describe pages. */
  repositoryUrl?: string;
  /** External storage config for externalizing large response batches. */
  externalLocation?: ExternalLocationConfig;
  /** Provider for vending pre-signed upload URLs to clients via {prefix}/__upload_url__/init. */
  uploadUrlProvider?: UploadUrlProvider;
  /** Optional advertised maximum upload size, surfaced via VGI-Max-Upload-Bytes. */
  maxUploadBytes?: number;
  /** OAuth scope for PKCE authorization requests. Default: "openid email". */
  oauthPkceScope?: string;
  /** Allowed return-to origins for external frontend redirects. Default: Set(["https://cupola.query-farm.services"]). */
  allowedReturnOrigins?: ReadonlySet<string>;
}

/** Serializer for stream state objects stored in state tokens. */
export interface StateSerializer {
  serialize(state: any): Uint8Array;
  deserialize(bytes: Uint8Array): any;
}

/** Default state serializer using JSON (with BigInt support). */
export const jsonStateSerializer: StateSerializer = {
  serialize(state: any): Uint8Array {
    return new TextEncoder().encode(
      JSON.stringify(state, (_key, value) => (typeof value === "bigint" ? `__bigint__:${value}` : value)),
    );
  },
  deserialize(bytes: Uint8Array): any {
    return JSON.parse(new TextDecoder().decode(bytes), (_key, value) =>
      typeof value === "string" && value.startsWith("__bigint__:") ? BigInt(value.slice(11)) : value,
    );
  },
};
