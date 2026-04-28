// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import type { ExternalLocationConfig, UploadUrlProvider } from "../external.js";
import type { DispatchHook } from "../types.js";
import type { AuthenticateFn, OAuthResourceMetadata } from "./auth.js";

/** Configuration options for createHttpHandler(). */
export interface HttpHandlerOptions {
  /** URL path prefix for all endpoints. Default: "" (root). */
  prefix?: string;
  /** HMAC-SHA256 signing key for state tokens. Random 32 bytes if omitted. */
  signingKey?: Uint8Array;
  /** State token time-to-live in seconds. Default: 3600 (1 hour). 0 disables TTL checks. */
  tokenTtl?: number;
  /** CORS allowed origins. If set, CORS headers are added to all responses. */
  corsOrigins?: string;
  /** Access-Control-Max-Age value in seconds for preflight OPTIONS responses. Default: 7200 (2 hours). null omits the header. */
  corsMaxAge?: number | null;
  /** Maximum request body size in bytes. Advertised via VGI-Max-Request-Bytes header. */
  maxRequestBytes?: number;
  /** Maximum bytes before a producer stream emits a continuation token. */
  maxStreamResponseBytes?: number;
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
