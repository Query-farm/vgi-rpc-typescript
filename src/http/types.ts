// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import type { ExternalLocationConfig, UploadUrlProvider } from "../external.js";
import type { DispatchHook, ServeStartHook } from "../types.js";
import type { AuthenticateFn, OAuthResourceMetadata } from "./auth.js";
import type { TokenResolver } from "./introspect.js";

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
  /** zstd compression level for responses (1-22).
   *
   *  **Defaults to 1 — response compression is on.** Responses are compressed
   *  with the codec the client asked for (see the Compression guide); the
   *  level applies to zstd only, since the Web `CompressionStream` gzip
   *  encoder exposes no level.
   *
   *  Level 1 rather than 3 because on Arrow IPC bodies it is not a speed/size
   *  tradeoff — on an 8.41 MB body level 1 measured 4.7x faster *and* produced
   *  a smaller result. Every VGI SDK defaults to the same level.
   *
   *  Set to `null` to disable response compression entirely: no codec is
   *  advertised (`VGI-Supported-Encodings` goes out present-but-empty) and
   *  bodies travel uncompressed however the client asks. */
  compressionLevel?: number | null;
  /** Optional authentication callback. Called for each request before dispatch. */
  authenticate?: AuthenticateFn;
  /** Advertise `VGI-Proxy-Proof-Required: true` on every response, so a proxy
   *  or operator can confirm this worker rejects unproofed requests.
   *
   *  Advertisement only — it enforces nothing. Set it alongside a
   *  `requireProxyProof` gate built with `mode: "require"`; that gate arrives
   *  as an opaque {@link AuthenticateFn} the handler cannot introspect, so the
   *  posture has to be stated. Default: false. */
  proxyProofRequired?: boolean;
  /** Proxy-injected headers this service's authentication depends on, for a
   *  custom {@link AuthenticateFn} the handler cannot introspect.
   *
   *  Declaring them turns on the 401 proxy note of `docs/unauthorized-spec.md`
   *  §5: `VGI-Auth-Proxy-Required: true` plus a `proxy_hint` explaining that a
   *  rejection here is at least as likely to be a proxy that is not forwarding
   *  the header as a bad credential — the failure mode that otherwise looks
   *  exactly like a rotated credential. A `requireProxyProof` gate declared
   *  through {@link proxyProofRequired} contributes its own header, so this is
   *  only needed on top of that. */
  proxyAuthHeaders?: readonly string[];
  /** Size of the per-process cache of resolved stream calls. The cache is a
   *  pure accelerator — a miss reopens the call token the client echoed — so
   *  `0` disables it and forces every continuation onto the miss path, which
   *  is what the cold-cache conformance group runs against. Default: 4096. */
  callStateCacheEntries?: number;
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
  /** VGI landing surface producer. When set, `GET {prefix}/` serves the shared
   *  vendored `landing.html`, and `GET {prefix}/describe.json` +
   *  `GET {prefix}/describe/{catalog}/{schema}/{table}.json` are served from the
   *  worker's catalog introspection (see {@link LandingDescribeProvider}). This
   *  replaces the generic styled landing page for VGI workers. */
  landingDescribe?: LandingDescribeProvider;
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

  /** Enable opt-in sticky sessions on this HTTP handler. When enabled the
   *  server advertises `VGI-Sticky-Enabled: true` (capability discovery),
   *  honours `VGI-Session` / `VGI-Session-Accept` headers, and exposes a
   *  `DELETE {prefix}/__session__` teardown endpoint. Default: false. */
  enableSticky?: boolean;
  /** Default session TTL in seconds when `ctx.openSession` is called without
   *  an explicit `ttl` override. Default: 300. */
  stickyDefaultTtl?: number;
  /** Headers the server emits as `VGI-Echo-<name>: <value>` on the
   *  session-opening response. A conformant client captures them and replays
   *  them on every subsequent request in the session — used for
   *  client-driven routing (e.g. `fly-force-instance-id` on Fly.io). */
  stickyEchoHeaders?: Record<string, string>;
  /** Enables `POST {prefix}/__introspect_token__`, which resolves an opaque
   *  bearer credential to a principal for a reverse proxy that must know the
   *  caller's identity before it can authorize.
   *
   *  Omitted (the default) leaves the endpoint **disabled** — it answers a
   *  definitive `404 {"error": "not_enabled"}` and holds no resolver — so no
   *  worker grows a credential-to-identity oracle by upgrading a dependency.
   *
   *  The callable returns a `TokenIdentity` or `null`, and throws
   *  `AuthUnavailableError` when the answer is not knowable, which a caller
   *  must retry rather than cache. It never returns claims; see
   *  `src/http/introspect.ts` for why. */
  introspectResolver?: TokenResolver;
  /** Principals permitted to introspect. Required whenever
   *  {@link introspectResolver} is set, with **no permissive default**:
   *  authentication and introspection are different capabilities, and a
   *  deployment where any valid credential may introspect lets any user
   *  resolve any other user's credential to its owner. */
  introspectPrincipals?: Iterable<string>;
  /** Cache window advertised as `ttl_seconds` when a resolved `TokenIdentity`
   *  names none. Default: 300. */
  introspectTtlSeconds?: number;
  /** Introspection requests allowed per caller per second (default 20). Bounds,
   *  rather than closes, the oracle an allowlisted-but-compromised caller still
   *  has. */
  introspectRateLimit?: number;

  /** Internal — invoked once at handler creation with a {@link DrainHandle}
   *  when sticky is enabled. Conformance fixtures use this to wire up the
   *  test-only `/__test_drain__` admin endpoint without the library
   *  exposing the registry directly. Production code should hold the
   *  handle returned by a future `createHttpHandlerWithDrainHandle` helper. */
  _onStickyHandle?: (handle: import("./sticky.js").DrainHandle) => void;
}

/**
 * Producer for the standardized VGI landing surface (see
 * `vgi/docs/http-landing-contract.md`). A VGI worker supplies this so the
 * catalog-agnostic HTTP handler can serve `GET {prefix}/describe.json` and the
 * lazy per-object column endpoint from the worker's own catalog introspection.
 *
 * When set, `GET {prefix}/` serves the shared vendored `landing.html` instead of
 * the generic styled endpoint page, and the describe JSON routes become active.
 */
export interface LandingDescribeProvider {
  /**
   * Build the full `describe.json` contract document. The handler passes the
   * runtime-derived `serverId` and whether OAuth/PKCE is active; the provider
   * fills in `worker`, `catalogs`, etc. Return value is JSON-serialized as-is.
   */
  describe(ctx: { serverId: string; oauth: boolean }): unknown | Promise<unknown>;
  /**
   * Lazy per-object columns for
   * `GET {prefix}/describe/{catalog}/{schema}/{table}.json`. Return `null` when
   * the object is not found (the handler responds 404).
   */
  columns(catalog: string, schema: string, table: string): unknown | null | Promise<unknown | null>;
}

/** Serializer for stream state objects stored in state tokens. */
export interface StateSerializer {
  /** Encode a stream-state object into the bytes sealed inside a state token. */
  serialize(state: any): Uint8Array;
  /** Decode the bytes recovered from a state token back into a state object. */
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
