// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import {
  batchFromColumns,
  deserializeBatch,
  field,
  schema as makeSchema,
  timestampMicro,
  utf8,
  type VgiSchema,
} from "../arrow/index.js";
import type { AuthContext } from "../auth.js";
import { DESCRIBE_METHOD_NAME, PROTOCOL_HASH_KEY, PROTOCOL_VERSION_KEY, RPC_ERROR_HEADER } from "../constants.js";
import { buildDescribeBatch } from "../dispatch/describe.js";
import { MethodNotImplementedError, ProtocolVersionError, parseProtocolVersion, SessionLostError } from "../errors.js";
import type { Protocol } from "../protocol.js";
import { type CallStatistics, type DispatchInfo, MethodType, type ServeStartHook, TransportKind } from "../types.js";
import { gzipCompress, gzipDecompress } from "../util/gzip.js";
import { randomBytes } from "../util/web-crypto.js";
import { isZstdCompressAvailable, zstdCompress, zstdDecompress } from "../util/zstd.js";
import { parseRequest } from "../wire/request.js";
import { buildErrorBatch } from "../wire/response.js";
import { buildWwwAuthenticateHeader, oauthResourceMetadataToJson, wellKnownPath } from "./auth.js";
import { chainAuthenticate } from "./bearer.js";
import {
  ARROW_CONTENT_TYPE,
  arrowResponse,
  ECHO_HEADER_PREFIX,
  HttpRpcError,
  readRequestFromBody as readRequestFromBodyImported,
  SESSION_ACCEPT_HEADER,
  SESSION_CLOSE_HEADER,
  SESSION_ENDPOINT,
  SESSION_HEADER,
  STICKY_DEFAULT_TTL_HEADER,
  STICKY_ECHO_HEADERS_HEADER,
  STICKY_ENABLED_HEADER,
  serializeIpcStream,
} from "./common.js";
import {
  httpDispatchDescribe,
  httpDispatchStreamExchange,
  httpDispatchStreamInit,
  httpDispatchUnary,
} from "./dispatch.js";
import {
  configureOAuthPkce,
  handleBrowserGetRedirect,
  handleEarlyReturnTo,
  handleOAuthCallback,
  handleOAuthLogout,
  handleOAuthTokenProxy,
  type OAuthPkceConfig,
  resolvePkceScope,
} from "./oauth-pkce.js";
import { buildDescribePage, buildLandingPage, buildNotFoundPage } from "./pages.js";
import {
  makeDrainHandle,
  openSessionToken,
  SessionRegistry,
  type StickySink,
  sealSessionToken,
  sessionIdHex,
  sessionPrincipalKey,
  startSessionReaper,
} from "./sticky.js";
import { computeAad } from "./token.js";
import { type HttpHandlerOptions, jsonStateSerializer } from "./types.js";

const EMPTY_SCHEMA: VgiSchema = makeSchema([]);

const EMPTY_COOKIES: ReadonlyMap<string, string> = new Map();

/**
 * Parse the Cookie request header into a Map.  Returns an empty map when
 * the header is absent.
 */
function parseRequestCookies(request: Request): ReadonlyMap<string, string> {
  const header = request.headers.get("cookie");
  if (!header) return EMPTY_COOKIES;
  const out = new Map<string, string>();
  for (const part of header.split(";")) {
    const pair = part.trim();
    if (!pair) continue;
    const eq = pair.indexOf("=");
    if (eq < 0) continue;
    out.set(pair.slice(0, eq).trim(), pair.slice(eq + 1).trim());
  }
  return out;
}

/**
 * Create a fetch-compatible HTTP handler for a vgi-rpc Protocol.
 *
 * Compatible with Bun.serve(), Deno.serve(), Cloudflare Workers, and any
 * Web API runtime that uses the standard Request/Response types.
 *
 * @example
 * ```typescript
 * const handler = createHttpHandler(protocol);
 * Bun.serve({ port: 8080, fetch: handler });
 * ```
 */
export function createHttpHandler(
  protocol: Protocol,
  options?: HttpHandlerOptions,
): (request: Request) => Response | Promise<Response> {
  const prefix = (options?.prefix ?? "").replace(/\/+$/, "");
  const tokenKey = options?.tokenKey ?? randomBytes(32);
  const tokenTtl = options?.tokenTtl ?? 3600;
  const corsOrigins = options?.corsOrigins;
  const corsMaxAge = options?.corsMaxAge === undefined ? 7200 : options.corsMaxAge;
  const maxRequestBytes = options?.maxRequestBytes;
  // Bomb-cap on `Content-Encoding: zstd` decompression. Default to
  // 16x maxRequestBytes when the operator set one — generous for normal
  // Arrow IPC zstd ratios on legitimate payloads, tight enough that a
  // tiny compressed body cannot inflate to hundreds of MB.  When
  // maxRequestBytes is unset the cap stays unbounded (operator-chosen,
  // explicit).  Mirrors Python's make_wsgi_app default.
  const maxDecompressedRequestBytes =
    options?.maxDecompressedRequestBytes ??
    (options?.maxRequestBytes != null ? options.maxRequestBytes * 16 : undefined);
  // ``maxStreamResponseBytes`` was the producer-only soft cap. Keep it
  // distinct from ``maxResponseBytes`` (the new hard cap that also applies
  // to unary/exchange) — falling one through to the other would turn the
  // producer hack into an unintended hard cap on every response.
  const maxStreamResponseBytes = options?.maxStreamResponseBytes;
  const maxResponseBytes = options?.maxResponseBytes;
  const maxExternalizedResponseBytes = options?.maxExternalizedResponseBytes;
  const serverId = options?.serverId ?? crypto.randomUUID().replace(/-/g, "").slice(0, 12);

  let authenticate = options?.authenticate;
  const oauthMetadata = options?.oauthResourceMetadata;

  // PKCE setup: when both authenticate and oauthMetadata.clientId are present
  let pkceConfig: OAuthPkceConfig | null = null;
  if (authenticate && oauthMetadata?.clientId) {
    const resourceUrl = new URL(oauthMetadata.resource);
    const secureCookie = resourceUrl.protocol === "https:";
    const redirectUri = `${oauthMetadata.resource.replace(/\/+$/, "")}${prefix}/_oauth/callback`;
    const issuer = oauthMetadata.authorizationServers[0];
    if (issuer) {
      const originalAuth = authenticate;
      pkceConfig = configureOAuthPkce(
        {
          signingKey: tokenKey,
          issuer,
          clientId: oauthMetadata.clientId,
          clientSecret: oauthMetadata.clientSecret,
          useIdToken: oauthMetadata.useIdTokenAsBearer,
          prefix,
          secureCookie,
          redirectUri,
          scope: resolvePkceScope(oauthMetadata.scopesSupported, options?.oauthPkceScope),
          allowedReturnOrigins: options?.allowedReturnOrigins,
        },
        originalAuth,
      );
      authenticate = chainAuthenticate(originalAuth, pkceConfig.cookieAuthenticate);
    }
  }

  const methods = protocol.getMethods();

  // Lazily compute the protocol hash once; it's the SHA-256 over the
  // canonical __describe__ payload and is derived from buildDescribeBatch's
  // metadata.  Async because Web Crypto digests are async.  Used to stamp
  // every dispatched access-log record with `protocol_hash`.
  let protocolHashPromise: Promise<string> | null = null;
  function getProtocolHash(): Promise<string> {
    if (!protocolHashPromise) {
      protocolHashPromise = buildDescribeBatch(
        protocol.name,
        methods,
        serverId,
        protocol.protocolVersion || undefined,
      ).then(({ metadata }) => metadata.get(PROTOCOL_HASH_KEY) ?? "");
    }
    return protocolHashPromise;
  }
  const protocolVersion = protocol.protocolVersion || options?.protocolVersion || "";

  // Dispatch-boundary protocol_version check, fires only when the Protocol
  // declares a `protocolVersion`. Mirrors Python's HTTP _app_unary /
  // _app_stream gate added after the dispatch-loop bypass was caught in
  // review. Throws ProtocolVersionError so the existing catch turns it into
  // a buffered error stream rather than a raw HTTP 500.
  function enforceProtocolVersion(reqBatchMeta: ReadonlyMap<string, string> | undefined): void {
    const parts = protocol.protocolVersionParts;
    if (parts === null) return;
    const serverVersion = protocol.protocolVersion;
    const clientVersion = reqBatchMeta?.get(PROTOCOL_VERSION_KEY);
    if (clientVersion === undefined) {
      throw new ProtocolVersionError(
        "VGI client/worker protocol_version mismatch.\n" +
          "  Client: <not declared>\n" +
          `  Server: ${serverVersion}\n` +
          "  Direction: the client did not send a vgi_rpc.protocol_version " +
          "metadata key. This is either a vgi-rpc framework bug or a " +
          "non-VGI client connecting to a VGI worker.",
      );
    }
    let clientParts: readonly [number, number, number];
    try {
      clientParts = parseProtocolVersion(clientVersion);
    } catch {
      throw new ProtocolVersionError(
        "VGI client/worker protocol_version mismatch.\n" +
          `  Client: ${clientVersion}\n` +
          `  Server: ${serverVersion}\n` +
          "  Direction: client sent a malformed protocol_version. " +
          "Expected canonical semver MAJOR.MINOR.PATCH.",
      );
    }
    if (clientParts[0] === parts[0] && clientParts[1] === parts[1]) return;
    const clientOlder = clientParts[0] < parts[0] || (clientParts[0] === parts[0] && clientParts[1] < parts[1]);
    const direction = clientOlder
      ? `client is too old; upgrade the VGI extension/client to a version supporting protocol_version ${serverVersion}.`
      : `server is too old; upgrade the VGI worker to a version supporting protocol_version ${clientVersion}.`;
    throw new ProtocolVersionError(
      "VGI client/worker protocol_version mismatch.\n" +
        `  Client: ${clientVersion}\n` +
        `  Server: ${serverVersion}\n` +
        `  Direction: ${direction}`,
    );
  }

  const compressionLevel = options?.compressionLevel;
  const stateSerializer = options?.stateSerializer ?? jsonStateSerializer;
  const dispatchHook = options?.dispatchHook;

  // Lazy on_serve_start firing — mirrors Python's middleware shape.
  // The bind is committed only after the hook returns successfully so a
  // transient failure on the first request leaves it un-fired and the
  // next request retries (matches Python 7b3999c).  Multiple
  // simultaneous first-callers serialize on the in-flight promise.
  const onServeStart: ServeStartHook | null = options?.onServeStart ?? null;
  let serveStartFired = false;
  let serveStartInFlight: Promise<void> | null = null;
  // The transport kind reported to access-log + dispatch hooks. Default
  // to HTTP; the launcher path (createUnixHandler) overrides this in a
  // future commit.
  const transportKind: TransportKind =
    (options as { _transportKind?: TransportKind })?._transportKind ?? TransportKind.HTTP;
  async function notifyTransport(kind: TransportKind): Promise<void> {
    if (serveStartFired) return;
    if (serveStartInFlight) {
      await serveStartInFlight;
      return;
    }
    if (!onServeStart) {
      serveStartFired = true;
      return;
    }
    serveStartInFlight = (async () => {
      try {
        await onServeStart(kind);
        serveStartFired = true;
      } finally {
        serveStartInFlight = null;
      }
    })();
    await serveStartInFlight;
  }

  // HTML page configuration
  const enableLandingPage = options?.enableLandingPage ?? true;
  const enableDescribePage = options?.enableDescribePage ?? true;
  const enableNotFoundPage = options?.enableNotFoundPage ?? true;
  const displayName = options?.protocolName ?? protocol.name;
  const repoUrl = options?.repositoryUrl ?? null;

  // Pre-render HTML pages for zero per-request overhead
  let landingHtml = enableLandingPage
    ? buildLandingPage(displayName, serverId, enableDescribePage ? `${prefix}/describe` : null, repoUrl)
    : null;
  let describeHtml = enableDescribePage ? buildDescribePage(displayName, serverId, methods, repoUrl) : null;
  const notFoundHtml = enableNotFoundPage ? buildNotFoundPage(prefix, displayName) : null;

  // Inject user-info HTML snippet when PKCE is active
  if (pkceConfig) {
    const snippet = pkceConfig.userInfoHtml;
    if (landingHtml) {
      landingHtml = landingHtml.replace("</body>", `${snippet}\n</body>`);
    }
    if (describeHtml) {
      describeHtml = describeHtml.replace("</body>", `${snippet}\n</body>`);
    }
  }

  const externalLocation = options?.externalLocation;
  const uploadUrlProvider = options?.uploadUrlProvider;
  const maxUploadBytes = options?.maxUploadBytes;

  // Pre-built response schema for the synthetic __upload_url__ endpoint.
  const UPLOAD_URL_RESPONSE_SCHEMA = makeSchema([
    field("upload_url", utf8(), false),
    field("download_url", utf8(), false),
    field("expires_at", timestampMicro("UTC"), false),
  ]);
  const UPLOAD_URL_METHOD = "__upload_url__";
  const MAX_UPLOAD_URL_COUNT = 100;

  // -------- Sticky session machinery --------
  const stickyEnabled = options?.enableSticky === true;
  const stickyDefaultTtl = options?.stickyDefaultTtl ?? 300;
  // Frozen snapshot so per-response emission doesn't re-read a mutable
  // operator dict mid-request.
  const stickyEchoHeadersArr: Array<[string, string]> = stickyEnabled
    ? Object.entries(options?.stickyEchoHeaders ?? {})
    : [];
  const sessionRegistry = stickyEnabled ? new SessionRegistry(stickyDefaultTtl) : null;
  // Reaper interval is unref'd (won't block process exit) but is still a live
  // resource — `DrainHandle.shutdown()` clears it via this stop fn so callers
  // that drain explicitly (tests, worker-exit hooks) don't leak the interval.
  const stopReaper = sessionRegistry ? startSessionReaper(sessionRegistry) : null;
  if (options?._onStickyHandle && sessionRegistry) {
    options._onStickyHandle(makeDrainHandle(sessionRegistry, stopReaper ?? undefined));
  }

  // Encodings the server can produce on the response side. Mirrors
  // Python's `VGI-Supported-Encodings` header from `_codec.py`.
  // `compressionLevel` gates response compression overall; zstd is only
  // advertised when the runtime can actually encode it (Bun, Node ≥22.15,
  // Deno ≥2.6.9 — workerd lacks an encoder). gzip is always available via
  // Web `CompressionStream`.
  const supportedResponseEncodings: string[] = [];
  const zstdResponseAvailable = compressionLevel != null && isZstdCompressAvailable();
  if (zstdResponseAvailable) {
    supportedResponseEncodings.push("zstd");
  }
  if (compressionLevel != null) {
    supportedResponseEncodings.push("gzip");
  }

  /** Append capability headers (advertised on every response when configured). */
  function addCapabilityHeaders(headers: Headers, isOptions = false): void {
    if (supportedResponseEncodings.length) {
      headers.set("VGI-Supported-Encodings", supportedResponseEncodings.join(", "));
    }
    if (maxRequestBytes != null) {
      headers.set("VGI-Max-Request-Bytes", String(maxRequestBytes));
    }
    if (maxResponseBytes != null) {
      headers.set("VGI-Max-Response-Bytes", String(maxResponseBytes));
    }
    if (maxExternalizedResponseBytes != null) {
      headers.set("VGI-Max-Externalized-Response-Bytes", String(maxExternalizedResponseBytes));
    }
    // Always present so capability-aware clients can decide whether to
    // expect externalised payloads.
    headers.set("VGI-Externalization-Enabled", externalLocation?.storage ? "true" : "false");
    if (uploadUrlProvider) {
      headers.set("VGI-Upload-URL-Support", "true");
      if (maxUploadBytes != null) {
        headers.set("VGI-Max-Upload-Bytes", String(maxUploadBytes));
      }
    }
    if (stickyEnabled) {
      headers.set(STICKY_ENABLED_HEADER, "true");
      headers.set(STICKY_DEFAULT_TTL_HEADER, String(Math.floor(stickyDefaultTtl)));
      if (stickyEchoHeadersArr.length > 0) {
        headers.set(STICKY_ECHO_HEADERS_HEADER, stickyEchoHeadersArr.map(([k]) => k).join(", "));
      }
    }
    if (isOptions && (maxRequestBytes != null || uploadUrlProvider || stickyEnabled)) {
      // Match Python: cache discovery results for 5 minutes.
      if (!headers.has("Cache-Control")) {
        headers.set("Cache-Control", "public, max-age=300");
      }
    }
  }

  // ctx is built per-request to include authContext; base fields set here
  const baseCtx = {
    tokenKey,
    tokenTtl,
    serverId,
    maxStreamResponseBytes,
    maxResponseBytes,
    maxExternalizedResponseBytes,
    stateSerializer,
    externalLocation,
    kind: transportKind,
  };

  function addCorsHeaders(headers: Headers, isOptions = false): void {
    if (corsOrigins) {
      headers.set("Access-Control-Allow-Origin", corsOrigins);
      headers.set("Access-Control-Allow-Methods", "POST, OPTIONS");
      headers.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
      headers.set(
        "Access-Control-Expose-Headers",
        `WWW-Authenticate, X-Request-ID, X-VGI-Content-Encoding, ${RPC_ERROR_HEADER}, VGI-Max-Response-Bytes, VGI-Max-Externalized-Response-Bytes, VGI-Externalization-Enabled`,
      );
      if (isOptions && corsMaxAge != null) {
        headers.set("Access-Control-Max-Age", String(corsMaxAge));
      }
    }
  }

  async function compressIfAccepted(
    response: Response,
    clientAcceptsZstd: boolean,
    clientAcceptsGzip: boolean,
  ): Promise<Response> {
    if (compressionLevel == null) return response;
    // Honour client preference: zstd preferred over gzip when the runtime
    // can actually produce zstd. Fall through to gzip otherwise.
    const codec = clientAcceptsZstd && zstdResponseAvailable ? "zstd" : clientAcceptsGzip ? "gzip" : null;
    if (!codec) return response;
    const responseBody = new Uint8Array(await response.arrayBuffer());
    const compressed =
      codec === "zstd" ? await zstdCompress(responseBody, compressionLevel) : await gzipCompress(responseBody);
    const headers = new Headers(response.headers);
    headers.set("Content-Encoding", codec);
    return new Response(compressed as unknown as BodyInit, {
      status: response.status,
      headers,
    });
  }

  function makeErrorResponse(error: Error, statusCode: number, schema: VgiSchema = EMPTY_SCHEMA): Response {
    const errBatch = buildErrorBatch(schema, error, serverId, null);
    const body = serializeIpcStream(schema, [errBatch]);
    const resp = arrowResponse(body, statusCode);
    addCorsHeaders(resp.headers);
    return resp;
  }

  const enableHealthEndpoint = options?.enableHealthEndpoint ?? true;
  const healthPath = `${prefix}/health`;
  const healthBody = enableHealthEndpoint
    ? JSON.stringify({ status: "ok", server_id: serverId, protocol: displayName })
    : null;

  return async function handler(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    // OAuth token-exchange proxy — exempt from auth (it's the mechanism by
    // which a client *gets* an auth token). Handled before the global OPTIONS
    // catch-all so the proxy can apply its own Origin-allowlist CORS.
    if (
      pkceConfig &&
      path === `${prefix}/_oauth/token` &&
      (request.method === "POST" || request.method === "OPTIONS")
    ) {
      return handleOAuthTokenProxy(request, pkceConfig);
    }

    // Health endpoint — exempt from authentication so orchestrators / load
    // balancers can probe even when every RPC endpoint requires auth.
    if (healthBody !== null && request.method === "GET" && path === healthPath) {
      const headers = new Headers({ "Content-Type": "application/json" });
      addCorsHeaders(headers);
      addCapabilityHeaders(headers);
      return new Response(healthBody, { status: 200, headers });
    }

    // Well-known endpoint: RFC 9728 OAuth Protected Resource Metadata
    if (oauthMetadata && path === wellKnownPath(prefix)) {
      if (request.method !== "GET") {
        return new Response("Method Not Allowed", { status: 405 });
      }
      const metaJson = oauthResourceMetadataToJson(oauthMetadata);
      // When PKCE + a server-side client_secret are configured, advertise the
      // token-proxy URL so SPA PKCE clients can complete token exchanges
      // without holding the secret themselves.
      if (pkceConfig && oauthMetadata.clientSecret) {
        const resourceUrl = new URL(oauthMetadata.resource);
        metaJson.token_endpoint = `${resourceUrl.protocol}//${resourceUrl.host}${prefix}/_oauth/token`;
      }
      const body = JSON.stringify(metaJson);
      const headers = new Headers({
        "Content-Type": "application/json",
        "Cache-Control": "public, max-age=60",
      });
      addCorsHeaders(headers);
      return new Response(body, { status: 200, headers });
    }

    // CORS preflight + capability discovery
    if (request.method === "OPTIONS") {
      const headers = new Headers();
      addCorsHeaders(headers, true);
      addCapabilityHeaders(headers, true);
      // Always answer OPTIONS so capability discovery via OPTIONS /health (or
      // any other path) works even when CORS isn't enabled.  Falls back to
      // 405 only if no capability/CORS configuration exists.
      if (
        corsOrigins ||
        maxRequestBytes != null ||
        maxResponseBytes != null ||
        maxExternalizedResponseBytes != null ||
        uploadUrlProvider ||
        stickyEnabled ||
        path === `${prefix}/__capabilities__`
      ) {
        return new Response(null, { status: 204, headers });
      }
      return new Response(null, { status: 405 });
    }

    // HTML pages for GET requests
    if (request.method === "GET") {
      // OAuth callback and logout routes (exempt from auth)
      if (pkceConfig) {
        if (path === `${prefix}/_oauth/callback`) {
          return handleOAuthCallback(request, pkceConfig);
        }
        if (path === `${prefix}/_oauth/logout`) {
          return handleOAuthLogout(request, pkceConfig);
        }

        // Early return-to redirect for already-authenticated users
        const earlyRedirect = handleEarlyReturnTo(request, pkceConfig);
        if (earlyRedirect) return earlyRedirect;
      }

      // If authenticate is configured, try to authenticate GET requests for pages
      // On auth failure with PKCE, redirect browsers to OAuth instead of 401
      if (authenticate && pkceConfig) {
        try {
          await authenticate(request);
        } catch {
          // Auth failed — redirect browser GETs to OAuth authorization
          const redirect = await handleBrowserGetRedirect(request, pkceConfig);
          if (redirect) return redirect;
          // Not a browser or OIDC discovery failed — fall through to normal page serving
        }
      }

      // Landing page: GET {prefix}/ or GET {prefix}
      if (landingHtml && (path === prefix || path === `${prefix}/`)) {
        const headers = new Headers({ "Content-Type": "text/html; charset=utf-8" });
        addCorsHeaders(headers);
        return new Response(landingHtml, { status: 200, headers });
      }

      // Describe page: GET {prefix}/describe
      if (describeHtml && path === `${prefix}/describe`) {
        const headers = new Headers({ "Content-Type": "text/html; charset=utf-8" });
        addCorsHeaders(headers);
        return new Response(describeHtml, { status: 200, headers });
      }

      // 404 page for any other GET
      if (notFoundHtml) {
        const headers = new Headers({ "Content-Type": "text/html; charset=utf-8" });
        addCorsHeaders(headers);
        return new Response(notFoundHtml, { status: 404, headers });
      }

      return new Response("Not Found", { status: 404 });
    }

    // DELETE {prefix}/__session__ — idempotent sticky-session teardown.
    // Mirrors Python's `_SessionResource.on_delete`: missing token /
    // malformed / wrong server_id / wrong principal / registry miss all
    // return 200 (so the endpoint cannot be used to probe for live
    // sessions); a successful close returns 204 + VGI-Session-Close: true.
    if (request.method === "DELETE" && stickyEnabled && sessionRegistry && path === `${prefix}/${SESSION_ENDPOINT}`) {
      const headers = new Headers();
      addCorsHeaders(headers);
      addCapabilityHeaders(headers);
      const tokenHeader = (request.headers.get(SESSION_HEADER) ?? "").trim();
      if (!tokenHeader) {
        return new Response(null, { status: 200, headers });
      }
      // Optional auth — re-uses the same authenticate path so principal
      // binding is consistent with the dispatch flow. AAD uses only the
      // authenticated principal (matching `computeAad` in `token.ts`);
      // the registry's principalKey compounds domain+principal as
      // defense-in-depth.
      let principalKey = sessionPrincipalKey(false, null, null);
      let aadPrincipal: string | null = null;
      if (authenticate) {
        try {
          const auth = await authenticate(request);
          if (auth?.authenticated) {
            aadPrincipal = auth.principal ?? "";
            principalKey = sessionPrincipalKey(true, auth.domain, auth.principal);
          }
        } catch {
          // Anonymous principal — stale / forged tokens already won't
          // decrypt under a real principal's AAD, so the auth failure
          // here is harmless; treat as anonymous and let the next steps
          // 200 out idempotently.
        }
      }
      const aad = computeAad(aadPrincipal);
      let opened: { serverId: string; sessionId: Uint8Array };
      try {
        opened = openSessionToken(tokenHeader, tokenKey, aad);
      } catch {
        return new Response(null, { status: 200, headers });
      }
      if (opened.serverId !== serverId) {
        return new Response(null, { status: 200, headers });
      }
      const entry = sessionRegistry.get(opened.sessionId, principalKey);
      if (!entry) {
        return new Response(null, { status: 200, headers });
      }
      const release = await entry.lock.acquire();
      try {
        sessionRegistry.close(opened.sessionId);
      } finally {
        release();
      }
      headers.set(SESSION_CLOSE_HEADER, "true");
      return new Response(null, { status: 204, headers });
    }

    if (request.method !== "POST") {
      return new Response("Method Not Allowed", { status: 405 });
    }

    // Build per-request dispatch context
    const ctx = { ...baseCtx, cookies: parseRequestCookies(request) } as typeof baseCtx & {
      authContext?: AuthContext;
      cookies: ReadonlyMap<string, string>;
      stickyContext?: StickySink;
    };

    // Authentication — run before content-type validation so unauthenticated
    // requests get 401 regardless of body shape.
    if (authenticate) {
      try {
        ctx.authContext = await authenticate(request);
      } catch (error: any) {
        const headers = new Headers({ "Content-Type": "text/plain" });
        addCorsHeaders(headers);
        if (oauthMetadata) {
          const metadataUrl = new URL(request.url);
          metadataUrl.pathname = wellKnownPath(prefix);
          metadataUrl.search = "";
          headers.set(
            "WWW-Authenticate",
            buildWwwAuthenticateHeader(
              metadataUrl.toString(),
              oauthMetadata.clientId,
              oauthMetadata.clientSecret,
              oauthMetadata.useIdTokenAsBearer,
              oauthMetadata.deviceCodeClientId,
              oauthMetadata.deviceCodeClientSecret,
            ),
          );
        }
        return new Response(error.message || "Unauthorized", { status: 401, headers });
      }
    }

    // Hoisted ahead of sticky resolution so the SessionLost path's
    // `compressIfAccepted` call can see them.
    const acceptEncodingEarly = (request.headers.get("Accept-Encoding") ?? "").toLowerCase();
    const clientAcceptsZstdEarly = acceptEncodingEarly.includes("zstd");
    const clientAcceptsGzipEarly = acceptEncodingEarly.includes("gzip");

    // -------- Sticky session resolution --------
    // Mirrors `_StickyMiddleware.process_request` in Python: read
    // VGI-Session-Accept + VGI-Session, decrypt the token (AAD-bound to
    // the request's principal), look up the registry entry, and acquire
    // the per-session lock so concurrent calls on the same session
    // serialize. On any failure we surface a typed SessionLostError as
    // a 500 + EXCEPTION-batch response (the same wire shape a
    // dispatch-time throw would produce).
    let stickyLockRelease: (() => void) | null = null;
    let stickySink: StickySink | null = null;
    if (stickyEnabled && sessionRegistry) {
      const auth = ctx.authContext;
      const aadPrincipal = auth?.authenticated ? (auth.principal ?? "") : null;
      const principalKey = sessionPrincipalKey(!!auth?.authenticated, auth?.domain, auth?.principal);
      const aad = computeAad(aadPrincipal);
      const acceptOpens = (request.headers.get(SESSION_ACCEPT_HEADER) ?? "").trim().toLowerCase() === "true";
      const sessionHeader = (request.headers.get(SESSION_HEADER) ?? "").trim();

      let resumeState: unknown | null = null;
      let resumeSessionId: string | null = null;

      if (sessionHeader) {
        let opened: { serverId: string; sessionId: Uint8Array };
        try {
          opened = openSessionToken(sessionHeader, tokenKey, aad);
          if (opened.serverId !== serverId) {
            throw new SessionLostError("session token was issued by a different worker (server_id mismatch)");
          }
        } catch (err) {
          // Wrong-AAD / wrong-key / malformed → SessionLostError.
          const e = err instanceof Error ? err : new Error(String(err));
          const r = makeErrorResponse(e, 500);
          addCapabilityHeaders(r.headers);
          return compressIfAccepted(r, clientAcceptsZstdEarly, clientAcceptsGzipEarly);
        }
        const entry = sessionRegistry.get(opened.sessionId, principalKey);
        if (!entry) {
          const r = makeErrorResponse(new SessionLostError("session not found, expired, or principal mismatch"), 500);
          addCapabilityHeaders(r.headers);
          return compressIfAccepted(r, clientAcceptsZstdEarly, clientAcceptsGzipEarly);
        }
        stickyLockRelease = await entry.lock.acquire();
        resumeState = entry.state;
        resumeSessionId = sessionIdHex(opened.sessionId);
      }

      // Build the sink. Captures `principalKey` and `aad` so `_open`
      // can register a new entry and mint a token bound to the same
      // principal as the request.
      const sink: StickySink = {
        acceptOpens,
        state: resumeState,
        sessionId: resumeSessionId,
        mintToken: null,
        closed: false,
        action: sessionHeader ? "resume" : "none",
        _open(state: unknown, ttl?: number) {
          const { sessionId, expiresAt } = sessionRegistry!.open(state, ttl, principalKey);
          sink.sessionId = sessionIdHex(sessionId);
          sink.state = state;
          sink.mintToken = sealSessionToken(serverId, sessionId, expiresAt, tokenKey, aad);
        },
        _close() {
          if (sink.closed) return;
          const sid = sink.sessionId;
          if (!sid) return;
          // Decode hex back to bytes for registry lookup.
          const bytes = new Uint8Array(sid.length / 2);
          for (let i = 0; i < bytes.length; i++) bytes[i] = parseInt(sid.slice(i * 2, i * 2 + 2), 16);
          // Release the lock before close so the resource isn't held
          // while close() runs.
          if (stickyLockRelease) {
            stickyLockRelease();
            stickyLockRelease = null;
          }
          sessionRegistry!.close(bytes);
          sink.state = null;
          sink.closed = true;
        },
      };
      stickySink = sink;
      ctx.stickyContext = sink;
    }

    // Validate Content-Type
    const contentType = request.headers.get("Content-Type");
    if (!contentType || !contentType.includes(ARROW_CONTENT_TYPE)) {
      if (stickyLockRelease) stickyLockRelease();
      return new Response(`Unsupported Media Type: expected ${ARROW_CONTENT_TYPE}`, { status: 415 });
    }

    // Check request body size (exempt the upload-URL and health endpoints —
    // their payloads are intrinsically tiny, and __upload_url__ exists
    // precisely to escape this limit).
    const exemptFromMaxBytes =
      path === healthPath || path === `${prefix}/${UPLOAD_URL_METHOD}/init` || path === `${prefix}/__capabilities__`;
    if (maxRequestBytes != null && !exemptFromMaxBytes) {
      const contentLength = request.headers.get("Content-Length");
      if (contentLength && parseInt(contentLength, 10) > maxRequestBytes) {
        return new Response("Request body too large", { status: 413 });
      }
    }

    const clientAcceptsZstd = clientAcceptsZstdEarly;
    const clientAcceptsGzip = clientAcceptsGzipEarly;

    // Read body, decompressing if needed
    let body = new Uint8Array(await request.arrayBuffer());
    if (maxRequestBytes != null && !exemptFromMaxBytes && body.byteLength > maxRequestBytes) {
      return new Response("Request body too large", { status: 413 });
    }
    const contentEncoding = (request.headers.get("Content-Encoding") ?? "").trim().toLowerCase();
    if (contentEncoding === "zstd" || contentEncoding === "gzip") {
      try {
        body =
          contentEncoding === "zstd"
            ? await zstdDecompress(body, maxDecompressedRequestBytes)
            : await gzipDecompress(body, maxDecompressedRequestBytes);
      } catch (error: any) {
        // Decompression-bomb refusal surfaces as 413 (the wire-cap
        // sibling of maxRequestBytes); other decode errors are 400.
        const message = error?.message ?? `${contentEncoding} decompression failed`;
        const status = message.includes("exceed") || message.includes("cap") ? 413 : 400;
        const headers = new Headers({ "Content-Type": "text/plain" });
        addCorsHeaders(headers);
        addCapabilityHeaders(headers);
        return new Response(message, { status, headers });
      }
    } else if (contentEncoding) {
      const headers = new Headers({ "Content-Type": "text/plain" });
      addCorsHeaders(headers);
      addCapabilityHeaders(headers);
      return new Response(`Unsupported Content-Encoding: ${contentEncoding}`, { status: 415, headers });
    }

    // Route: {prefix}/__upload_url__/init — vend pre-signed upload URL pairs
    if (path === `${prefix}/${UPLOAD_URL_METHOD}/init`) {
      if (!uploadUrlProvider) {
        return new Response("Not Found", { status: 404 });
      }
      try {
        const { schema: reqSchema, batch: reqBatch } = await readRequestFromBodyImported(body);
        const parsed = parseRequest(reqSchema, reqBatch);
        if (parsed.methodName !== UPLOAD_URL_METHOD) {
          throw new HttpRpcError(
            `Method name in request '${parsed.methodName}' does not match URL '${UPLOAD_URL_METHOD}'`,
            400,
          );
        }
        const rawCount = parsed.params.count;
        let count = typeof rawCount === "bigint" ? Number(rawCount) : Number(rawCount ?? 1);
        if (!Number.isFinite(count) || count < 1) count = 1;
        if (count > MAX_UPLOAD_URL_COUNT) count = MAX_UPLOAD_URL_COUNT;

        const urls: { uploadUrl: string; downloadUrl: string; expiresAt: Date }[] = [];
        for (let i = 0; i < count; i++) {
          urls.push(await uploadUrlProvider.generateUploadUrl());
        }

        // Timestamp(MICROSECOND) — arrow-js's `setTimestampMicrosecond`
        // visitor internally does `BigInt(value * 1000)`, so we have to
        // pass a Number of milliseconds. Passing a BigInt of microseconds
        // would trip "Invalid mix of BigInt and other type in
        // multiplication". The Date.getTime() ms value fits in Number
        // safely for the next ~285k years.
        const expiresAt = urls.map((u) => u.expiresAt.getTime());
        const resultBatch = batchFromColumns(UPLOAD_URL_RESPONSE_SCHEMA, {
          upload_url: urls.map((u) => u.uploadUrl),
          download_url: urls.map((u) => u.downloadUrl),
          expires_at: expiresAt,
        });
        const responseBody = serializeIpcStream(UPLOAD_URL_RESPONSE_SCHEMA, [resultBatch]);
        const response = arrowResponse(responseBody);
        addCorsHeaders(response.headers);
        addCapabilityHeaders(response.headers);
        return compressIfAccepted(response, clientAcceptsZstd, clientAcceptsGzip);
      } catch (error: any) {
        if (error instanceof HttpRpcError) {
          const r = makeErrorResponse(error, error.statusCode, UPLOAD_URL_RESPONSE_SCHEMA);
          addCapabilityHeaders(r.headers);
          return compressIfAccepted(r, clientAcceptsZstd, clientAcceptsGzip);
        }
        const r = makeErrorResponse(error, 500, UPLOAD_URL_RESPONSE_SCHEMA);
        addCapabilityHeaders(r.headers);
        return compressIfAccepted(r, clientAcceptsZstd, clientAcceptsGzip);
      }
    }

    // Route: {prefix}/__describe__
    if (path === `${prefix}/${DESCRIBE_METHOD_NAME}`) {
      try {
        const response = await httpDispatchDescribe(
          protocol.name,
          methods,
          serverId,
          protocol.protocolVersion || undefined,
        );
        addCorsHeaders(response.headers);
        return compressIfAccepted(response, clientAcceptsZstd, clientAcceptsGzip);
      } catch (error: any) {
        return compressIfAccepted(makeErrorResponse(error, 500), clientAcceptsZstd, clientAcceptsGzip);
      }
    }

    // Parse method name and sub-path from URL
    if (!path.startsWith(`${prefix}/`)) {
      return new Response("Not Found", { status: 404 });
    }

    const subPath = path.slice(prefix.length + 1);
    let methodName: string;
    let action: "call" | "init" | "exchange";

    if (subPath.endsWith("/init")) {
      methodName = subPath.slice(0, -5);
      action = "init";
    } else if (subPath.endsWith("/exchange")) {
      methodName = subPath.slice(0, -9);
      action = "exchange";
    } else {
      methodName = subPath;
      action = "call";
    }

    // Look up method
    const method = methods.get(methodName);
    if (!method) {
      const available = [...methods.keys()].sort();
      const err = new MethodNotImplementedError(
        `Unknown method: '${methodName}'. Available methods: [${available.join(", ")}]`,
      );
      return compressIfAccepted(makeErrorResponse(err, 404), clientAcceptsZstd, clientAcceptsGzip);
    }

    // Application-protocol-version gate (HTTP dispatch path). Fires only
    // when the Protocol declared a `protocolVersion`, on unary calls and
    // stream init. `/exchange` continuations skip the gate — the Python
    // client (and parity TS client) only emits `vgi_rpc.protocol_version`
    // on the dispatch-entry request, not on follow-up exchange batches.
    // `__describe__` is exempt — diagnostic path for mismatched clients to
    // discover the server's version. Mirrors Python's _app_unary /
    // _app_stream gate.
    if (protocol.protocolVersionParts !== null && methodName !== DESCRIBE_METHOD_NAME && action !== "exchange") {
      try {
        // Peek at request batch metadata without consuming the body — the
        // dispatch helpers re-deserialize. Cost is one extra deserialize
        // per protocol-versioned dispatch; acceptable for a typed gate.
        let reqMeta: ReadonlyMap<string, string> | undefined;
        try {
          const peeked = deserializeBatch(body);
          reqMeta = peeked.metadata ?? undefined;
        } catch {
          // Malformed body — fall through to the dispatch helper, which
          // will surface the parse error properly.
        }
        enforceProtocolVersion(reqMeta);
      } catch (exc) {
        const errSchema = method.type === MethodType.UNARY ? method.resultSchema : EMPTY_SCHEMA;
        const errBatch = buildErrorBatch(errSchema, exc as Error, serverId, null);
        const errBody = serializeIpcStream(errSchema, [errBatch]);
        const response = arrowResponse(errBody, 400);
        addCorsHeaders(response.headers);
        addCapabilityHeaders(response.headers);
        return compressIfAccepted(response, clientAcceptsZstd, clientAcceptsGzip);
      }
    }

    // Fire on_serve_start lazily (idempotent on success). A failure here
    // propagates as a 500 to the client, leaves the bind un-fired, and
    // the next request retries.
    await notifyTransport(transportKind);

    const methodType = method.type === MethodType.UNARY ? "unary" : "stream";
    const protocolHash = await getProtocolHash();
    const auth = ctx.authContext;
    const info: DispatchInfo = {
      method: methodName,
      methodType,
      serverId,
      requestId: null,
      protocol: protocol.name,
      protocolHash,
      protocolVersion,
      kind: transportKind,
      principal: auth?.principal ?? "",
      authDomain: auth?.domain ?? "",
      authenticated: auth?.authenticated ?? false,
      // Self-contained Arrow IPC stream of the request batch — the body
      // we already buffered.  Best-effort: the access-log can still emit
      // even if we couldn't capture it.
      requestData: action === "call" ? body : undefined,
    };
    const stats: CallStatistics = {
      inputBatches: 0,
      outputBatches: 0,
      inputRows: 0,
      outputRows: 0,
      inputBytes: 0,
      outputBytes: 0,
    };

    const hookToken = dispatchHook?.onDispatchStart(info);
    let dispatchError: Error | undefined;

    try {
      let response: Response;

      if (action === "call") {
        if (method.type !== MethodType.UNARY) {
          throw new HttpRpcError(`Method '${methodName}' is a stream method. Use /init and /exchange endpoints.`, 400);
        }
        response = await httpDispatchUnary(method, body, ctx);
      } else if (action === "init") {
        if (method.type !== MethodType.STREAM) {
          throw new HttpRpcError(
            `Method '${methodName}' is a unary method. Use POST ${prefix}/${methodName} instead.`,
            400,
          );
        }
        response = await httpDispatchStreamInit(method, body, ctx);
      } else {
        if (method.type !== MethodType.STREAM) {
          throw new HttpRpcError(
            `Method '${methodName}' is a unary method. Use POST ${prefix}/${methodName} instead.`,
            400,
          );
        }
        response = await httpDispatchStreamExchange(method, body, ctx);
      }

      // Check if the dispatch function caught an error internally
      const internalError = (response as any).__dispatchError;
      if (internalError) {
        dispatchError = internalError instanceof Error ? internalError : new Error(String(internalError));
      }
      addCorsHeaders(response.headers);
      addCapabilityHeaders(response.headers);
      applyStickyResponseHeaders(response.headers, stickySink);
      return compressIfAccepted(response, clientAcceptsZstd, clientAcceptsGzip);
    } catch (error: any) {
      dispatchError = error instanceof Error ? error : new Error(String(error));
      if (error instanceof HttpRpcError) {
        const r = makeErrorResponse(error, error.statusCode);
        addCapabilityHeaders(r.headers);
        applyStickyResponseHeaders(r.headers, stickySink);
        return compressIfAccepted(r, clientAcceptsZstd, clientAcceptsGzip);
      }
      const r = makeErrorResponse(error, 500);
      addCapabilityHeaders(r.headers);
      applyStickyResponseHeaders(r.headers, stickySink);
      return compressIfAccepted(r, clientAcceptsZstd, clientAcceptsGzip);
    } finally {
      // Surface sticky lifecycle on the access log.
      if (stickySink) {
        if (stickySink.sessionId) info.sessionId = stickySink.sessionId;
        info.sessionAction = stickySink.action;
      }
      dispatchHook?.onDispatchEnd(hookToken, info, stats, dispatchError);
      // Release the per-session lock if dispatch held it and the handler
      // didn't already release it via close_session.
      if (stickyLockRelease) {
        try {
          stickyLockRelease();
        } catch {
          // ignore — mutex release is best-effort
        }
        stickyLockRelease = null;
      }
    }
  };

  /** Emit sticky-session response headers based on the sink's per-request state. */
  function applyStickyResponseHeaders(headers: Headers, sink: StickySink | null): void {
    if (!sink) return;
    if (sink.mintToken !== null) {
      headers.set(SESSION_HEADER, sink.mintToken);
      // Echo headers — emitted only on the session-opening response. The
      // client captures `VGI-Echo-<name>` and replays `<name>` for the
      // remainder of the session view.
      for (const [name, value] of stickyEchoHeadersArr) {
        headers.set(`${ECHO_HEADER_PREFIX}${name}`, value);
      }
    }
    if (sink.closed) {
      headers.set(SESSION_CLOSE_HEADER, "true");
    }
  }
}
