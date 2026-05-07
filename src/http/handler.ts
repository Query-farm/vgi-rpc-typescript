// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { randomBytes } from "../util/web-crypto.js";
import {
  type VgiSchema,
  batchFromColumns,
  field,
  schema as makeSchema,
  timestampMicro,
  utf8,
} from "../arrow/index.js";
import type { AuthContext } from "../auth.js";
import { DESCRIBE_METHOD_NAME, RPC_ERROR_HEADER } from "../constants.js";
import type { Protocol } from "../protocol.js";
import { type CallStatistics, type DispatchInfo, MethodType } from "../types.js";
import { zstdCompress, zstdDecompress } from "../util/zstd.js";
import { parseRequest } from "../wire/request.js";
import { buildErrorBatch } from "../wire/response.js";
import { buildWwwAuthenticateHeader, oauthResourceMetadataToJson, wellKnownPath } from "./auth.js";
import { chainAuthenticate } from "./bearer.js";
import {
  ARROW_CONTENT_TYPE,
  arrowResponse,
  HttpRpcError,
  readRequestFromBody as readRequestFromBodyImported,
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
  const signingKey = options?.signingKey ?? randomBytes(32);
  const tokenTtl = options?.tokenTtl ?? 3600;
  const corsOrigins = options?.corsOrigins;
  const corsMaxAge = options?.corsMaxAge === undefined ? 7200 : options.corsMaxAge;
  const maxRequestBytes = options?.maxRequestBytes;
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
          signingKey,
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

  const compressionLevel = options?.compressionLevel;
  const stateSerializer = options?.stateSerializer ?? jsonStateSerializer;
  const dispatchHook = options?.dispatchHook;

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

  /** Append capability headers (advertised on every response when configured). */
  function addCapabilityHeaders(headers: Headers, isOptions = false): void {
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
    if (isOptions && (maxRequestBytes != null || uploadUrlProvider)) {
      // Match Python: cache discovery results for 5 minutes.
      if (!headers.has("Cache-Control")) {
        headers.set("Cache-Control", "public, max-age=300");
      }
    }
  }

  // ctx is built per-request to include authContext; base fields set here
  const baseCtx = {
    signingKey,
    tokenTtl,
    serverId,
    maxStreamResponseBytes,
    maxResponseBytes,
    maxExternalizedResponseBytes,
    stateSerializer,
    externalLocation,
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

  async function compressIfAccepted(response: Response, clientAcceptsZstd: boolean): Promise<Response> {
    if (compressionLevel == null || !clientAcceptsZstd) return response;
    const responseBody = new Uint8Array(await response.arrayBuffer());
    const compressed = await zstdCompress(responseBody, compressionLevel);
    const headers = new Headers(response.headers);
    headers.set("Content-Encoding", "zstd");
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
    if (pkceConfig && path === `${prefix}/_oauth/token` &&
        (request.method === "POST" || request.method === "OPTIONS")) {
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

    if (request.method !== "POST") {
      return new Response("Method Not Allowed", { status: 405 });
    }

    // Build per-request dispatch context
    const ctx = { ...baseCtx, cookies: parseRequestCookies(request) } as typeof baseCtx & {
      authContext?: AuthContext;
      cookies: ReadonlyMap<string, string>;
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

    // Validate Content-Type
    const contentType = request.headers.get("Content-Type");
    if (!contentType || !contentType.includes(ARROW_CONTENT_TYPE)) {
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

    const clientAcceptsZstd = (request.headers.get("Accept-Encoding") ?? "").includes("zstd");

    // Read body, decompressing if needed
    let body = new Uint8Array(await request.arrayBuffer());
    if (maxRequestBytes != null && !exemptFromMaxBytes && body.byteLength > maxRequestBytes) {
      return new Response("Request body too large", { status: 413 });
    }
    const contentEncoding = request.headers.get("Content-Encoding");
    if (contentEncoding === "zstd") {
      body = await zstdDecompress(body);
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

        // Timestamp(MICROSECOND) stores int64 microseconds since epoch.
        // Both backends accept BigInt for int64-class types (the arrow-js
        // facade auto-coerces; flechette requires it under useBigIntTimestamp).
        const expiresAt = urls.map((u) => BigInt(u.expiresAt.getTime()) * 1000n);
        const resultBatch = batchFromColumns(UPLOAD_URL_RESPONSE_SCHEMA, {
          upload_url: urls.map((u) => u.uploadUrl),
          download_url: urls.map((u) => u.downloadUrl),
          expires_at: expiresAt,
        });
        const responseBody = serializeIpcStream(UPLOAD_URL_RESPONSE_SCHEMA, [resultBatch]);
        const response = arrowResponse(responseBody);
        addCorsHeaders(response.headers);
        addCapabilityHeaders(response.headers);
        return compressIfAccepted(response, clientAcceptsZstd);
      } catch (error: any) {
        if (error instanceof HttpRpcError) {
          const r = makeErrorResponse(error, error.statusCode, UPLOAD_URL_RESPONSE_SCHEMA);
          addCapabilityHeaders(r.headers);
          return compressIfAccepted(r, clientAcceptsZstd);
        }
        const r = makeErrorResponse(error, 500, UPLOAD_URL_RESPONSE_SCHEMA);
        addCapabilityHeaders(r.headers);
        return compressIfAccepted(r, clientAcceptsZstd);
      }
    }

    // Route: {prefix}/__describe__
    if (path === `${prefix}/${DESCRIBE_METHOD_NAME}`) {
      try {
        const response = await httpDispatchDescribe(protocol.name, methods, serverId);
        addCorsHeaders(response.headers);
        return compressIfAccepted(response, clientAcceptsZstd);
      } catch (error: any) {
        return compressIfAccepted(makeErrorResponse(error, 500), clientAcceptsZstd);
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
      const err = new Error(`Unknown method: '${methodName}'. Available methods: [${available.join(", ")}]`);
      return compressIfAccepted(makeErrorResponse(err, 404), clientAcceptsZstd);
    }

    const methodType = method.type === MethodType.UNARY ? "unary" : "stream";
    const info: DispatchInfo = { method: methodName, methodType, serverId, requestId: null };
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
      return compressIfAccepted(response, clientAcceptsZstd);
    } catch (error: any) {
      dispatchError = error instanceof Error ? error : new Error(String(error));
      if (error instanceof HttpRpcError) {
        const r = makeErrorResponse(error, error.statusCode);
        addCapabilityHeaders(r.headers);
        return compressIfAccepted(r, clientAcceptsZstd);
      }
      const r = makeErrorResponse(error, 500);
      addCapabilityHeaders(r.headers);
      return compressIfAccepted(r, clientAcceptsZstd);
    } finally {
      dispatchHook?.onDispatchEnd(hookToken, info, stats, dispatchError);
    }
  };
}
