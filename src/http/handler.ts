// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { batchFromColumns, deserializeBatch, schema as makeSchema, type VgiSchema } from "../arrow/index.js";
import { AuthContext } from "../auth.js";
import {
  DESCRIBE_METHOD_NAME,
  PROTOCOL_HASH_KEY,
  PROTOCOL_VERSION_KEY,
  REQUEST_ID_HEADER,
  RPC_ERROR_HEADER,
} from "../constants.js";
import { buildDescribeBatch } from "../dispatch/describe.js";
import { MethodNotImplementedError, ProtocolVersionError, parseProtocolVersion, SessionLostError } from "../errors.js";
import {
  PeerEvidenceSet,
  PeerIdentityRejectedError,
  PeerIdentityResult,
  PeerIdentityStatus,
  PeerIdentityUnavailableError,
  PeerResolutionContext,
  type PeerResolutionOptions,
} from "../identity.js";
import type { Protocol } from "../protocol.js";
import {
  type AccessLogDeferral,
  type CallStatistics,
  type DispatchInfo,
  MethodType,
  type ServeStartHook,
  TransportKind,
} from "../types.js";
import { gzipCompress, gzipDecompress } from "../util/gzip.js";
import { isWorkerd } from "../util/runtime.js";
import { randomBytes } from "../util/web-crypto.js";
import { isZstdCompressAvailable, zstdCompress, zstdDecompress } from "../util/zstd.js";
import { parseRequest, validateRequestSchema } from "../wire/request.js";
import { buildErrorBatch } from "../wire/response.js";
import { buildWwwAuthenticateHeader, oauthResourceMetadataToJson, wellKnownPath } from "./auth.js";
import { chainAuthenticate } from "./bearer.js";
import {
  COMPRESSION_ENCODINGS,
  CONTENT_ENCODING_HEADER,
  type CompressionEncoding,
  DEFAULT_COMPRESSION_LEVEL,
  type NegotiatedEncoding,
  pickResponseEncoding,
  SUPPORTED_ENCODINGS_HEADER,
  VGI_ACCEPT_ENCODING_HEADER,
  VGI_CONTENT_ENCODING_HEADER,
} from "./codec.js";
import {
  ARROW_CONTENT_TYPE,
  arrowResponse,
  ECHO_HEADER_PREFIX,
  HttpRpcError,
  MAX_UPLOAD_URL_COUNT,
  readRequestFromBody as readRequestFromBodyImported,
  SESSION_ACCEPT_HEADER,
  SESSION_CLOSE_HEADER,
  SESSION_ENDPOINT,
  SESSION_HEADER,
  STICKY_DEFAULT_TTL_HEADER,
  STICKY_ECHO_HEADERS_HEADER,
  STICKY_ENABLED_HEADER,
  serializeIpcStream,
  UPLOAD_URL_METHOD,
  UPLOAD_URL_PARAMS_SCHEMA,
  UPLOAD_URL_RESPONSE_SCHEMA,
} from "./common.js";
import {
  httpDispatchDescribe,
  httpDispatchStreamExchange,
  httpDispatchStreamInit,
  httpDispatchUnary,
} from "./dispatch.js";
import {
  createIntrospector,
  INTROSPECT_ENABLED_HEADER,
  INTROSPECT_ENDPOINT,
  introspectionDisabledResponse,
} from "./introspect.js";
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
import { PROOF_HEADER, PROOF_REQUIRED_HEADER } from "./proof.js";
import {
  ACCEPT_MAX_RESPONSE_BYTES_HEADER,
  ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER,
  minPositive,
  optionalPositiveSafeInteger,
  optionalResponseBudget,
  parseResponseBudgetDecimal,
} from "./response-budget.js";
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
import {
  AUTH_PROXY_REQUIRED_HEADER,
  AUTH_REASON_HEADER,
  AuthFailure,
  AuthReason,
  AuthUnavailableError,
  buildProxyHint,
  classifyAuthFailure,
  unauthorizedEnvelope,
} from "./unauthorized.js";

const EMPTY_SCHEMA: VgiSchema = makeSchema([]);

const EMPTY_COOKIES: ReadonlyMap<string, string> = new Map();

/** The upload-URL request is one nullable int64 parameter. Keep a hard cap
 * even when the general handler body cap is disabled so this helper endpoint
 * can never become an unbounded buffering exception. */
const MAX_UPLOAD_URL_REQUEST_BYTES = 8 * 1024;

async function readBodyBounded(request: Request, maxBytes?: number): Promise<Uint8Array> {
  if (maxBytes == null) return new Uint8Array(await request.arrayBuffer());

  const declared = request.headers.get("Content-Length");
  if (declared != null) {
    const parsed = Number(declared);
    if (Number.isFinite(parsed) && parsed > maxBytes) {
      throw new HttpRpcError("Request body too large", 413);
    }
  }

  if (!request.body) return new Uint8Array();
  const reader = request.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > maxBytes) {
        await reader.cancel("request body limit exceeded");
        throw new HttpRpcError("Request body too large", 413);
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
  const corsMaxAge = options?.corsMaxAge === undefined ? 300 : options.corsMaxAge;
  const maxRequestBytes = minPositive(
    optionalPositiveSafeInteger(options?.maxRequestBytes, "maxRequestBytes"),
    optionalPositiveSafeInteger(options?.hostingMaxRequestBytes, "hostingMaxRequestBytes"),
  );
  // The advertised request cap applies independently to encoded and decoded
  // bytes. Keeping the decoded default equal to maxRequestBytes prevents a
  // small compressed request from bypassing the capability clients use to
  // decide whether they must externalize.
  const configuredDecompressedCap = options?.maxDecompressedRequestBytes;
  const maxDecompressedRequestBytes =
    maxRequestBytes == null
      ? configuredDecompressedCap
      : configuredDecompressedCap == null
        ? maxRequestBytes
        : Math.min(maxRequestBytes, configuredDecompressedCap);
  // Fold the deprecated producer-only option into the application hard cap.
  // A producer turn is no longer allowed to overshoot and export a cursor.
  const maxResponseBytes = minPositive(
    optionalResponseBudget(options?.maxResponseBytes ?? options?.maxStreamResponseBytes, "maxResponseBytes"),
    optionalResponseBudget(options?.hostingMaxResponseBytes, "hostingMaxResponseBytes"),
  );
  const configuredPreferredResponseBytes = optionalResponseBudget(
    options?.preferredResponseBytes,
    "preferredResponseBytes",
  );
  const maxExternalizedResponseBytes = options?.maxExternalizedResponseBytes;
  const serverId = options?.serverId ?? crypto.randomUUID().replace(/-/g, "").slice(0, 12);

  let authenticate = options?.authenticate;
  const oauthMetadata = options?.oauthResourceMetadata;
  const peerIdentityProviders = [...(options?.peerIdentityProviders ?? [])];
  const peerAuthenticationPolicy = options?.peerAuthenticationPolicy;
  const peerResolutionTimeoutMs = options?.peerResolutionTimeoutMs ?? 5000;
  if (!Number.isFinite(peerResolutionTimeoutMs) || peerResolutionTimeoutMs <= 0) {
    throw new TypeError("peerResolutionTimeoutMs must be positive");
  }
  const peerProviderConcurrency = options?.peerProviderConcurrency ?? 64;
  if (!Number.isInteger(peerProviderConcurrency) || peerProviderConcurrency <= 0) {
    throw new TypeError("peerProviderConcurrency must be a positive integer");
  }
  let activePeerProviderCalls = 0;
  const peerProviderNames = new Set<string>();
  for (const provider of peerIdentityProviders) {
    if (!provider?.provider || peerProviderNames.has(provider.provider)) {
      throw new TypeError("peer identity providers must have unique non-empty names");
    }
    peerProviderNames.add(provider.provider);
  }
  if (peerProviderConcurrency < peerIdentityProviders.length) {
    throw new TypeError("peerProviderConcurrency must be at least the configured provider fanout");
  }

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

  // Response compression is ON by default at zstd level 1 (see
  // DEFAULT_COMPRESSION_LEVEL for why level 1 and not 3). `undefined` means
  // "not configured" and takes the default; an explicit `null` is the caller
  // deliberately turning compression off, which must stay reachable — it is
  // the only way to get the present-but-empty `VGI-Supported-Encodings`
  // advertisement that the cross-language conformance suite pins down.
  const compressionLevel =
    options?.compressionLevel === undefined ? DEFAULT_COMPRESSION_LEVEL : options.compressionLevel;
  // Cloudflare gzips a worker response that already carries a standard
  // `Content-Encoding`, without amending the header — so compressing here and
  // stamping `Content-Encoding: gzip` ships a *double*-encoded body under a
  // single header, and a conforming client decodes once, finds gzip bytes
  // where Arrow IPC should be, and fails. Measured against a deployed worker
  // over direct HTTPS, not just `wrangler dev` (which does the same).
  //
  // The fix is to keep compressing and move the label: `X-VGI-Content-Encoding`
  // is precisely the "an intermediary will mangle the standard header" escape
  // hatch already used for the browser path, and the edge leaves a body it
  // sees as unencoded alone. Compression is worth this trouble — gzip is ~60%
  // off a catalog response — so this must not degrade to identity.
  //
  // Note the codec here is gzip, not zstd: workerd exposes no zstd *encoder*
  // (`CompressionStream` does gzip/deflate only, and the fzstd fallback is
  // decompress-only), so `canProduceEncoding` already drops zstd and
  // `supportedEncodings` advertises gzip alone on this runtime.
  const stampCustomContentEncoding = isWorkerd();
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
    // Publish the promise before invoking user code.  An async IIFE whose
    // body calls a synchronously-throwing hook can run its `finally` before
    // the assignment of that IIFE's returned promise completes, leaving the
    // rejected promise cached forever.  Starting through a microtask avoids
    // that assignment-order trap and the identity check prevents an older
    // attempt from clearing a newer one.
    const attempt = Promise.resolve().then(() => onServeStart(kind));
    serveStartInFlight = attempt;
    try {
      await attempt;
      serveStartFired = true;
    } finally {
      if (serveStartInFlight === attempt) serveStartInFlight = null;
    }
  }

  // HTML page configuration
  const enableLandingPage = options?.enableLandingPage ?? true;
  const enableDescribePage = options?.enableDescribePage ?? true;
  const enableNotFoundPage = options?.enableNotFoundPage ?? true;
  const displayName = options?.protocolName ?? protocol.name;
  const repoUrl = options?.repositoryUrl ?? null;

  // Routes contributed by a layer above this one — see ExtraRouteHandler. The
  // VGI landing surface arrives this way from `@query-farm/vgi`; it used to be
  // built in here, which meant this package shipped a page describing catalogs
  // and a compiled bundle of the client library that depends on it.
  const extraRoutes = options?.extraRoutes ?? null;
  const oauthActive = pkceConfig != null;

  // Pre-render HTML pages for zero per-request overhead. A contributed route
  // that answers GET {prefix}/ takes precedence over the generic page, so both
  // can be configured without conflicting.
  const genericLandingHtml = enableLandingPage
    ? buildLandingPage(displayName, serverId, enableDescribePage ? `${prefix}/describe` : null, repoUrl)
    : null;
  const describeHtml = enableDescribePage ? buildDescribePage(displayName, serverId, methods, repoUrl) : null;
  const notFoundHtml = enableNotFoundPage ? buildNotFoundPage(prefix, displayName) : null;

  const externalLocation = options?.externalLocation;
  const uploadUrlProvider = options?.uploadUrlProvider;
  const maxUploadBytes = options?.maxUploadBytes;

  // Advertisement only: the proof gate is an opaque authenticate callback, so
  // the handler cannot tell `require` from `allow` and the operator states it.
  const proxyProofRequired = options?.proxyProofRequired === true;

  // The proxy-injected headers this service's authentication depends on, and
  // the operator-facing note derived from them. Both are fixed at
  // construction: the note describes a static property of the deployment, not
  // what failed on a given request, so every 401 carries the identical text
  // and it discloses nothing about which stage rejected an attempt (see
  // `./unauthorized.ts`). A proof gate contributes only in `require` mode —
  // in `allow` mode an absent proof never denies, so the note would misdirect.
  const proxyAuthHeaders = [...(proxyProofRequired ? [PROOF_HEADER] : []), ...(options?.proxyAuthHeaders ?? [])];
  const proxyHint = buildProxyHint(proxyAuthHeaders);

  // -------- Token introspection --------
  // Validated here, before any request exists, so a misconfiguration fails at
  // construction rather than at the first proxy preflight. Absent a resolver
  // the endpoint holds nothing and looks nothing up — no worker grows a
  // credential-to-identity oracle by upgrading a dependency.
  const introspectPath = `${prefix}${INTROSPECT_ENDPOINT}`;
  const introspector = options?.introspectResolver
    ? createIntrospector({
        resolver: options.introspectResolver,
        principals: options.introspectPrincipals,
        ttlSeconds: options.introspectTtlSeconds,
        rateLimit: options.introspectRateLimit,
      })
    : null;
  if (!introspector && options?.introspectPrincipals) {
    throw new Error(
      "introspectPrincipals was given without introspectResolver; the endpoint stays " +
        "disabled, so the allowlist would have no effect. Pass both or neither.",
    );
  }

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

  // Encodings the server can produce on the response side.
  // `compressionLevel` gates response compression overall; zstd is only
  // available when the runtime can actually encode it (Bun, Node ≥22.15,
  // Deno ≥2.6.9 — workerd lacks an encoder). gzip is always available via
  // Web `CompressionStream`.
  const zstdResponseAvailable = compressionLevel != null && isZstdCompressAvailable();

  /**
   * Can the server emit this codec right now? Both conditions must hold:
   * response compression is enabled (`compressionLevel != null` — true unless
   * the caller passed an explicit `null`) and the runtime has an encoder.
   * `zstdResponseAvailable` memoises `isZstdCompressAvailable()` so the
   * negotiation walk doesn't re-probe `node:zlib` per request.
   *
   * This is the single source of truth: both the negotiation walk and the
   * advertised `VGI-Supported-Encodings` list derive from it, so the two
   * cannot drift.
   */
  function canProduceEncoding(encoding: CompressionEncoding): boolean {
    if (compressionLevel == null) return false;
    return encoding === "zstd" ? zstdResponseAvailable : true;
  }

  /**
   * Can the server *decode* this codec on a request body? Both are always
   * decodable here, independent of `compressionLevel`: gzip via Web
   * `DecompressionStream`, zstd via the native decoder or the `fzstd`
   * pure-JS fallback (which is why workerd can decode zstd it cannot encode).
   */
  function canDecodeEncoding(_encoding: CompressionEncoding): boolean {
    return true;
  }

  // What this server speaks in *both* directions, in server-preference order:
  // the intersection of decodable-on-request and producible-on-response,
  // `identity` excluded (always available, carries no information). A stock
  // server therefore advertises `zstd, gzip` on Bun / Node ≥22.15 / Deno
  // ≥2.6.9; on workerd and older Node zstd decodes but cannot be *encoded*,
  // so it drops out and only `gzip` is advertised. The list is derived from
  // the runtime predicate, never a fixed string. Only an explicit
  // `compressionLevel: null` empties it.
  const supportedEncodings: CompressionEncoding[] = COMPRESSION_ENCODINGS.filter(
    (e) => canDecodeEncoding(e) && canProduceEncoding(e),
  );

  /** Append capability headers (advertised on every response when configured). */
  function addCapabilityHeaders(headers: Headers, isOptions = false): void {
    // Always emitted, even empty: present-but-empty means "I speak no
    // compression", which a client must be able to tell apart from an absent
    // header (legacy server — assume zstd).
    headers.set(SUPPORTED_ENCODINGS_HEADER, supportedEncodings.join(", "));
    if (maxRequestBytes != null) {
      headers.set("VGI-Max-Request-Bytes", String(maxRequestBytes));
    }
    if (maxResponseBytes != null) {
      headers.set("VGI-Max-Response-Bytes", String(maxResponseBytes));
    }
    headers.set(ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER, "true");
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
    // Emitted only in `require` mode — `allow` never denies, so advertising
    // there would tell a proxy the hop is enforced when it is not.
    if (proxyProofRequired) {
      headers.set(PROOF_REQUIRED_HEADER, "true");
    }
    // Absent, never "false", when disabled: a proxy preflights this at boot
    // rather than discovering at first login that the worker cannot answer.
    if (introspector) {
      headers.set(INTROSPECT_ENABLED_HEADER, "true");
    }
    if (stickyEnabled) {
      headers.set(STICKY_ENABLED_HEADER, "true");
      headers.set(STICKY_DEFAULT_TTL_HEADER, String(Math.floor(stickyDefaultTtl)));
      if (stickyEchoHeadersArr.length > 0) {
        headers.set(STICKY_ECHO_HEADERS_HEADER, stickyEchoHeadersArr.map(([k]) => k).join(", "));
      }
    }
    if (isOptions) {
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
    maxResponseBytes,
    preferredResponseBytes: configuredPreferredResponseBytes,
    maxExternalizedResponseBytes,
    stateSerializer,
    externalLocation,
    kind: transportKind,
    callStateCacheEntries: options?.callStateCacheEntries,
  };

  // Built once: a browser hides every response header from JavaScript unless
  // it is named here, so whatever this configuration *advertises* it must also
  // expose — each conditional below mirrors the condition on the emission.
  const corsExposeHeaders = [
    "WWW-Authenticate",
    REQUEST_ID_HEADER,
    VGI_CONTENT_ENCODING_HEADER,
    RPC_ERROR_HEADER,
    "VGI-Max-Response-Bytes",
    "VGI-Max-Externalized-Response-Bytes",
    "VGI-Externalization-Enabled",
    ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER,
    SUPPORTED_ENCODINGS_HEADER,
    ...(maxRequestBytes != null ? ["VGI-Max-Request-Bytes"] : []),
    ...(uploadUrlProvider
      ? ["VGI-Upload-URL-Support", ...(maxUploadBytes != null ? ["VGI-Max-Upload-Bytes"] : [])]
      : []),
    ...(proxyProofRequired ? [PROOF_REQUIRED_HEADER] : []),
    ...(introspector ? [INTROSPECT_ENABLED_HEADER] : []),
    // Not just the advert: a client that cannot read VGI-Session never learns
    // the token it is meant to replay, and one that cannot read the
    // VGI-Echo-<name> values cannot route the rest of the session. Sticky
    // degrades silently rather than failing, which is the worse failure.
    ...(stickyEnabled
      ? [
          STICKY_ENABLED_HEADER,
          STICKY_DEFAULT_TTL_HEADER,
          ...(stickyEchoHeadersArr.length > 0 ? [STICKY_ECHO_HEADERS_HEADER] : []),
          SESSION_HEADER,
          SESSION_CLOSE_HEADER,
          ...stickyEchoHeadersArr.map(([name]) => `${ECHO_HEADER_PREFIX}${name}`),
        ]
      : []),
    // A browser client that cannot read the rejection headers is back to
    // guessing the reason out of the body, so they are exposed cross-origin —
    // the proxy note only when this service would ever emit it.
    AUTH_REASON_HEADER,
    ...(proxyHint ? [AUTH_PROXY_REQUIRED_HEADER] : []),
  ].join(", ");

  function addCorsHeaders(headers: Headers, isOptions = false, requestedHeaders?: string | null): void {
    if (corsOrigins) {
      headers.set("Access-Control-Allow-Origin", corsOrigins);
      headers.set("Access-Control-Allow-Methods", "POST, OPTIONS");
      // Reflect the preflight's requested headers so clients may send custom
      // VGI request headers (e.g. x-vgi-accept-encoding, VGI-Session) without a
      // hard-coded allow-list. Mirrors the Python framework, whose preflight
      // echoes Access-Control-Request-Headers. Falls back to the common pair.
      headers.set(
        "Access-Control-Allow-Headers",
        requestedHeaders && requestedHeaders.length > 0
          ? requestedHeaders
          : `Content-Type, Authorization, ${ACCEPT_MAX_RESPONSE_BYTES_HEADER}`,
      );
      headers.set("Access-Control-Expose-Headers", corsExposeHeaders);
      // A server that has opted into serving cross-origin callers has, by
      // construction, opted into being embeddable by them: without this a
      // caller running under COEP require-corp (any cross-origin-isolated
      // page, e.g. multithreaded WASM) has the response blocked outright.
      headers.set("Cross-Origin-Resource-Policy", "cross-origin");
      if (isOptions && corsMaxAge != null) {
        headers.set("Access-Control-Max-Age", String(corsMaxAge));
      }
    }
  }

  /**
   * Collapse runs of `/` into one. See the call site for why this matters.
   *
   * Cheap on the common path: the scan `includes` does is far cheaper than a
   * regex replace, and virtually every request arrives already normalized.
   */
  function normalizePath(pathname: string): string {
    return pathname.includes("//") ? pathname.replace(/\/{2,}/g, "/") : pathname;
  }

  /**
   * Split a POST path into the method it names and the action on it, or `null`
   * when the path lies outside this worker's prefix.
   */
  function resolveRoute(path: string): { methodName: string; action: "call" | "init" | "exchange" } | null {
    if (!path.startsWith(`${prefix}/`)) return null;
    const subPath = path.slice(prefix.length + 1);
    if (subPath.endsWith("/init")) return { methodName: subPath.slice(0, -5), action: "init" };
    if (subPath.endsWith("/exchange")) return { methodName: subPath.slice(0, -9), action: "exchange" };
    return { methodName: subPath, action: "call" };
  }

  /** Negotiate the response codec from the request's two accept headers. */
  function negotiateResponseEncoding(request: Request): NegotiatedEncoding {
    return pickResponseEncoding(
      // On workerd the runtime rewrites the client's Accept-Encoding before the
      // worker sees it — measured: `identity`, `deflate` and a wholly absent
      // header all arrive as `br, gzip`, and a browser's own list arrives
      // stripped. The header therefore carries nothing about the client, and
      // `identity` cannot be expressed through it at all.
      //
      // That would be harmless if we could answer with a standard
      // Content-Encoding, which every fetch layer undoes. We cannot: on this
      // runtime the response can only be labelled X-VGI-Content-Encoding (see
      // stampCustomContentEncoding), which nothing undoes automatically. So
      // honouring the fabricated header means compressing for a client that
      // never asked and may have no idea it must decompress — it then feeds
      // gzip's magic bytes to the Arrow reader, which reports them as a
      // 559903-byte metadata length.
      //
      // X-VGI-Accept-Encoding is a custom header, so workerd passes it through
      // untouched; it is the only trustworthy signal here, and requiring it
      // makes compression opt-in exactly where the label is opt-in too.
      stampCustomContentEncoding ? null : request.headers.get("Accept-Encoding"),
      request.headers.get(VGI_ACCEPT_ENCODING_HEADER),
      canProduceEncoding,
    );
  }

  async function compressIfAccepted(
    response: Response,
    negotiated: NegotiatedEncoding,
    responseLimitBytes?: number,
  ): Promise<Response> {
    const { codec, usedCustom } = negotiated;
    const responseBody = new Uint8Array(await response.arrayBuffer());
    if (responseLimitBytes != null && responseBody.byteLength > responseLimitBytes) {
      const headers = new Headers(response.headers);
      headers.delete(CONTENT_ENCODING_HEADER);
      headers.delete(VGI_CONTENT_ENCODING_HEADER);
      const error = new Error(
        `HTTP body exceeds max_response_bytes (${responseBody.byteLength} > ${responseLimitBytes})`,
      );
      error.name = "ResponseTooLargeError";
      const errorBatch = buildErrorBatch(EMPTY_SCHEMA, error, serverId, null);
      const errorBody = serializeIpcStream(EMPTY_SCHEMA, [errorBatch]);
      headers.set("Content-Type", ARROW_CONTENT_TYPE);
      headers.set(RPC_ERROR_HEADER, "true");
      headers.set("Content-Length", String(errorBody.byteLength));
      return new Response(errorBody as unknown as BodyInit, { status: 200, headers });
    }
    if (compressionLevel == null || !codec) {
      return new Response(responseBody as unknown as BodyInit, { status: response.status, headers: response.headers });
    }
    const compressed =
      codec === "zstd" ? await zstdCompress(responseBody, compressionLevel) : await gzipCompress(responseBody);
    const headers = new Headers(response.headers);
    // Two cases want the VGI header rather than the standard one, both being
    // "something between us and the client would mangle a standard
    // Content-Encoding":
    //  - the client could only state its preference through
    //    X-VGI-Accept-Encoding (a browser `fetch()`, which cannot set
    //    Accept-Encoding), so its fetch layer would transparently decode it;
    //  - we are on workerd, where the edge re-gzips an already-encoded body
    //    (see `stampCustomContentEncoding`).
    // Clients resolve X-VGI-Content-Encoding ahead of Content-Encoding, so the
    // body is decoded exactly once either way.
    const useCustomHeader = usedCustom || stampCustomContentEncoding;
    headers.set(useCustomHeader ? VGI_CONTENT_ENCODING_HEADER : CONTENT_ENCODING_HEADER, codec);
    return new Response(compressed as unknown as BodyInit, {
      status: response.status,
      headers,
    });
  }

  /**
   * Render the standardized 401 of `docs/unauthorized-spec.md` §4: the reason
   * header, a no-store cache directive, the proxy note when this service's
   * authentication depends on a proxy, and the JSON envelope.
   *
   * §4.2 lets a service skip the HTML page and always answer with JSON; what
   * it must never do is answer a non-HTML request with HTML. This port takes
   * the JSON-only option, so `Accept` does not steer the body — and the reason
   * header, the part clients actually parse, is set either way.
   */
  function unauthorizedResponse(reason: AuthReason, detail: string, headers: Headers): Response {
    headers.set("Content-Type", "application/json");
    headers.set(AUTH_REASON_HEADER, reason);
    if (proxyHint) headers.set(AUTH_PROXY_REQUIRED_HEADER, "true");
    // A 401 is per-request and flips to 200 on the next attempt with a
    // credential, so it must never be held by a shared cache.
    headers.set("Cache-Control", "no-store");
    return new Response(unauthorizedEnvelope(reason, detail, proxyHint), { status: 401, headers });
  }

  function authenticationErrorResponse(error: unknown, request: Request): Response {
    if (error instanceof AuthUnavailableError || error instanceof PeerIdentityUnavailableError) {
      const headers = new Headers({ "Content-Type": "application/json", "Cache-Control": "no-store" });
      addCorsHeaders(headers);
      const retryAfter = Number.isFinite(error.retryAfter) && error.retryAfter >= 0 ? Math.ceil(error.retryAfter) : 5;
      headers.set("Retry-After", String(retryAfter));
      const detail =
        error instanceof PeerIdentityUnavailableError
          ? "peer identity unavailable"
          : "authentication authority unavailable";
      return new Response(JSON.stringify({ error: "authentication_unavailable", detail }), { status: 503, headers });
    }
    const headers = new Headers();
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
    const { reason } = classifyAuthFailure(error);
    return unauthorizedResponse(reason, "authentication rejected", headers);
  }

  function peerEvidenceBinding(auth: AuthContext | undefined): string | undefined {
    const value = auth?.claims?.peer_evidence_binding;
    return typeof value === "string" && value ? value : undefined;
  }

  async function resolveRequestIdentity(
    request: Request,
  ): Promise<{ authContext: AuthContext; peerEvidence: PeerEvidenceSet }> {
    let authContext = AuthContext.anonymous();
    let missingCredential: AuthFailure | undefined;
    if (authenticate) {
      try {
        authContext = (await authenticate(request)) ?? AuthContext.anonymous();
      } catch (error) {
        if (peerAuthenticationPolicy && error instanceof AuthFailure && error.reason === AuthReason.MissingCredential) {
          missingCredential = error;
        } else {
          throw error;
        }
      }
    }

    let peerEvidence = PeerEvidenceSet.EMPTY;
    if (peerIdentityProviders.length > 0) {
      const controller = new AbortController();
      const deadline = Date.now() + peerResolutionTimeoutMs;
      const startedAt = performance.now();
      const timer = setTimeout(() => controller.abort(), peerResolutionTimeoutMs);
      const timeout = new Promise<never>((_resolve, reject) => {
        const rejectTimeout = () => reject(new PeerIdentityUnavailableError("peer identity resolution timed out"));
        if (controller.signal.aborted) rejectTimeout();
        else controller.signal.addEventListener("abort", rejectTimeout, { once: true });
      });
      try {
        let supplied: PeerResolutionOptions;
        try {
          supplied = (await Promise.race([Promise.resolve(options?.peerResolutionContext?.(request)), timeout])) ?? {};
        } catch (error) {
          if (error instanceof PeerIdentityRejectedError || error instanceof PeerIdentityUnavailableError) throw error;
          throw new PeerIdentityUnavailableError("peer identity resolution context failed");
        }
        const remainingBudgetMs = peerResolutionTimeoutMs - (performance.now() - startedAt);
        if (remainingBudgetMs <= 0 || controller.signal.aborted) {
          throw new PeerIdentityUnavailableError("peer identity resolution timed out");
        }
        const resolution = new PeerResolutionContext("http", {
          authority: new URL(request.url).host,
          serviceName: options?.peerServiceName,
          // Fetch Headers irreversibly merges duplicates. Identity-bearing headers are only
          // accepted when a runtime adapter supplies raw multiplicity-preserving values.
          headers: new Map<string, readonly string[]>(),
          ...supplied,
          deadline,
          budgetMs: remainingBudgetMs,
        });
        const outcomes: Array<PeerIdentityResult | undefined> = new Array(peerIdentityProviders.length);
        const providerTasks = peerIdentityProviders.map((provider, index) => {
          if (activePeerProviderCalls >= peerProviderConcurrency) {
            outcomes[index] = new PeerIdentityResult(provider.provider, PeerIdentityStatus.UNAVAILABLE);
            return Promise.resolve();
          }
          activePeerProviderCalls++;
          return Promise.resolve()
            .then(() => provider.resolve(resolution, controller.signal))
            .then((result) => {
              outcomes[index] =
                result && result.provider === provider.provider
                  ? result
                  : new PeerIdentityResult(provider.provider, PeerIdentityStatus.INVALID);
            })
            .catch((error: unknown) => {
              outcomes[index] = new PeerIdentityResult(
                provider.provider,
                error instanceof PeerIdentityRejectedError
                  ? PeerIdentityStatus.INVALID
                  : PeerIdentityStatus.UNAVAILABLE,
              );
            })
            .finally(() => {
              activePeerProviderCalls--;
            });
        });
        await Promise.race([Promise.all(providerTasks), timeout]).catch((error: unknown) => {
          if (!(error instanceof PeerIdentityUnavailableError)) throw error;
        });
        // Promise reactions for providers that completed in the deadline turn run before
        // this continuation. Yield once more before classifying only genuinely unfinished
        // calls, so a completed INVALID result can never be downgraded to UNAVAILABLE.
        await Promise.resolve();
        const results = Array.from(
          { length: peerIdentityProviders.length },
          (_unused, index) =>
            outcomes[index] ??
            new PeerIdentityResult(peerIdentityProviders[index].provider, PeerIdentityStatus.UNAVAILABLE),
        );
        peerEvidence = new PeerEvidenceSet(results);
      } finally {
        clearTimeout(timer);
      }
    }

    if (peerAuthenticationPolicy) {
      try {
        authContext = await peerAuthenticationPolicy(peerEvidence, authContext);
      } catch (error) {
        if (error instanceof PeerIdentityUnavailableError) {
          throw new PeerIdentityUnavailableError();
        }
        if (error instanceof PeerIdentityRejectedError) {
          throw new PeerIdentityRejectedError("peer identity authentication rejected", error.vgiAuthReason);
        }
        throw new PeerIdentityRejectedError("peer identity authentication rejected");
      }
    }
    if (missingCredential && !authContext.authenticated) throw missingCredential;
    return { authContext, peerEvidence };
  }

  function makeErrorResponse(error: Error, statusCode: number, schema: VgiSchema = EMPTY_SCHEMA): Response {
    const errBatch = buildErrorBatch(schema, error, serverId, null);
    const body = serializeIpcStream(schema, [errBatch]);
    const resp = arrowResponse(body, statusCode);
    addCorsHeaders(resp.headers);
    return resp;
  }

  function invalidAcceptedResponseBudget(error: unknown, request: Request, isOptions = false): Response {
    const valueError = new Error(
      `Invalid ${ACCEPT_MAX_RESPONSE_BYTES_HEADER}: ${error instanceof Error ? error.message : String(error)}`,
    );
    valueError.name = "ValueError";
    const response = makeErrorResponse(valueError, 400);
    // Malformed request-budget syntax is an Arrow RPC error even though its
    // HTTP status remains 400. The support marker lets a capability-aware
    // client distinguish validation from a legacy intermediary response.
    response.headers.set(RPC_ERROR_HEADER, "true");
    if (isOptions) {
      addCorsHeaders(response.headers, true, request.headers.get("Access-Control-Request-Headers"));
    }
    addCapabilityHeaders(response.headers, isOptions);
    return response;
  }

  const enableHealthEndpoint = options?.enableHealthEndpoint ?? true;
  const healthPath = `${prefix}/health`;
  const healthBody = enableHealthEndpoint
    ? JSON.stringify({ status: "ok", server_id: serverId, protocol: displayName })
    : null;

  const dispatchRequest = async function handler(
    request: Request,
    deferral?: AccessLogDeferral,
    egress?: { externalizedBytes: number },
    requestId: string | null = null,
  ): Promise<Response> {
    const url = new URL(request.url);
    // Collapse repeated slashes before anything routes on the path.
    //
    // A client that joins a base URL already ending in "/" with "/<method>"
    // sends "//<method>". With an empty prefix `resolveRoute` then slices
    // exactly one character off and dispatches the method name "/<method>",
    // which matches nothing — surfacing as
    //   Unknown method: '/__describe__'. Available methods: [...]
    // where every name in that list is unprefixed, so the leading slash is the
    // whole story. Every other route (health, landing, the client bundle,
    // .well-known) 404s the same way for the same reason.
    //
    // RFC 3986 treats "//a" and "/a" as distinct paths, but no method name here
    // can contain a slash and no route depends on an empty segment, so
    // collapsing is safe and matches what ordinary HTTP servers do. Normalizing
    // once here rather than inside `resolveRoute` keeps every route consistent.
    const path = normalizePath(url.pathname);

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
    //
    // HEAD is answered alongside GET: /health is the mandatory capability-
    // discovery endpoint and the C++ client probes it with HEAD. Without a HEAD
    // responder the probe 405s and discovery silently degrades to defaults.
    if (healthBody !== null && (request.method === "GET" || request.method === "HEAD") && path === healthPath) {
      const headers = new Headers({ "Content-Type": "application/json" });
      addCorsHeaders(headers);
      addCapabilityHeaders(headers);
      if (request.method === "HEAD") {
        headers.set("Content-Length", String(new TextEncoder().encode(healthBody).byteLength));
        return new Response(null, { status: 200, headers });
      }
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
      const acceptedRaw = request.headers.get(ACCEPT_MAX_RESPONSE_BYTES_HEADER);
      if (acceptedRaw !== null) {
        try {
          parseResponseBudgetDecimal(acceptedRaw);
        } catch (error) {
          return invalidAcceptedResponseBudget(error, request, true);
        }
      }
      const headers = new Headers();
      addCorsHeaders(headers, true, request.headers.get("Access-Control-Request-Headers"));
      addCapabilityHeaders(headers, true);
      // Always answer OPTIONS so capability discovery via OPTIONS /health (or
      // any other path) works even when CORS isn't enabled. There is no
      // configuration under which we have nothing to advertise: every server
      // emits `VGI-Supported-Encodings` (present-but-empty when it speaks no
      // codec) and `VGI-Externalization-Enabled`, so the former 405
      // fallback would have hidden real capability state from the probe.
      return new Response(null, { status: 204, headers });
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

      // Routes contributed from above (e.g. the VGI landing surface). Placed
      // here deliberately: after the OAuth browser-redirect branch, so a
      // contributed page carries the same auth exposure as the built-in pages
      // rather than jumping ahead of that redirect, and before the generic
      // landing page and the 404, so it can replace either. This is the exact
      // position the VGI landing block occupied before it moved out.
      if (extraRoutes) {
        // Hand over a URL whose pathname is already collapsed. Contributed
        // routes match on `ctx.url.pathname` rather than on the `path` this
        // handler computed, so without this they would each have to remember to
        // normalize — and the one in @query-farm/vgi did not, leaving
        // `GET //vgi-client.js` a 404 after the RPC routes were fixed. Rebuilt
        // only when it would differ, since this is every non-RPC request.
        const routeUrl = url.pathname === path ? url : new URL(url.href);
        if (routeUrl !== url) routeUrl.pathname = path;
        const contributed = await extraRoutes(request, {
          url: routeUrl,
          prefix,
          serverId,
          oauthActive,
          addCorsHeaders,
        });
        if (contributed) return contributed;
      }

      // Generic landing page: GET {prefix}/ or GET {prefix}, when no
      // contributed route claimed it.
      if (genericLandingHtml && (path === prefix || path === `${prefix}/`)) {
        const headers = new Headers({ "Content-Type": "text/html; charset=utf-8" });
        addCorsHeaders(headers);
        return new Response(genericLandingHtml, { status: 200, headers });
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
      // Resolve the same application and transport identity as dispatch so
      // teardown cannot cross an evidence boundary established at open time.
      let principalKey = sessionPrincipalKey(false, null, null);
      let aadPrincipal: string | null = null;
      let aadDomain: string | null = null;
      let evidenceBinding: string | undefined;
      try {
        const identity = await resolveRequestIdentity(request);
        const auth = identity.authContext;
        evidenceBinding = peerEvidenceBinding(auth);
        if (auth.authenticated) {
          aadPrincipal = auth.principal ?? "";
          aadDomain = auth.domain;
        }
        principalKey = sessionPrincipalKey(auth.authenticated, auth.domain, auth.principal, evidenceBinding);
      } catch {
        // Stale, forged, and identity-mismatched tokens all intentionally
        // collapse to the endpoint's idempotent 200 response.
      }
      const aad = computeAad(aadPrincipal, evidenceBinding, aadDomain);
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

    // POST {prefix}/__introspect_token__ on a worker that never enabled it.
    // Answered ahead of authentication on purpose: "this worker does not do
    // introspection" is not a secret, and a caller must learn it at preflight
    // rather than after arranging credentials it will never need.
    if (!introspector && path === introspectPath) {
      const response = introspectionDisabledResponse();
      addCorsHeaders(response.headers);
      return response;
    }

    // Build per-request dispatch context. `streamObserver` is where the
    // dispatcher reports the two stream facts the handler cannot see for
    // itself — the chain id sealed inside the tokens, and a cancel flag that
    // rides in request-batch metadata.
    const streamObserver: { streamId?: string; cancelled?: boolean } = {};
    // Authentication — run before content-type validation so unauthenticated
    // requests get 401 regardless of body shape or response-budget syntax.
    let identity: { authContext: AuthContext; peerEvidence?: PeerEvidenceSet };
    try {
      identity = await resolveRequestIdentity(request);
    } catch (error) {
      return authenticationErrorResponse(error, request);
    }

    let acceptedMaxResponseBytes: number | undefined;
    const acceptedRaw = request.headers.get(ACCEPT_MAX_RESPONSE_BYTES_HEADER);
    if (acceptedRaw !== null) {
      try {
        acceptedMaxResponseBytes = parseResponseBudgetDecimal(acceptedRaw);
      } catch (error) {
        return invalidAcceptedResponseBudget(error, request);
      }
    }
    const responseLimitBytes = minPositive(maxResponseBytes, acceptedMaxResponseBytes);
    const preferredResponseBytes =
      configuredPreferredResponseBytes == null
        ? undefined
        : minPositive(configuredPreferredResponseBytes, responseLimitBytes);

    const ctx = {
      ...baseCtx,
      maxResponseBytes: responseLimitBytes,
      preferredResponseBytes,
      authContext: identity.authContext,
      peerEvidence: identity.peerEvidence,
      cookies: parseRequestCookies(request),
      egress,
      streamObserver,
    } as typeof baseCtx & {
      authContext?: AuthContext;
      peerEvidence?: PeerEvidenceSet;
      cookies: ReadonlyMap<string, string>;
      stickyContext?: StickySink;
      streamObserver: { streamId?: string; cancelled?: boolean };
    };

    // POST {prefix}/__introspect_token__ — JSON in, JSON out, so it sits ahead
    // of the Arrow media-type gate. It needs the caller's identity (the
    // allowlist is the whole point) but none of the dispatch machinery below.
    if (introspector && path === introspectPath) {
      const response = await introspector.handle(request, ctx.authContext);
      addCorsHeaders(response.headers);
      addCapabilityHeaders(response.headers);
      return response;
    }

    // Hoisted ahead of sticky resolution so the SessionLost path's
    // `compressIfAccepted` call can see it. Honours the client's stated
    // order across `X-VGI-Accept-Encoding` + `Accept-Encoding`; see
    // `./codec.ts`.
    const responseEncoding = negotiateResponseEncoding(request);

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
      const evidenceBinding = peerEvidenceBinding(auth);
      const principalKey = sessionPrincipalKey(!!auth?.authenticated, auth?.domain, auth?.principal, evidenceBinding);
      const aad = computeAad(aadPrincipal, evidenceBinding, auth?.domain);
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
          return compressIfAccepted(r, responseEncoding, responseLimitBytes);
        }
        const entry = sessionRegistry.get(opened.sessionId, principalKey);
        if (!entry) {
          const r = makeErrorResponse(new SessionLostError("session not found, expired, or principal mismatch"), 500);
          addCapabilityHeaders(r.headers);
          return compressIfAccepted(r, responseEncoding, responseLimitBytes);
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

    // Route resolution precedes media-type validation, matching the reference
    // server, where an unmatched path hits the 404 sink before any resource
    // inspects the body. A path this worker does not serve is a 404 whatever
    // the body claims to be; answering 415 is what makes a caller that
    // classifies 401/403/404 as definitive — the classification token
    // introspection mandates — retry an unrouted path forever.
    const specialPost = path === `${prefix}/${UPLOAD_URL_METHOD}/init` || path === `${prefix}/${DESCRIBE_METHOD_NAME}`;
    const route = specialPost ? null : resolveRoute(path);
    if (!specialPost) {
      if (!route) {
        if (stickyLockRelease) stickyLockRelease();
        return new Response("Not Found", { status: 404 });
      }
      if (!methods.has(route.methodName)) {
        if (stickyLockRelease) stickyLockRelease();
        const available = [...methods.keys()].sort();
        const err = new MethodNotImplementedError(
          `Unknown method: '${route.methodName}'. Available methods: [${available.join(", ")}]`,
        );
        return compressIfAccepted(makeErrorResponse(err, 404), responseEncoding, responseLimitBytes);
      }
    }

    // Validate Content-Type
    const contentType = request.headers.get("Content-Type");
    if (!contentType?.includes(ARROW_CONTENT_TYPE)) {
      if (stickyLockRelease) stickyLockRelease();
      return new Response(`Unsupported Media Type: expected ${ARROW_CONTENT_TYPE}`, { status: 415 });
    }

    // Read incrementally so chunked requests cannot bypass the limit and do
    // not first become one unbounded ArrayBuffer. The upload-URL helper has a
    // small unconditional cap even if the general RPC cap is disabled.
    const isUploadUrlRequest = path === `${prefix}/${UPLOAD_URL_METHOD}/init`;
    const wireBodyCap = isUploadUrlRequest
      ? Math.min(maxRequestBytes ?? Number.POSITIVE_INFINITY, MAX_UPLOAD_URL_REQUEST_BYTES)
      : maxRequestBytes;
    let body: Uint8Array;
    try {
      body = await readBodyBounded(request, wireBodyCap);
    } catch (error) {
      if (error instanceof HttpRpcError) return new Response(error.message, { status: error.statusCode });
      throw error;
    }
    // What the peer actually sent, captured before decompression — the
    // egress-accounting figure. `input_bytes` on the same record measures the
    // decoded Arrow buffers instead, and the two differ by whatever the
    // client's compressor achieved.
    const requestWireBytes = body.byteLength;
    const contentEncoding = (request.headers.get("Content-Encoding") ?? "").trim().toLowerCase();
    if (contentEncoding === "zstd" || contentEncoding === "gzip") {
      try {
        const decompressedCap = isUploadUrlRequest
          ? Math.min(maxDecompressedRequestBytes ?? Number.POSITIVE_INFINITY, MAX_UPLOAD_URL_REQUEST_BYTES)
          : maxDecompressedRequestBytes;
        body =
          contentEncoding === "zstd"
            ? await zstdDecompress(body, decompressedCap)
            : await gzipDecompress(body, decompressedCap);
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
        try {
          validateRequestSchema(reqSchema, UPLOAD_URL_PARAMS_SCHEMA, UPLOAD_URL_METHOD);
        } catch (error) {
          const message = (error as { errorMessage?: unknown })?.errorMessage;
          const httpError = new HttpRpcError(typeof message === "string" ? message : String(error), 400);
          httpError.name = "ProtocolError";
          throw httpError;
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
        return compressIfAccepted(response, responseEncoding, responseLimitBytes);
      } catch (error: any) {
        if (error instanceof HttpRpcError) {
          const r = makeErrorResponse(error, error.statusCode, UPLOAD_URL_RESPONSE_SCHEMA);
          addCapabilityHeaders(r.headers);
          return compressIfAccepted(r, responseEncoding, responseLimitBytes);
        }
        const r = makeErrorResponse(error, 500, UPLOAD_URL_RESPONSE_SCHEMA);
        addCapabilityHeaders(r.headers);
        return compressIfAccepted(r, responseEncoding, responseLimitBytes);
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
        return compressIfAccepted(response, responseEncoding, responseLimitBytes);
      } catch (error: any) {
        return compressIfAccepted(makeErrorResponse(error, 500), responseEncoding, responseLimitBytes);
      }
    }

    // Resolved above, ahead of the media-type gate; both are non-null there.
    const { methodName, action } = route!;
    const method = methods.get(methodName)!;

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
        return compressIfAccepted(response, responseEncoding, responseLimitBytes);
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
      // The same value the response carries as `X-Request-ID`. Agreement
      // between the header and the record is the entire point of the field:
      // an id on the response that names nothing in the log looks like a
      // working trail right up to the moment somebody follows it.
      requestId,
      protocol: protocol.name,
      protocolHash,
      protocolVersion,
      kind: transportKind,
      principal: auth?.principal ?? "",
      authDomain: auth?.domain ?? "",
      authenticated: auth?.authenticated ?? false,
      // Self-contained Arrow IPC stream of the request batch — the body we
      // already buffered.  Carried on unary calls *and* stream `/init`, both
      // of which spec §4.3 requires it on; `/exchange` continuations are the
      // one shape that must not have it. Best-effort: the access log can
      // still emit even if we couldn't capture it.
      requestData: action === "call" || action === "init" ? body : undefined,
      claims: auth?.claims,
      requestBytes: requestWireBytes,
      deferral,
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
      info.httpStatus = response.status;
      return compressIfAccepted(response, responseEncoding, responseLimitBytes);
    } catch (error: any) {
      dispatchError = error instanceof Error ? error : new Error(String(error));
      if (error instanceof HttpRpcError) {
        const r = makeErrorResponse(error, error.statusCode);
        addCapabilityHeaders(r.headers);
        applyStickyResponseHeaders(r.headers, stickySink);
        info.httpStatus = r.status;
        return compressIfAccepted(r, responseEncoding, responseLimitBytes);
      }
      const r = makeErrorResponse(error, 500);
      addCapabilityHeaders(r.headers);
      applyStickyResponseHeaders(r.headers, stickySink);
      info.httpStatus = r.status;
      return compressIfAccepted(r, responseEncoding, responseLimitBytes);
    } finally {
      // Surface sticky lifecycle on the access log.
      if (stickySink) {
        if (stickySink.sessionId) info.sessionId = stickySink.sessionId;
        info.sessionAction = stickySink.action;
      }
      // Stream identity and cancellation, as reported by the dispatcher.
      // Read here rather than at construction because the dispatcher only
      // learns them while opening the tokens — a record assembled before the
      // call ran can name at best a placeholder, which is what this used to
      // emit: the same 32 zeros for every stream on the server.
      if (streamObserver.streamId) info.streamId = streamObserver.streamId;
      if (streamObserver.cancelled) info.cancelled = true;
      if (egress?.externalizedBytes) info.externalizedBytes = egress.externalizedBytes;
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

  return async function handler(request: Request): Promise<Response> {
    // Per-request correlation id, resolved before anything else so every exit
    // from the handler can carry it. An inbound value is propagated unchanged
    // — a proxy or client that minted an id needs this worker's records under
    // the same one, or the two logs cannot be joined at all.
    const requestId = resolveRequestId(request);

    if (!dispatchHook) {
      const plain = await dispatchRequest(request, undefined, undefined, requestId);
      stampResponseBudgetSupport(plain);
      stampRequestId(plain, requestId);
      return plain;
    }

    // Deferred access-log emission. A record assembled at dispatch time
    // cannot carry `response_bytes`: compression runs on the way out, below,
    // so the number a handler could report is the uncompressed one — off by
    // a factor of ~1000 on a compressible result, and the wrong number for
    // anything that costs money. So dispatch queues its records here and they
    // are emitted once the final body exists. A crash in between loses that
    // request's records; the alternative is a permanently wrong figure.
    const pending: ((responseBytes: number | undefined) => void)[] = [];
    const deferral: AccessLogDeferral = { defer: (emit) => pending.push(emit) };
    const egress = { externalizedBytes: 0 };
    let response = await dispatchRequest(request, deferral, egress, requestId);
    if (pending.length === 0) {
      stampResponseBudgetSupport(response);
      stampRequestId(response, requestId);
      return response;
    }
    let responseBytes: number | undefined;
    try {
      const measured = await finalBodyBytes(response);
      responseBytes = measured.size;
      response = measured.response;
    } catch {
      // Unmeasurable body — the spec says omit the field rather than guess.
    }
    for (const emit of pending) emit(responseBytes);
    stampResponseBudgetSupport(response);
    stampRequestId(response, requestId);
    return response;
  };

  function stampResponseBudgetSupport(response: Response): void {
    try {
      response.headers.set(ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER, "true");
    } catch {
      // Upstream fetch responses may expose immutable headers. Those are not
      // VGI RPC responses generated by this handler.
    }
  }

  /**
   * Echo the caller's `X-Request-ID`, or mint one.
   *
   * Minted ids are 16 hex characters, matching the reference implementation's
   * `_generate_request_id`. Uniqueness is the whole contract: an id shared by
   * two requests joins records that describe different work, which is worse
   * than joining nothing.
   */
  function resolveRequestId(request: Request): string {
    const inbound = request.headers.get(REQUEST_ID_HEADER);
    const trimmed = inbound?.trim();
    if (trimmed) return trimmed;
    return crypto.randomUUID().replace(/-/g, "").slice(0, 16);
  }

  /**
   * Put the correlation id on the way out.
   *
   * Best-effort by construction: a `Response` handed back by `fetch` (the
   * OAuth token proxy returns one) carries immutable headers, and failing to
   * label a proxied response is not a reason to fail the request.
   */
  function stampRequestId(response: Response, requestId: string): void {
    try {
      response.headers.set(REQUEST_ID_HEADER, requestId);
    } catch {
      // immutable headers — nothing to do
    }
  }

  /**
   * Size the response body as it will go on the wire, without consuming it.
   *
   * A body already buffered by a runtime that stamped `Content-Length` costs
   * nothing to measure; otherwise it is read and re-wrapped, which is why
   * this runs only when a record is actually waiting on the number.
   */
  async function finalBodyBytes(response: Response): Promise<{ size: number | undefined; response: Response }> {
    const declared = response.headers.get("Content-Length");
    if (declared !== null) {
      const n = Number(declared);
      if (Number.isFinite(n) && n >= 0) return { size: n, response };
    }
    if (response.body === null) return { size: 0, response };
    const body = new Uint8Array(await response.arrayBuffer());
    return {
      size: body.byteLength,
      response: new Response(body as unknown as BodyInit, { status: response.status, headers: response.headers }),
    };
  }

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
