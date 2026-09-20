// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import type { RecordBatch, Schema } from "@query-farm/apache-arrow";
import { schema as makeSchema } from "#vgi-rpc-arrow";
import { DEFAULT_ACCEPTED_MAX_RESPONSE_BYTES } from "#vgi-rpc-client-response-budget";
import { CALL_STATE_KEY, LOG_LEVEL_KEY, PROTOCOL_KEY, STATE_KEY } from "../constants.js";
import { RpcError } from "../errors.js";
import { isExternalLocationBatch, resolveExternalLocation } from "../external.js";
import { clientAcceptEncoding, VGI_ACCEPT_ENCODING_HEADER } from "../http/codec.js";
import {
  ARROW_CONTENT_TYPE,
  ECHO_HEADER_PREFIX,
  reservedPath,
  rpcPathFromPrefix,
  SESSION_ACCEPT_HEADER,
  SESSION_CLOSE_HEADER,
  SESSION_ENDPOINT,
  SESSION_HEADER,
} from "../http/common.js";
import { ACCEPT_MAX_RESPONSE_BYTES_HEADER, minPositive, optionalResponseBudget } from "../http/response-budget.js";
import {
  discoverHttpCapabilities,
  type HttpServerCapabilities,
  isCapabilitySnapshotFresh,
  requireResponseBudgetSupport,
} from "./capabilities.js";
import { decodeResponseBody, readResponseBodyBounded } from "./decode.js";
import { httpIntrospect, type MethodInfo, type ServiceDescription } from "./introspect.js";
import {
  buildRequestIpc,
  dispatchLogOrError,
  extractBatchRows,
  readResponseBatches,
  readSequentialStreams,
} from "./ipc.js";
import { serializeRequest } from "./outbound.js";
import type { RawBatch, RawStreamSession } from "./raw.js";
import { rawBatchOf, rawInputBatch } from "./raw-util.js";
import { HttpStreamSession, unpackResumeToken } from "./stream.js";
import type { HttpConnectOptions, StreamSession } from "./types.js";
import { externalizeRequestBody, requestUploadUrls, type UploadUrlPair } from "./uploadUrl.js";

type CompressFn = (data: Uint8Array, level: number) => Promise<Uint8Array>;
type DecompressFn = (data: Uint8Array) => Promise<Uint8Array>;

/** A connected RPC client, returned by {@link httpConnect}, {@link pipeConnect}, and {@link subprocessConnect}. */
export interface RpcClient {
  /** Invoke a unary method. Returns the single result row, or `null` for void methods. Parameter defaults from the server's description are applied automatically. */
  call(method: string, params?: Record<string, any>): Promise<Record<string, any> | null>;
  /** Open a streaming method, returning a {@link StreamSession} for exchange or producer iteration. */
  stream(method: string, params?: Record<string, any>): Promise<StreamSession>;
  /**
   * Invoke a unary method from an already-encoded request batch.
   *
   * The batch-level twin of {@link RpcClient.call}, for a caller that holds
   * encoded Arrow rather than values: `input` crosses verbatim (schema,
   * buffers and custom metadata alike), and the reply comes back as the
   * server encoded it. Resolves to `null` when the method returns nothing.
   *
   * `input.metadata` is the call's dispatch metadata and must already carry
   * `vgi_rpc.method` and `vgi_rpc.protocol`; nothing here supplies a default
   * for either. Unlike {@link RpcClient.call} this applies no parameter
   * defaults and needs no introspection round trip.
   */
  callRaw(method: string, input: RawBatch): Promise<RawBatch | null>;
  /**
   * Open a streaming method from an already-encoded request batch.
   *
   * `options.isExchange` and `options.hasHeader` come from the method's
   * declaration — this is the batch-level path, so there is no description to
   * read them from and no name-shaped guess worth making.
   */
  streamRaw(
    method: string,
    input: RawBatch,
    options: { isExchange: boolean; hasHeader: boolean },
  ): Promise<RawStreamSession>;
  /** Fetch the server's method/protocol description (cached after the first call). */
  describe(): Promise<ServiceDescription>;
  /** Release transport resources; for subprocess clients this also terminates the child process. */
  close(): void;
}

/** An HTTP-connected RPC client: {@link RpcClient} plus the HTTP-only continuation-resume surface. */
export interface HttpRpcClient extends RpcClient {
  /** Open a streaming method, returning an {@link HttpStreamSession} for exchange or producer iteration. */
  stream(method: string, params?: Record<string, any>): Promise<HttpStreamSession>;
  /**
   * Resume a producer stream from a continuation `token` without re-binding.
   *
   * A continuation request (`POST /{method}/exchange` carrying only the
   * `STATE_KEY` token) is fully self-describing: the server recovers the
   * producer state, schemas, and function identity from the signed token
   * alone, so no bind/init round-trip is needed. This is the cheap path for a
   * stateless relay that holds a per-batch token (see
   * {@link HttpStreamSession.nextWithToken}) and resumes on any
   * connection/node — unlike `stream(...)` which would produce and discard a
   * fresh first turn before seeking.
   *
   * `token` is the opaque blob from {@link HttpStreamSession.nextWithToken},
   * which packs both the cursor and the call token; the resuming node may
   * never have seen this stream's `/init`, so it needs both.
   *
   * The returned session is positioned at `token`; the first `nextWithToken()`
   * (or iteration) issues the continuation. `outputSchema` is unused on the
   * producer-continuation path (each response's IPC stream carries its own
   * schema) and defaults to the empty schema.
   *
   * Mirrors Python's `_HttpProxy.resume_stream`.
   */
  resumeStream(method: string, token: string, outputSchema?: Schema): Promise<HttpStreamSession>;
  /** Open a streaming method from an already-encoded request batch. */
  streamRaw(
    method: string,
    input: RawBatch,
    options: { isExchange: boolean; hasHeader: boolean },
  ): Promise<HttpStreamSession>;
  /** Discover what this server advertises on `OPTIONS {prefix}/health`. */
  capabilities(): Promise<HttpServerCapabilities>;
  /** Ask the server for `count` pre-signed upload/download URL pairs. */
  requestUploadUrls(count?: number): Promise<UploadUrlPair[]>;
  /**
   * Enter a sticky-session scope on this connection.
   *
   * Every subsequent request carries `VGI-Session-Accept: true` (the
   * server-side opt-in), the session token once the server has minted one,
   * and any `VGI-Echo-<name>` headers the server asked to have echoed back —
   * which is how a session survives a load balancer that has no cookie to
   * work with. Pass `token` to resume a session the server already holds.
   *
   * Scoped, not permanent: {@link HttpRpcClient.endSession} closes it.
   */
  beginSession(token?: string | null): void;
  /** The session token in flight, or `null` when no session is open. */
  currentSessionToken(): string | null;
  /**
   * The `VGI-Echo-*` values captured when the session opened, keyed by the
   * header name to replay them under. Empty when there are none.
   */
  currentEchoHeaders(): Record<string, string>;
  /**
   * Hand the session token to the caller and stop tracking it, so
   * {@link HttpRpcClient.endSession} leaves the server-side session alive for
   * a later {@link HttpRpcClient.beginSession} to resume.
   */
  detachSession(): string | null;
  /**
   * Leave the sticky-session scope, closing the server-side session with a
   * best-effort `DELETE {prefix}/__session__` unless it was detached.
   */
  endSession(): Promise<void>;
}

/**
 * Connect to a vgi-rpc server over HTTP. The returned client lazily introspects
 * the server via `vgi_rpc.Reflection.v1` (caching the result) on the first call and transparently handles
 * zstd compression, authorization, and 413 request externalization.
 */
export function httpConnect(rawBaseUrl: string, options?: HttpConnectOptions): HttpRpcClient {
  // Strip trailing slashes from the base URL for the same reason `prefix` does
  // below: every request path is built as `${baseUrl}${prefix}/${method}`, so a
  // base that already ends in "/" yields "https://host//method". Servers
  // predating the path normalization in `createHttpHandler` route that to the
  // method name "/method" and reject it as unknown — a confusing failure for
  // what is only a cosmetically different URL. Pasting a URL with a trailing
  // slash is the normal case, not an edge case.
  const baseUrl = rawBaseUrl.replace(/\/+$/, "");
  const prefix = (options?.prefix ?? "").replace(/\/+$/, "");
  const onLog = options?.onLog;
  const compressionLevel = options?.compressionLevel;
  const authorization = options?.authorization;
  const externalConfig = options?.externalLocation;
  const baseFetch = options?.fetch ?? globalThis.fetch;
  const acceptedMaxResponseBytes = options?.acceptedMaxResponseBytes ?? DEFAULT_ACCEPTED_MAX_RESPONSE_BYTES;
  optionalResponseBudget(acceptedMaxResponseBytes, "acceptedMaxResponseBytes");

  // --- Sticky sessions -----------------------------------------------------
  // Headers, not cookies, so several concurrent sessions to one host from one
  // client multiplex correctly. State lives here rather than in a wrapper
  // object because every HTTP path in this file — RPC, introspection,
  // capability discovery, upload URLs — already funnels through one fetch.
  //
  // A *stack* rather than one slot: scopes nest, and a nested scope must not
  // consume the enclosing one's token. Opening an inner session to observe a
  // rejection and then continuing on the outer session is an ordinary thing
  // to do, and with one slot the outer session simply vanished at the inner
  // scope's exit.
  interface SessionScope {
    token: string | null;
    echo: Record<string, string>;
  }
  const sessionScopes: SessionScope[] = [];

  function currentScope(): SessionScope | null {
    return sessionScopes.length === 0 ? null : sessionScopes[sessionScopes.length - 1];
  }

  function mergeSessionHeaders(scope: SessionScope, init: RequestInit | undefined): Headers {
    const headers = new Headers(init?.headers);
    headers.set(SESSION_ACCEPT_HEADER, "true");
    if (scope.token !== null) headers.set(SESSION_HEADER, scope.token);
    // Caller-supplied headers win: an operator overriding an echo header
    // per call has a reason to, and the normal path sets none of them.
    for (const [name, value] of Object.entries(scope.echo)) {
      if (!headers.has(name)) headers.set(name, value);
    }
    return headers;
  }

  function captureSessionHeaders(scope: SessionScope, response: Response): void {
    const token = response.headers.get(SESSION_HEADER);
    if (token) scope.token = token;
    const prefixLower = ECHO_HEADER_PREFIX.toLowerCase();
    response.headers.forEach((value, name) => {
      if (name.toLowerCase().startsWith(prefixLower)) {
        scope.echo[name.slice(ECHO_HEADER_PREFIX.length)] = value;
      }
    });
    if ((response.headers.get(SESSION_CLOSE_HEADER) ?? "").trim().toLowerCase() === "true") {
      scope.token = null;
      scope.echo = {};
    }
  }

  const fetchFn = (async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const scope = currentScope();
    if (scope === null) return baseFetch(input, init);
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    // An externalized body goes to storage with a PUT to a signed URL. Those
    // are not vgi-rpc requests, and unexpected headers can invalidate a
    // signature, so session headers stop at this connection's own base URL.
    if (!url.startsWith(baseUrl) || (init?.method ?? "GET").toUpperCase() === "PUT") {
      return baseFetch(input, init);
    }
    const response = await baseFetch(input, { ...init, headers: mergeSessionHeaders(scope, init) });
    captureSessionHeaders(scope, response);
    return response;
  }) as typeof globalThis.fetch;

  const effectiveExternalConfig = externalConfig ? { ...externalConfig, fetch: fetchFn } : externalConfig;

  let methodCache: Map<string, MethodInfo> | null = options?.description
    ? new Map(options.description.methods.map((method) => [method.name, method]))
    : null;
  /** Application protocol surface version discovered via reflection. When
   *  non-empty, the client emits it on every request as
   *  `vgi_rpc.protocol_version` so a versioned server can validate at the
   *  dispatch boundary. */
  let serverProtocolVersion = options?.description?.protocolVersion ?? "";
  // The routing key, learned from the same introspection response. Reflection
  // has a fixed name, so it is the bootstrap: ask the one protocol whose name a
  // client can know a priori what else the server speaks, then address that.
  let serverProtocolName = options?.description?.protocolName ?? "";

  /** `{prefix}/{protocol}` — the namespaced prefix every RPC path hangs off.
   *
   *  Folded once here rather than at each call site, matching the reference
   *  client. The reserved endpoints (`__upload_url__/init`, `health`,
   *  `__session__`) belong to the server rather than to any one protocol and
   *  stay on the flat `prefix`. */
  function rpcPrefix(): string {
    if (!serverProtocolName) {
      throw new RpcError(
        "ProtocolError",
        "The server did not report a protocol name, so no RPC path can be built. " +
          "Every request must name the protocol it addresses.",
        "",
      );
    }
    return `${prefix}/${serverProtocolName}`;
  }
  let compressFn: CompressFn | undefined;
  let decompressFn: DecompressFn | undefined;
  let compressionLoaded = false;
  let capabilities: HttpServerCapabilities | null = null;
  let responseBudgetSupport: Promise<void> | null = null;

  function updateCapabilitiesFromResponse(resp: Response): void {
    const next = requireResponseBudgetSupport(resp.headers);
    // Only treat the snapshot as authoritative when the server actually
    // emitted capability hints. Otherwise leave any prior cache in place.
    if (
      next.maxRequestBytes != null ||
      next.maxResponseBytes != null ||
      next.uploadUrlSupport ||
      next.acceptMaxResponseBytesSupport
    ) {
      capabilities = capabilities
        ? {
            ...next,
            maxRequestBytes: next.maxRequestBytes ?? capabilities.maxRequestBytes,
            maxResponseBytes: next.maxResponseBytes ?? capabilities.maxResponseBytes,
            maxUploadBytes: next.maxUploadBytes ?? capabilities.maxUploadBytes,
          }
        : next;
    }
  }

  async function ensureResponseBudgetSupport(): Promise<void> {
    if (!responseBudgetSupport) {
      responseBudgetSupport = discoverHttpCapabilities(
        baseUrl,
        prefix,
        authorization,
        acceptedMaxResponseBytes,
        fetchFn,
      )
        .then((snapshot) => {
          if (!snapshot.acceptMaxResponseBytesSupport) {
            throw new RpcError(
              "ProtocolError",
              "Server must advertise VGI-Accept-Max-Response-Bytes-Support: true before RPC dispatch",
              "",
            );
          }
          capabilities = snapshot;
        })
        .catch((error) => {
          responseBudgetSupport = null;
          throw error;
        });
    }
    await responseBudgetSupport;
  }

  function responseReadLimit(): number {
    return (
      minPositive(acceptedMaxResponseBytes, capabilities?.maxResponseBytes ?? undefined) ?? acceptedMaxResponseBytes
    );
  }

  async function maybeExternalize(body: Uint8Array): Promise<Uint8Array> {
    const caps = isCapabilitySnapshotFresh(capabilities) ? capabilities : null;
    if (!caps) return body;
    if (!caps.uploadUrlSupport) return body;
    if (caps.maxRequestBytes == null || body.byteLength <= caps.maxRequestBytes) return body;
    return externalizeRequestBody(body, {
      baseUrl,
      prefix,
      authorization,
      urlValidator: externalConfig?.urlValidator ?? null,
      fetch: fetchFn,
      acceptedMaxResponseBytes,
      responseBudgetVerified: true,
    });
  }

  /**
   * Send a POST request, transparently retrying with externalization if
   * the server returns 413 (Payload Too Large) and advertises upload-URL
   * support. Mirrors Python's 413 fallback in `_HttpProxy._post_with_externalization`.
   */
  async function postWithExternalization(url: string, body: Uint8Array): Promise<Response> {
    await ensureResponseBudgetSupport();
    const sendBody = await maybeExternalize(body);
    let resp = await fetchFn(url, {
      method: "POST",
      headers: buildHeaders(),
      body: (await prepareBody(sendBody)) as unknown as BodyInit,
    });
    updateCapabilitiesFromResponse(resp);

    if (resp.status === 413 && capabilities?.uploadUrlSupport && body.byteLength > 0) {
      // Refresh-and-retry: caps tell us we can externalize.
      const externalized = await externalizeRequestBody(body, {
        baseUrl,
        prefix,
        authorization,
        urlValidator: externalConfig?.urlValidator ?? null,
        fetch: fetchFn,
        acceptedMaxResponseBytes,
        responseBudgetVerified: true,
      });
      resp = await fetchFn(url, {
        method: "POST",
        headers: buildHeaders(),
        body: (await prepareBody(externalized)) as unknown as BodyInit,
      });
      updateCapabilitiesFromResponse(resp);
    }

    return resp;
  }

  async function ensureCompression(): Promise<void> {
    if (compressionLoaded || compressionLevel == null) return;
    try {
      const mod = await import("../util/zstd.js");
      compressFn = mod.zstdCompress;
      decompressFn = mod.zstdDecompress;
    } catch {
      // zstd not available in this runtime
    }
    compressionLoaded = true;
  }

  function buildHeaders(): Record<string, string> {
    const headers: Record<string, string> = {
      "Content-Type": ARROW_CONTENT_TYPE,
    };
    if (compressionLevel != null && compressFn) {
      headers["Content-Encoding"] = "zstd";
    }
    if (compressionLevel != null && decompressFn) {
      headers["Accept-Encoding"] = "zstd";
    }
    // Unconditional, and independent of `compressionLevel`: that option governs
    // whether we compress our *request* bodies, while this states what we can
    // decode on the way back. A server that cannot trust `Accept-Encoding` (see
    // clientAcceptEncoding) otherwise has to assume the worst and send identity.
    headers[VGI_ACCEPT_ENCODING_HEADER] = clientAcceptEncoding(decompressFn != null);
    headers[ACCEPT_MAX_RESPONSE_BYTES_HEADER] = String(acceptedMaxResponseBytes);
    if (authorization) {
      headers.Authorization = authorization;
    }
    return headers;
  }

  async function prepareBody(content: Uint8Array): Promise<Uint8Array> {
    if (compressionLevel != null && compressFn) {
      return await compressFn(content, compressionLevel);
    }
    return content;
  }

  function checkAuth(resp: Response): void {
    if (resp.status === 401) {
      throw new RpcError("AuthenticationError", "Authentication required", "");
    }
  }

  async function readResponse(resp: Response): Promise<Uint8Array<ArrayBuffer>> {
    const limit = responseReadLimit();
    const body = await readResponseBodyBounded(resp, limit);
    const decoded = new Uint8Array(await decodeResponseBody(resp.headers, body, decompressFn, limit));
    if (decoded.byteLength > limit) {
      throw new RpcError(
        "TransportError",
        `Decoded HTTP response exceeds accepted limit (${decoded.byteLength} > ${limit})`,
        "",
      );
    }
    return decoded;
  }

  async function ensureMethodCache(): Promise<Map<string, MethodInfo>> {
    if (methodCache) return methodCache;
    await ensureResponseBudgetSupport();
    await ensureCompression();
    const desc = await httpIntrospect(baseUrl, {
      prefix,
      protocol: options?.protocol,
      externalLocation: effectiveExternalConfig,
      authorization,
      compressionLevel,
      compressFn,
      decompressFn,
      acceptedMaxResponseBytes: responseReadLimit(),
      fetch: fetchFn,
      responseBudgetVerified: true,
    });
    methodCache = new Map(desc.methods.map((m) => [m.name, m]));
    serverProtocolVersion = desc.protocolVersion;
    serverProtocolName = desc.protocolName;
    return methodCache;
  }

  /**
   * Resolve the `{prefix}/{protocol}` an encoded call routes under.
   *
   * A caller holding an already-built request batch has already decided which
   * protocol it addresses: the routing key is in the batch's
   * `vgi_rpc.protocol` metadata, and an explicit `protocol` option names it
   * too. Either is authoritative and costs no round trip. Only when neither
   * is present does this fall back to introspection, which is what the
   * row-oriented path always does.
   */
  async function rawPrefix(metadata: ReadonlyMap<string, string>): Promise<string> {
    const named = options?.protocol ?? metadata.get(PROTOCOL_KEY);
    if (named) return `${prefix}/${named}`;
    await ensureMethodCache();
    return rpcPrefix();
  }

  /** What one `/init` response yielded, before any value decoding. */
  interface StreamInit {
    headerBatch: RecordBatch | null;
    stateToken: string | null;
    callStateToken: string | null;
    pendingBatches: RecordBatch[];
    finished: boolean;
    outputSchema: Schema | null;
  }

  /** POST one stream `/init` body and parse the response into {@link StreamInit}. */
  async function openStreamOverHttp(
    method: string,
    body: Uint8Array,
    hasHeader: boolean,
    prefixOverride?: string,
  ): Promise<StreamInit> {
    const resp = await postWithExternalization(
      baseUrl + rpcPathFromPrefix(prefixOverride ?? rpcPrefix(), method, { suffix: "/init" }),
      body,
    );
    checkAuth(resp);

    const responseBody = await readResponse(resp);

    let headerBatch: RecordBatch | null = null;
    let stateToken: string | null = null;
    // Only /init hands over a call token; the client keeps it for the
    // life of the stream and echoes it on every subsequent request.
    let callStateToken: string | null = null;
    const pendingBatches: RecordBatch[] = [];
    let dataBatchesInTurn = 0;
    const queueDataBatch = (batch: RecordBatch): void => {
      dataBatchesInTurn += 1;
      if (dataBatchesInTurn > 1) {
        throw new RpcError("ProtocolError", "A stream init returned more than one data batch", "");
      }
      pendingBatches.push(batch);
    };
    let finished = false;
    let streamSchema: Schema | null = null;

    if (hasHeader) {
      // Response may contain two concatenated IPC streams:
      // 1. Header stream
      // 2. Data stream (with state token and/or data batches)
      const reader = await readSequentialStreams(responseBody);

      // First stream: header
      const headerStream = await reader.readStream();
      if (headerStream) {
        for (const batch of headerStream.batches as any[]) {
          if (batch.numRows === 0) {
            // A header is data like any other, so a server that externalizes
            // its responses sends it as a pointer — which this dropped,
            // leaving `session.header` null against exactly the servers whose
            // headers are worth externalizing.
            if (isExternalLocationBatch(batch)) {
              headerBatch = (await resolveExternalLocation(batch, effectiveExternalConfig, onLog)) as any;
              continue;
            }
            dispatchLogOrError(batch, onLog);
            continue;
          }
          headerBatch = batch;
        }
      }

      // Second stream: data/state
      const dataStream = await reader.readStream();
      if (dataStream) {
        streamSchema = dataStream.schema as any;
      }
      const headerErrorBatches: RecordBatch[] = [];
      if (dataStream) {
        for (const batch of dataStream.batches as any[]) {
          if (batch.numRows === 0) {
            // Check for state token
            const token = batch.metadata?.get(STATE_KEY);
            if (token) {
              stateToken = token;
              callStateToken = batch.metadata?.get(CALL_STATE_KEY) ?? callStateToken;
              continue;
            }
            if (isExternalLocationBatch(batch)) {
              queueDataBatch(batch);
              continue;
            }
            const level = batch.metadata?.get(LOG_LEVEL_KEY);
            if (level === "EXCEPTION") {
              headerErrorBatches.push(batch);
              continue;
            }
            dispatchLogOrError(batch, onLog);
            continue;
          }
          queueDataBatch(batch);
        }
      }

      if (headerErrorBatches.length > 0) {
        if (pendingBatches.length > 0 || stateToken !== null) {
          pendingBatches.push(...headerErrorBatches);
        } else {
          for (const batch of headerErrorBatches) {
            dispatchLogOrError(batch, onLog);
          }
        }
      }

      if (!dataStream && !stateToken) {
        finished = true;
      }
    } else {
      // Single IPC stream: data/state (no header)
      const { schema: responseSchema, batches } = await readResponseBatches(responseBody);
      streamSchema = responseSchema;

      // Collect error batches separately — only defer them if there are
      // data batches or state tokens (mid-stream errors). Otherwise throw
      // immediately (init-only errors like exchange_error_on_init).
      const errorBatches: RecordBatch[] = [];

      for (const batch of batches) {
        if (batch.numRows === 0) {
          // Check for state token
          const token = batch.metadata?.get(STATE_KEY);
          if (token) {
            stateToken = token;
            callStateToken = batch.metadata?.get(CALL_STATE_KEY) ?? callStateToken;
            continue;
          }
          if (isExternalLocationBatch(batch)) {
            queueDataBatch(batch);
            continue;
          }
          // Collect EXCEPTION batches for deferred dispatch
          const level = batch.metadata?.get(LOG_LEVEL_KEY);
          if (level === "EXCEPTION") {
            errorBatches.push(batch);
            continue;
          }
          dispatchLogOrError(batch, onLog);
          continue;
        }
        queueDataBatch(batch);
      }

      // If we have data batches or a state token, defer errors to iteration.
      // Otherwise throw immediately (error on init).
      if (errorBatches.length > 0) {
        if (pendingBatches.length > 0 || stateToken !== null) {
          pendingBatches.push(...errorBatches);
        } else {
          // No data, no state — this is a pure init error. Throw now.
          for (const batch of errorBatches) {
            dispatchLogOrError(batch, onLog);
          }
        }
      }
    }

    if (pendingBatches.length === 0 && stateToken === null) {
      finished = true;
    }

    // Determine output schema: prefer the IPC stream schema from the init
    // response (it carries the server's actual output schema even for
    // zero-row token batches), then pending batch schemas, then describe info.
    const outputSchema =
      (streamSchema && streamSchema.fields.length > 0 ? streamSchema : null) ??
      (pendingBatches.length > 0 ? pendingBatches[0].schema : null);

    return { headerBatch, stateToken, callStateToken, pendingBatches, finished, outputSchema };
  }

  return {
    async call(method: string, params?: Record<string, any>): Promise<Record<string, any> | null> {
      await ensureCompression();
      const methods = await ensureMethodCache();
      const info = methods.get(method);
      if (!info) {
        throw new Error(`Unknown method: '${method}'`);
      }

      // Apply defaults
      const fullParams = { ...(info.defaults ?? {}), ...(params ?? {}) };

      const body = buildRequestIpc(info.paramsSchema, fullParams, method, {
        protocolVersion: serverProtocolVersion,
        protocol: serverProtocolName,
      });
      const resp = await postWithExternalization(baseUrl + rpcPathFromPrefix(rpcPrefix(), method), body);
      checkAuth(resp);

      const responseBody = await readResponse(resp);
      const { batches } = await readResponseBatches(responseBody);

      // Process batches: dispatch logs, resolve external pointers, find result
      let resultBatch: RecordBatch | null = null;
      for (let batch of batches) {
        if (batch.numRows === 0) {
          // Check for external location pointer batch
          if (isExternalLocationBatch(batch as any)) {
            batch = (await resolveExternalLocation(batch as any, effectiveExternalConfig, onLog)) as any;
          } else {
            dispatchLogOrError(batch, onLog);
            continue;
          }
        }
        if (resultBatch !== null) {
          throw new RpcError("ProtocolError", "A unary response returned more than one data batch", "");
        }
        resultBatch = batch;
      }

      if (!resultBatch) {
        // Void return (result schema has no fields)
        return null;
      }

      // Extract single-row result
      const rows = extractBatchRows(resultBatch);
      if (rows.length === 0) return null;

      const result = rows[0];
      // For void methods (empty result schema), return null
      if (info.resultSchema.fields.length === 0) return null;

      // For single-field results, return the whole object
      return result;
    },

    async stream(method: string, params?: Record<string, any>): Promise<HttpStreamSession> {
      await ensureCompression();
      const methods = await ensureMethodCache();
      const info = methods.get(method);
      if (!info) {
        throw new Error(`Unknown method: '${method}'`);
      }

      // Apply defaults
      const fullParams = { ...(info.defaults ?? {}), ...(params ?? {}) };

      const body = buildRequestIpc(info.paramsSchema, fullParams, method, {
        protocolVersion: serverProtocolVersion,
        protocol: serverProtocolName,
      });
      const init = await openStreamOverHttp(method, body, info.headerSchema != null);
      const header = init.headerBatch === null ? null : (extractBatchRows(init.headerBatch)[0] ?? null);

      return new HttpStreamSession({
        baseUrl,
        prefix: rpcPrefix(),
        method,
        stateToken: init.stateToken,
        callStateToken: init.callStateToken,
        outputSchema: init.outputSchema ?? info.outputSchema ?? info.resultSchema,
        inputSchema: info.inputSchema,
        onLog,
        pendingBatches: init.pendingBatches,
        finished: init.finished,
        header,
        rawHeader: init.headerBatch === null ? null : rawBatchOf(init.headerBatch),
        compressionLevel,
        compressFn,
        decompressFn,
        authorization,
        externalConfig: effectiveExternalConfig,
        acceptedMaxResponseBytes: responseReadLimit(),
        postFn: postWithExternalization,
      });
    },

    async callRaw(method: string, input: RawBatch): Promise<RawBatch | null> {
      await ensureCompression();
      const batch = rawInputBatch(input);
      const resp = await postWithExternalization(
        baseUrl + rpcPathFromPrefix(await rawPrefix(input.metadata), method),
        serializeRequest(batch.schema, [batch]),
      );
      checkAuth(resp);
      const responseBody = await readResponse(resp);
      const { batches } = await readResponseBatches(responseBody);

      let resultBatch: RecordBatch | null = null;
      for (let responseBatch of batches) {
        if (responseBatch.numRows === 0) {
          if (isExternalLocationBatch(responseBatch as any)) {
            responseBatch = (await resolveExternalLocation(
              responseBatch as any,
              effectiveExternalConfig,
              onLog,
            )) as any;
          } else {
            dispatchLogOrError(responseBatch, onLog);
            continue;
          }
        }
        if (resultBatch !== null) {
          throw new RpcError("ProtocolError", "A unary response returned more than one data batch", "");
        }
        resultBatch = responseBatch;
      }
      return resultBatch === null ? null : rawBatchOf(resultBatch);
    },

    async streamRaw(
      method: string,
      input: RawBatch,
      options: { isExchange: boolean; hasHeader: boolean },
    ): Promise<HttpStreamSession> {
      await ensureCompression();
      const batch = rawInputBatch(input);
      const prefixForCall = await rawPrefix(input.metadata);
      const init = await openStreamOverHttp(
        method,
        serializeRequest(batch.schema, [batch]),
        options.hasHeader,
        prefixForCall,
      );
      return new HttpStreamSession({
        baseUrl,
        prefix: prefixForCall,
        method,
        stateToken: init.stateToken,
        callStateToken: init.callStateToken,
        outputSchema: init.outputSchema ?? (makeSchema([]) as unknown as Schema),
        onLog,
        pendingBatches: init.pendingBatches,
        finished: init.finished,
        header: null,
        rawHeader: init.headerBatch === null ? null : rawBatchOf(init.headerBatch),
        compressionLevel,
        compressFn,
        decompressFn,
        authorization,
        externalConfig: effectiveExternalConfig,
        acceptedMaxResponseBytes: responseReadLimit(),
        postFn: postWithExternalization,
      });
    },

    async resumeStream(method: string, token: string, outputSchema?: Schema): Promise<HttpStreamSession> {
      // No bind/init round-trip: the continuation token alone identifies the
      // stream. ensureCompression is memoized and only probes when a
      // compressionLevel was requested and no call has run yet.
      await ensureCompression();
      await ensureResponseBudgetSupport();
      // The routing key is part of the `/exchange` path now, and a resumed
      // stream arrives with nothing but its tokens — so the protocol name has
      // to come from introspection even though the resume itself needs no
      // bind/init round-trip. Memoized, and a no-op when the caller supplied a
      // `description`.
      await ensureMethodCache();
      const { cursor, callToken } = unpackResumeToken(token);
      return new HttpStreamSession({
        baseUrl,
        prefix: rpcPrefix(),
        method,
        stateToken: cursor,
        callStateToken: callToken,
        outputSchema: outputSchema ?? (makeSchema([]) as unknown as Schema),
        onLog,
        pendingBatches: [],
        finished: false,
        header: null,
        compressionLevel,
        compressFn,
        decompressFn,
        authorization,
        externalConfig: effectiveExternalConfig,
        acceptedMaxResponseBytes: responseReadLimit(),
        postFn: postWithExternalization,
      });
    },

    async describe(): Promise<ServiceDescription> {
      await ensureCompression();
      await ensureResponseBudgetSupport();
      return httpIntrospect(baseUrl, {
        prefix,
        protocol: options?.protocol,
        externalLocation: effectiveExternalConfig,
        authorization,
        compressionLevel,
        compressFn,
        decompressFn,
        acceptedMaxResponseBytes: responseReadLimit(),
        fetch: fetchFn,
        responseBudgetVerified: true,
      });
    },

    async capabilities(): Promise<HttpServerCapabilities> {
      const snapshot = await discoverHttpCapabilities(
        baseUrl,
        prefix,
        authorization,
        acceptedMaxResponseBytes,
        fetchFn,
      );
      capabilities = snapshot;
      return snapshot;
    },

    async requestUploadUrls(count = 1): Promise<UploadUrlPair[]> {
      return requestUploadUrls(baseUrl, prefix, count, authorization, fetchFn, acceptedMaxResponseBytes);
    },

    beginSession(token?: string | null): void {
      sessionScopes.push({ token: token ? token : null, echo: {} });
    },

    currentSessionToken(): string | null {
      return currentScope()?.token ?? null;
    },

    currentEchoHeaders(): Record<string, string> {
      return { ...(currentScope()?.echo ?? {}) };
    },

    detachSession(): string | null {
      const scope = currentScope();
      if (scope === null) return null;
      const token = scope.token;
      scope.token = null;
      scope.echo = {};
      return token;
    },

    async endSession(): Promise<void> {
      const scope = sessionScopes.pop();
      if (scope === undefined) return;
      const token = scope.token;
      const echo = scope.echo;
      if (token === null) return;
      const headers: Record<string, string> = { [SESSION_HEADER]: token };
      if (authorization) headers.Authorization = authorization;
      for (const [name, value] of Object.entries(echo)) headers[name] = value;
      try {
        // Best-effort: the session's TTL releases it anyway, and a failure
        // here must not become the caller's error on the way out of a scope.
        await baseFetch(baseUrl + reservedPath(SESSION_ENDPOINT, { prefix }), { method: "DELETE", headers });
      } catch {
        // Ignored, deliberately.
      }
    },

    close(): void {
      // No-op (HTTP stateless)
    },
  };
}

/**
 * The error every method on an {@link RpcClient} throws, re-exported here.
 *
 * A client-only consumer would otherwise have to reach for the package root to
 * catch what this module's own functions raise, and the root re-exports the
 * whole framework — protocol, dispatch, access log, the server. A bundler
 * cannot drop it, so browsers shipped `RpcServer` to `instanceof`-check an
 * error class that has no imports of its own. `@query-farm/vgi` did exactly
 * that, and it cost its consumers ~160 kB.
 */
export { RpcError } from "../errors.js";
