// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { type RecordBatch, Schema } from "@query-farm/apache-arrow";
import { DEFAULT_ACCEPTED_MAX_RESPONSE_BYTES } from "#vgi-rpc-client-response-budget";
import { CALL_STATE_KEY, LOG_LEVEL_KEY, STATE_KEY } from "../constants.js";
import { RpcError } from "../errors.js";
import { isExternalLocationBatch, resolveExternalLocation } from "../external.js";
import { clientAcceptEncoding, VGI_ACCEPT_ENCODING_HEADER } from "../http/codec.js";
import { ARROW_CONTENT_TYPE } from "../http/common.js";
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
import { HttpStreamSession, unpackResumeToken } from "./stream.js";
import type { HttpConnectOptions, StreamSession } from "./types.js";
import { externalizeRequestBody } from "./uploadUrl.js";

type CompressFn = (data: Uint8Array, level: number) => Promise<Uint8Array>;
type DecompressFn = (data: Uint8Array) => Promise<Uint8Array>;

/** A connected RPC client, returned by {@link httpConnect}, {@link pipeConnect}, and {@link subprocessConnect}. */
export interface RpcClient {
  /** Invoke a unary method. Returns the single result row, or `null` for void methods. Parameter defaults from `__describe__` are applied automatically. */
  call(method: string, params?: Record<string, any>): Promise<Record<string, any> | null>;
  /** Open a streaming method, returning a {@link StreamSession} for exchange or producer iteration. */
  stream(method: string, params?: Record<string, any>): Promise<StreamSession>;
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
}

/**
 * Connect to a vgi-rpc server over HTTP. The returned client lazily introspects
 * the server (caching `__describe__`) on the first call and transparently handles
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
  const fetchFn = options?.fetch ?? globalThis.fetch;
  const acceptedMaxResponseBytes = options?.acceptedMaxResponseBytes ?? DEFAULT_ACCEPTED_MAX_RESPONSE_BYTES;
  optionalResponseBudget(acceptedMaxResponseBytes, "acceptedMaxResponseBytes");
  const effectiveExternalConfig = externalConfig ? { ...externalConfig, fetch: fetchFn } : externalConfig;

  let methodCache: Map<string, MethodInfo> | null = options?.description
    ? new Map(options.description.methods.map((method) => [method.name, method]))
    : null;
  /** Application protocol surface version discovered via __describe__. When
   *  non-empty, the client emits it on every request as
   *  `vgi_rpc.protocol_version` so a versioned server can validate at the
   *  dispatch boundary. */
  let serverProtocolVersion = options?.description?.protocolVersion ?? "";
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
    return methodCache;
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

      const body = buildRequestIpc(info.paramsSchema, fullParams, method, { protocolVersion: serverProtocolVersion });
      const resp = await postWithExternalization(`${baseUrl}${prefix}/${method}`, body);
      checkAuth(resp);

      const responseBody = await readResponse(resp);
      const { batches } = await readResponseBatches(responseBody);

      // Process batches: dispatch logs, resolve external pointers, find result
      let resultBatch: RecordBatch | null = null;
      for (let batch of batches) {
        if (batch.numRows === 0) {
          // Check for external location pointer batch
          if (isExternalLocationBatch(batch as any)) {
            batch = (await resolveExternalLocation(batch as any, effectiveExternalConfig)) as any;
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

      const body = buildRequestIpc(info.paramsSchema, fullParams, method, { protocolVersion: serverProtocolVersion });
      const resp = await postWithExternalization(`${baseUrl}${prefix}/${method}/init`, body);
      checkAuth(resp);

      const responseBody = await readResponse(resp);

      // Parse the response: may contain header stream + data stream
      let header: Record<string, any> | null = null;
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

      if (info.headerSchema) {
        // Response may contain two concatenated IPC streams:
        // 1. Header stream
        // 2. Data stream (with state token and/or data batches)
        const reader = await readSequentialStreams(responseBody);

        // First stream: header
        const headerStream = await reader.readStream();
        if (headerStream) {
          for (const batch of headerStream.batches as any[]) {
            if (batch.numRows === 0) {
              dispatchLogOrError(batch, onLog);
              continue;
            }
            const rows = extractBatchRows(batch);
            if (rows.length > 0) {
              header = rows[0];
            }
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
        (pendingBatches.length > 0 ? pendingBatches[0].schema : null) ??
        info.outputSchema ??
        info.resultSchema;

      return new HttpStreamSession({
        baseUrl,
        prefix,
        method,
        stateToken,
        callStateToken,
        outputSchema,
        inputSchema: info.inputSchema,
        onLog,
        pendingBatches,
        finished,
        header,
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
      const { cursor, callToken } = unpackResumeToken(token);
      return new HttpStreamSession({
        baseUrl,
        prefix,
        method,
        stateToken: cursor,
        callStateToken: callToken,
        outputSchema: outputSchema ?? new Schema([]),
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
        authorization,
        compressionLevel,
        compressFn,
        decompressFn,
        acceptedMaxResponseBytes: responseReadLimit(),
        fetch: fetchFn,
        responseBudgetVerified: true,
      });
    },

    close(): void {
      // No-op (HTTP stateless)
    },
  };
}
