// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { RecordBatch, Schema } from "apache-arrow";
import { STATE_KEY, LOG_LEVEL_KEY, DESCRIBE_METHOD_NAME } from "../constants.js";
import { ARROW_CONTENT_TYPE } from "../http/common.js";
import {
  buildRequestIpc,
  readResponseBatches,
  dispatchLogOrError,
  extractBatchRows,
  readSequentialStreams,
} from "./ipc.js";
import { httpIntrospect, type MethodInfo, type ServiceDescription } from "./introspect.js";
import { HttpStreamSession } from "./stream.js";
import type { HttpConnectOptions, LogMessage, StreamSession } from "./types.js";

type CompressFn = (data: Uint8Array, level: number) => Uint8Array;
type DecompressFn = (data: Uint8Array) => Uint8Array;

export interface RpcClient {
  call(method: string, params?: Record<string, any>): Promise<Record<string, any> | null>;
  stream(method: string, params?: Record<string, any>): Promise<StreamSession>;
  describe(): Promise<ServiceDescription>;
  close(): void;
}

export function httpConnect(
  baseUrl: string,
  options?: HttpConnectOptions,
): RpcClient {
  const prefix = (options?.prefix ?? "/vgi").replace(/\/+$/, "");
  const onLog = options?.onLog;
  const compressionLevel = options?.compressionLevel;

  let methodCache: Map<string, MethodInfo> | null = null;
  let compressFn: CompressFn | undefined;
  let decompressFn: DecompressFn | undefined;
  let compressionLoaded = false;

  async function ensureCompression(): Promise<void> {
    if (compressionLoaded || compressionLevel == null) return;
    compressionLoaded = true;
    try {
      const mod = await import("../util/zstd.js");
      compressFn = mod.zstdCompress;
      decompressFn = mod.zstdDecompress;
    } catch {
      // zstd not available in this runtime
    }
  }

  function buildHeaders(): Record<string, string> {
    const headers: Record<string, string> = {
      "Content-Type": ARROW_CONTENT_TYPE,
    };
    if (compressionLevel != null) {
      headers["Content-Encoding"] = "zstd";
      headers["Accept-Encoding"] = "zstd";
    }
    return headers;
  }

  function prepareBody(content: Uint8Array): Uint8Array {
    if (compressionLevel != null && compressFn) {
      return compressFn(content, compressionLevel);
    }
    return content;
  }

  async function readResponse(resp: Response): Promise<Uint8Array<ArrayBuffer>> {
    let body = new Uint8Array(await resp.arrayBuffer());
    if (resp.headers.get("Content-Encoding") === "zstd" && decompressFn) {
      body = new Uint8Array(decompressFn(body));
    }
    return body;
  }

  async function ensureMethodCache(): Promise<Map<string, MethodInfo>> {
    if (methodCache) return methodCache;
    const desc = await httpIntrospect(baseUrl, { prefix });
    methodCache = new Map(desc.methods.map((m) => [m.name, m]));
    return methodCache;
  }

  return {
    async call(
      method: string,
      params?: Record<string, any>,
    ): Promise<Record<string, any> | null> {
      await ensureCompression();
      const methods = await ensureMethodCache();
      const info = methods.get(method);
      if (!info) {
        throw new Error(`Unknown method: '${method}'`);
      }

      // Apply defaults
      const fullParams = { ...(info.defaults ?? {}), ...(params ?? {}) };

      const body = buildRequestIpc(info.paramsSchema, fullParams, method);
      const resp = await fetch(`${baseUrl}${prefix}/${method}`, {
        method: "POST",
        headers: buildHeaders(),
        body: prepareBody(body) as unknown as BodyInit,
      });

      const responseBody = await readResponse(resp);
      const { batches } = await readResponseBatches(responseBody);

      // Process batches: dispatch logs, find result
      let resultBatch: RecordBatch | null = null;
      for (const batch of batches) {
        if (batch.numRows === 0) {
          dispatchLogOrError(batch, onLog);
          continue;
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

    async stream(
      method: string,
      params?: Record<string, any>,
    ): Promise<HttpStreamSession> {
      await ensureCompression();
      const methods = await ensureMethodCache();
      const info = methods.get(method);
      if (!info) {
        throw new Error(`Unknown method: '${method}'`);
      }

      // Apply defaults
      const fullParams = { ...(info.defaults ?? {}), ...(params ?? {}) };

      const body = buildRequestIpc(info.paramsSchema, fullParams, method);
      const resp = await fetch(`${baseUrl}${prefix}/${method}/init`, {
        method: "POST",
        headers: buildHeaders(),
        body: prepareBody(body) as unknown as BodyInit,
      });

      const responseBody = await readResponse(resp);

      // Parse the response: may contain header stream + data stream
      let header: Record<string, any> | null = null;
      let stateToken: string | null = null;
      const pendingBatches: RecordBatch[] = [];
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
          for (const batch of headerStream.batches) {
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
          streamSchema = dataStream.schema;
        }
        const headerErrorBatches: RecordBatch[] = [];
        if (dataStream) {
          for (const batch of dataStream.batches) {
            if (batch.numRows === 0) {
              // Check for state token
              const token = batch.metadata?.get(STATE_KEY);
              if (token) {
                stateToken = token;
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
            pendingBatches.push(batch);
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
          pendingBatches.push(batch);
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
        (streamSchema && streamSchema.fields.length > 0 ? streamSchema : null)
          ?? (pendingBatches.length > 0 ? pendingBatches[0].schema : null)
          ?? info.outputSchema ?? info.resultSchema;

      return new HttpStreamSession({
        baseUrl,
        prefix,
        method,
        stateToken,
        outputSchema,
        inputSchema: info.inputSchema,
        onLog,
        pendingBatches,
        finished,
        header,
        compressionLevel,
        compressFn,
        decompressFn,
      });
    },

    async describe(): Promise<ServiceDescription> {
      return httpIntrospect(baseUrl, { prefix });
    },

    close(): void {
      // No-op (HTTP stateless)
    },
  };
}
