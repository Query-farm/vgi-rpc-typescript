// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { Schema } from "apache-arrow";
import { randomBytes } from "node:crypto";
import type { Protocol } from "../protocol.js";
import { MethodType } from "../types.js";
import { DESCRIBE_METHOD_NAME } from "../constants.js";
import { buildErrorBatch } from "../wire/response.js";
import { jsonStateSerializer, type HttpHandlerOptions } from "./types.js";
import {
  ARROW_CONTENT_TYPE,
  HttpRpcError,
  serializeIpcStream,
  arrowResponse,
} from "./common.js";
import {
  httpDispatchDescribe,
  httpDispatchUnary,
  httpDispatchStreamInit,
  httpDispatchStreamExchange,
} from "./dispatch.js";

const EMPTY_SCHEMA = new Schema([]);

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
  const prefix = (options?.prefix ?? "/vgi").replace(/\/+$/, "");
  const signingKey = options?.signingKey ?? randomBytes(32);
  const tokenTtl = options?.tokenTtl ?? 3600;
  const corsOrigins = options?.corsOrigins;
  const maxRequestBytes = options?.maxRequestBytes;
  const maxStreamResponseBytes = options?.maxStreamResponseBytes;
  const serverId =
    options?.serverId ?? crypto.randomUUID().replace(/-/g, "").slice(0, 12);

  const methods = protocol.getMethods();

  const stateSerializer = options?.stateSerializer ?? jsonStateSerializer;

  const ctx = {
    signingKey,
    tokenTtl,
    serverId,
    maxStreamResponseBytes,
    stateSerializer,
  };

  function addCorsHeaders(headers: Headers): void {
    if (corsOrigins) {
      headers.set("Access-Control-Allow-Origin", corsOrigins);
      headers.set("Access-Control-Allow-Methods", "POST, OPTIONS");
      headers.set("Access-Control-Allow-Headers", "Content-Type");
    }
  }

  function makeErrorResponse(
    error: Error,
    statusCode: number,
    schema: Schema = EMPTY_SCHEMA,
  ): Response {
    const errBatch = buildErrorBatch(schema, error, serverId, null);
    const body = serializeIpcStream(schema, [errBatch]);
    const resp = arrowResponse(body, statusCode);
    addCorsHeaders(resp.headers);
    return resp;
  }

  return async function handler(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    // CORS preflight
    if (request.method === "OPTIONS") {
      if (path === `${prefix}/__capabilities__`) {
        const headers = new Headers();
        addCorsHeaders(headers);
        if (maxRequestBytes != null) {
          headers.set("VGI-Max-Request-Bytes", String(maxRequestBytes));
        }
        return new Response(null, { status: 204, headers });
      }

      if (corsOrigins) {
        const headers = new Headers();
        addCorsHeaders(headers);
        return new Response(null, { status: 204, headers });
      }

      return new Response(null, { status: 405 });
    }

    if (request.method !== "POST") {
      return new Response("Method Not Allowed", { status: 405 });
    }

    // Validate Content-Type
    const contentType = request.headers.get("Content-Type");
    if (!contentType || !contentType.includes(ARROW_CONTENT_TYPE)) {
      return new Response(
        `Unsupported Media Type: expected ${ARROW_CONTENT_TYPE}`,
        { status: 415 },
      );
    }

    // Check request body size
    if (maxRequestBytes != null) {
      const contentLength = request.headers.get("Content-Length");
      if (contentLength && parseInt(contentLength) > maxRequestBytes) {
        return new Response("Request body too large", { status: 413 });
      }
    }

    // Read body, decompressing if needed
    let body = new Uint8Array(await request.arrayBuffer());
    const contentEncoding = request.headers.get("Content-Encoding");
    if (contentEncoding === "zstd") {
      body = Bun.zstdDecompress(body);
    }

    // Route: {prefix}/__describe__
    if (path === `${prefix}/${DESCRIBE_METHOD_NAME}`) {
      try {
        const response = httpDispatchDescribe(protocol.name, methods, serverId);
        addCorsHeaders(response.headers);
        return response;
      } catch (error: any) {
        return makeErrorResponse(error, 500);
      }
    }

    // Parse method name and sub-path from URL
    if (!path.startsWith(prefix + "/")) {
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
      const err = new Error(
        `Unknown method: '${methodName}'. Available methods: [${available.join(", ")}]`,
      );
      return makeErrorResponse(err, 404);
    }

    try {
      let response: Response;

      if (action === "call") {
        if (method.type !== MethodType.UNARY) {
          throw new HttpRpcError(
            `Method '${methodName}' is a stream method. Use /init and /exchange endpoints.`,
            400,
          );
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

      addCorsHeaders(response.headers);
      return response;
    } catch (error: any) {
      if (error instanceof HttpRpcError) {
        return makeErrorResponse(error, error.statusCode);
      }
      return makeErrorResponse(error, 500);
    }
  };
}
