// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { randomBytes } from "node:crypto";
import { Schema } from "@query-farm/apache-arrow";
import type { AuthContext } from "../auth.js";
import { DESCRIBE_METHOD_NAME } from "../constants.js";
import type { Protocol } from "../protocol.js";
import { MethodType } from "../types.js";
import { zstdCompress, zstdDecompress } from "../util/zstd.js";
import { buildErrorBatch } from "../wire/response.js";
import { buildWwwAuthenticateHeader, oauthResourceMetadataToJson, wellKnownPath } from "./auth.js";
import { ARROW_CONTENT_TYPE, arrowResponse, HttpRpcError, serializeIpcStream } from "./common.js";
import {
  httpDispatchDescribe,
  httpDispatchStreamExchange,
  httpDispatchStreamInit,
  httpDispatchUnary,
} from "./dispatch.js";
import { type HttpHandlerOptions, jsonStateSerializer } from "./types.js";

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
  const serverId = options?.serverId ?? crypto.randomUUID().replace(/-/g, "").slice(0, 12);

  const authenticate = options?.authenticate;
  const oauthMetadata = options?.oauthResourceMetadata;

  const methods = protocol.getMethods();

  const compressionLevel = options?.compressionLevel;
  const stateSerializer = options?.stateSerializer ?? jsonStateSerializer;

  // ctx is built per-request to include authContext; base fields set here
  const baseCtx = {
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

  async function compressIfAccepted(response: Response, clientAcceptsZstd: boolean): Promise<Response> {
    if (compressionLevel == null || !clientAcceptsZstd) return response;
    const responseBody = new Uint8Array(await response.arrayBuffer());
    const compressed = zstdCompress(responseBody, compressionLevel);
    const headers = new Headers(response.headers);
    headers.set("Content-Encoding", "zstd");
    return new Response(compressed as unknown as BodyInit, {
      status: response.status,
      headers,
    });
  }

  function makeErrorResponse(error: Error, statusCode: number, schema: Schema = EMPTY_SCHEMA): Response {
    const errBatch = buildErrorBatch(schema, error, serverId, null);
    const body = serializeIpcStream(schema, [errBatch]);
    const resp = arrowResponse(body, statusCode);
    addCorsHeaders(resp.headers);
    return resp;
  }

  return async function handler(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    // Well-known endpoint: RFC 9728 OAuth Protected Resource Metadata
    if (oauthMetadata && path === wellKnownPath(prefix)) {
      if (request.method !== "GET") {
        return new Response("Method Not Allowed", { status: 405 });
      }
      const body = JSON.stringify(oauthResourceMetadataToJson(oauthMetadata));
      const headers = new Headers({
        "Content-Type": "application/json",
        "Cache-Control": "public, max-age=60",
      });
      addCorsHeaders(headers);
      return new Response(body, { status: 200, headers });
    }

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
      return new Response(`Unsupported Media Type: expected ${ARROW_CONTENT_TYPE}`, { status: 415 });
    }

    // Check request body size
    if (maxRequestBytes != null) {
      const contentLength = request.headers.get("Content-Length");
      if (contentLength && parseInt(contentLength, 10) > maxRequestBytes) {
        return new Response("Request body too large", { status: 413 });
      }
    }

    const clientAcceptsZstd = (request.headers.get("Accept-Encoding") ?? "").includes("zstd");

    // Read body, decompressing if needed
    let body = new Uint8Array(await request.arrayBuffer());
    const contentEncoding = request.headers.get("Content-Encoding");
    if (contentEncoding === "zstd") {
      body = zstdDecompress(body);
    }

    // Build per-request dispatch context
    const ctx = { ...baseCtx } as typeof baseCtx & { authContext?: AuthContext };

    // Authentication
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
          headers.set("WWW-Authenticate", buildWwwAuthenticateHeader(metadataUrl.toString(), oauthMetadata.clientId, oauthMetadata.clientSecret, oauthMetadata.useIdTokenAsBearer));
        }
        return new Response(error.message || "Unauthorized", { status: 401, headers });
      }
    }

    // Route: {prefix}/__describe__
    if (path === `${prefix}/${DESCRIBE_METHOD_NAME}`) {
      try {
        const response = httpDispatchDescribe(protocol.name, methods, serverId);
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

      addCorsHeaders(response.headers);
      return compressIfAccepted(response, clientAcceptsZstd);
    } catch (error: any) {
      if (error instanceof HttpRpcError) {
        return compressIfAccepted(makeErrorResponse(error, error.statusCode), clientAcceptsZstd);
      }
      return compressIfAccepted(makeErrorResponse(error, 500), clientAcceptsZstd);
    }
  };
}
