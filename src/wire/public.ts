// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Public wire-framing helpers for VGI-RPC intermediaries.
 *
 * vgi-rpc has two first-class roles: *client* (`connect`) and *server*
 * (`VgiRpcServer` / `serveStdio`). A third role — an **intermediary** (proxy,
 * router, gateway, test harness) — needs to read a request off the wire,
 * rewrite it, re-frame it for forwarding, and synthesize in-band error
 * responses, without standing up a full client or server.
 *
 * These helpers are the stable surface for that role, so intermediaries don't
 * reach into the internal request/response codec.
 *
 * Ported from `vgi_rpc.wire` in vgi-rpc (Python). The Python helpers are
 * synchronous; here the ones that walk an IPC body are `async`, because the
 * only multi-stream reader available on this side is stream-based.
 */

import {
  deserializeBatch,
  emptyBatchWithMetadata,
  singleRowBatchWithMetadata,
  type VgiBatch,
  type VgiSchema,
} from "../arrow/index.js";
import { buildRequestIpc, readSequentialStreams } from "../client/ipc.js";
import { LOG_LEVEL_KEY, PROTOCOL_VERSION_KEY, STATE_KEY } from "../constants.js";
import type { ExternalLocationConfig } from "../external.js";
import { resolveExternalLocation } from "../external.js";
import { serializeIpcStream } from "../http/common.js";
import { parseRequest } from "./request.js";
import { buildErrorBatch } from "./response.js";

/** A request parsed off the wire by {@link readRequest}. */
export interface WireRequest {
  /** The dispatched method name (`vgi_rpc.method`). */
  methodName: string;
  /** The method's parameters, keyed by field name. */
  params: Record<string, any>;
  /** The request batch's schema — the method's parameter schema. */
  schema: VgiSchema;
}

/**
 * Parse a request IPC body into its method name and parameters.
 *
 * `externalConfig` resolves externalized (`vgi_rpc.location`) pointer requests
 * by fetching the referenced bytes; omitting it disables resolution, so a
 * pointer request parses as its pointer batch — callers that require
 * resolution should treat that as a fail-closed denial.
 */
export async function readRequest(data: Uint8Array, externalConfig?: ExternalLocationConfig): Promise<WireRequest> {
  let batch: VgiBatch = deserializeBatch(data);
  if (externalConfig) {
    batch = await resolveExternalLocation(batch, externalConfig);
  }
  const parsed = parseRequest(batch.schema, batch);
  return { methodName: parsed.methodName, params: parsed.params, schema: batch.schema };
}

/**
 * Frame a request as a complete IPC stream body for forwarding.
 *
 * Pass `protocolVersion` to stamp the application protocol version on the
 * request, so a versioned server's dispatch-boundary check still sees the
 * originating client's version. Omit it to emit a request that is structurally
 * exempt from that check (the key is simply absent).
 */
export function writeRequest(
  methodName: string,
  paramsSchema: VgiSchema,
  params: Record<string, any>,
  protocolVersion?: string,
): Uint8Array {
  return buildRequestIpc(paramsSchema as any, params, methodName, { protocolVersion });
}

/**
 * Build a complete IPC stream carrying a single error batch.
 *
 * This is the wire shape an intermediary returns to deny or abort a call
 * in-band — the client decodes it back into a thrown error.
 */
export function buildErrorStream(
  error: Error,
  schema: VgiSchema,
  serverId = "",
  requestId: string | null = null,
): Uint8Array {
  return serializeIpcStream(schema, [buildErrorBatch(schema, error, serverId, requestId)]);
}

/**
 * Return the stream-state continuation token carried in a request/response body.
 *
 * The token (`vgi_rpc.stream_state#b64`) rides in a record batch's
 * `custom_metadata` — not a header. Stream continuations recover their state
 * from it, never from headers, so an intermediary routing or correlating a
 * stream by it must read the batch metadata.
 *
 * Two body shapes are handled by one walk: an **exchange request** is a single
 * IPC stream whose first batch carries the token; a **producer init/exchange
 * response** may be several concatenated IPC streams (a header stream followed
 * by the producer's data stream), so the token can be in a later stream.
 *
 * Returns the first token found across all concatenated streams, or `null` when
 * absent or the body is unparseable. For a response that rotates the token
 * across several data batches the *last* token is the continuation the peer
 * will send next; this returns the first. Single-token responses (the common
 * case) make them identical.
 */
export function findStateToken(data: Uint8Array): Promise<string | null> {
  return findBatchMetadataValue(data, STATE_KEY);
}

/**
 * Return the application `protocol_version` stamped on a request body.
 *
 * The twin of {@link findStateToken}. An intermediary that rewrites a request
 * must recover and re-stamp this (see {@link writeRequest}) so the backend's
 * dispatch-boundary version check still sees the originating client's version.
 * Returns `null` when absent or unparseable — a request that never carried one
 * is structurally exempt from the check.
 */
export function findProtocolVersion(data: Uint8Array): Promise<string | null> {
  return findBatchMetadataValue(data, PROTOCOL_VERSION_KEY);
}

// Walk every concatenated IPC stream in `data`, returning the first non-empty
// value of `key` found in any batch's custom_metadata.
async function findBatchMetadataValue(data: Uint8Array, key: string): Promise<string | null> {
  try {
    const reader = await readSequentialStreams(data);
    while (true) {
      const stream = await reader.readStream();
      if (!stream) return null;
      for (const batch of stream.batches) {
        const value = batch.metadata?.get(key);
        if (value) return value;
      }
    }
  } catch {
    return null;
  }
}

/** A unary response unwrapped by {@link readUnaryResult}. */
export interface UnaryResult {
  /** The response envelope schema (a single `result` field). */
  envelopeSchema: VgiSchema;
  /** The serialized response object, undecoded. */
  resultBytes: Uint8Array;
}

/**
 * Unwrap a unary-RPC response into its envelope schema and raw result bytes.
 *
 * A unary response is an IPC stream of zero or more leading **log batches**
 * (0-row, carrying `vgi_rpc.log_level`) followed by one data batch whose
 * `result` column holds the serialized response object. This returns the raw
 * bytes — no typed decode — for an intermediary that inspects or rewrites the
 * response and re-wraps it via {@link writeUnaryResult}.
 *
 * Lenient: returns `null` for an error, empty, or non-`result` stream, so the
 * caller can forward it unchanged.
 */
export async function readUnaryResult(data: Uint8Array): Promise<UnaryResult | null> {
  const reader = await readSequentialStreams(data);
  const stream = await reader.readStream();
  if (!stream) return null;
  for (const batch of stream.batches) {
    if (batch.numRows > 0) {
      const column = batch.getChild("result");
      if (!column) return null;
      const value = column.get(0);
      if (value == null) return null;
      return { envelopeSchema: batch.schema, resultBytes: value as Uint8Array };
    }
    // 0-row batch: a log batch is skipped, anything else terminates the walk.
    if (!batch.metadata?.has(LOG_LEVEL_KEY)) return null;
  }
  return null;
}

/**
 * Build a unary-RPC response IPC stream wrapping `resultBytes`.
 *
 * The inverse of {@link readUnaryResult}: emit a single data batch whose
 * `result` column carries `resultBytes` under `envelopeSchema`.
 */
export function writeUnaryResult(envelopeSchema: VgiSchema, resultBytes: Uint8Array): Uint8Array {
  const batch: VgiBatch =
    envelopeSchema.fields.length === 0
      ? emptyBatchWithMetadata(envelopeSchema, new Map())
      : singleRowBatchWithMetadata(envelopeSchema, { result: resultBytes }, new Map());
  return serializeIpcStream(envelopeSchema, [batch]);
}
