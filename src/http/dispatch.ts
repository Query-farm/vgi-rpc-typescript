// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { Schema, RecordBatch } from "apache-arrow";
import type { MethodDefinition } from "../types.js";
import { OutputCollector } from "../types.js";
import { parseRequest } from "../wire/request.js";
import {
  buildResultBatch,
  buildErrorBatch,
  buildEmptyBatch,
} from "../wire/response.js";
import { buildDescribeBatch, DESCRIBE_SCHEMA } from "../dispatch/describe.js";
import { STATE_KEY } from "../constants.js";
import { serializeSchema } from "../util/schema.js";
import {
  HttpRpcError,
  serializeIpcStream,
  readRequestFromBody,
  arrowResponse,
} from "./common.js";
import { packStateToken, unpackStateToken } from "./token.js";
import type { StateSerializer } from "./types.js";

const EMPTY_SCHEMA = new Schema([]);

export interface DispatchContext {
  signingKey: Uint8Array;
  tokenTtl: number;
  serverId: string;
  maxStreamResponseBytes?: number;
  stateSerializer: StateSerializer;
}

/** Dispatch a __describe__ request. */
export function httpDispatchDescribe(
  protocolName: string,
  methods: Map<string, MethodDefinition>,
  serverId: string,
): Response {
  const { batch } = buildDescribeBatch(protocolName, methods, serverId);
  const body = serializeIpcStream(DESCRIBE_SCHEMA, [batch]);
  return arrowResponse(body);
}

/** Dispatch a unary HTTP request. */
export async function httpDispatchUnary(
  method: MethodDefinition,
  body: Uint8Array,
  ctx: DispatchContext,
): Promise<Response> {
  const schema = method.resultSchema;
  const { schema: reqSchema, batch: reqBatch } = await readRequestFromBody(body);
  const parsed = parseRequest(reqSchema, reqBatch);

  if (parsed.methodName !== method.name) {
    throw new HttpRpcError(
      `Method name in request '${parsed.methodName}' does not match URL '${method.name}'`,
      400,
    );
  }

  const out = new OutputCollector(schema, true, ctx.serverId, parsed.requestId);

  try {
    const result = await method.handler!(parsed.params, out);
    const resultBatch = buildResultBatch(schema, result, ctx.serverId, parsed.requestId);
    const batches = [...out.batches.map((b) => b.batch), resultBatch];
    return arrowResponse(serializeIpcStream(schema, batches));
  } catch (error: any) {
    const errBatch = buildErrorBatch(schema, error, ctx.serverId, parsed.requestId);
    return arrowResponse(serializeIpcStream(schema, [errBatch]), 500);
  }
}

/** Dispatch a stream init HTTP request (producer or exchange). */
export async function httpDispatchStreamInit(
  method: MethodDefinition,
  body: Uint8Array,
  ctx: DispatchContext,
): Promise<Response> {
  const isProducer =
    !method.inputSchema || method.inputSchema.fields.length === 0;
  const outputSchema = method.outputSchema!;
  const inputSchema = method.inputSchema ?? EMPTY_SCHEMA;

  const { schema: reqSchema, batch: reqBatch } = await readRequestFromBody(body);
  const parsed = parseRequest(reqSchema, reqBatch);

  if (parsed.methodName !== method.name) {
    throw new HttpRpcError(
      `Method name in request '${parsed.methodName}' does not match URL '${method.name}'`,
      400,
    );
  }

  // Init state
  let state: any;
  try {
    if (isProducer) {
      state = await method.producerInit!(parsed.params);
    } else {
      state = await method.exchangeInit!(parsed.params);
    }
  } catch (error: any) {
    const errSchema = method.headerSchema ?? EMPTY_SCHEMA;
    const errBatch = buildErrorBatch(errSchema, error, ctx.serverId, parsed.requestId);
    return arrowResponse(serializeIpcStream(errSchema, [errBatch]), 500);
  }

  // Support dynamic output schemas (same as pipe transport)
  const resolvedOutputSchema = state?.__outputSchema ?? outputSchema;
  const effectiveProducer = state?.__isProducer ?? isProducer;

  // Build header IPC stream if method has a header schema
  let headerBytes: Uint8Array | null = null;
  if (method.headerSchema && method.headerInit) {
    try {
      const headerOut = new OutputCollector(
        method.headerSchema,
        true,
        ctx.serverId,
        parsed.requestId,
      );
      const headerValues = method.headerInit(parsed.params, state, headerOut);
      const headerBatch = buildResultBatch(
        method.headerSchema,
        headerValues,
        ctx.serverId,
        parsed.requestId,
      );
      const headerBatches = [
        ...headerOut.batches.map((b) => b.batch),
        headerBatch,
      ];
      headerBytes = serializeIpcStream(method.headerSchema, headerBatches);
    } catch (error: any) {
      const errBatch = buildErrorBatch(
        method.headerSchema,
        error,
        ctx.serverId,
        parsed.requestId,
      );
      return arrowResponse(serializeIpcStream(method.headerSchema, [errBatch]), 500);
    }
  }

  if (effectiveProducer) {
    return produceStreamResponse(
      method,
      state,
      resolvedOutputSchema,
      inputSchema,
      ctx,
      parsed.requestId,
      headerBytes,
    );
  } else {
    // Exchange: serialize state into signed token, return zero-row batch with token
    const stateBytes = ctx.stateSerializer.serialize(state);
    const schemaBytes = serializeSchema(resolvedOutputSchema);
    const inputSchemaBytes = serializeSchema(inputSchema);
    const token = packStateToken(
      stateBytes,
      schemaBytes,
      inputSchemaBytes,
      ctx.signingKey,
    );

    const tokenMeta = new Map<string, string>();
    tokenMeta.set(STATE_KEY, token);
    const tokenBatch = buildEmptyBatch(resolvedOutputSchema, tokenMeta);
    const tokenStreamBytes = serializeIpcStream(resolvedOutputSchema, [tokenBatch]);

    let responseBody: Uint8Array;
    if (headerBytes) {
      responseBody = concatBytes(headerBytes, tokenStreamBytes);
    } else {
      responseBody = tokenStreamBytes;
    }

    return arrowResponse(responseBody);
  }
}

/** Dispatch a stream exchange HTTP request (producer continuation or exchange round). */
export async function httpDispatchStreamExchange(
  method: MethodDefinition,
  body: Uint8Array,
  ctx: DispatchContext,
): Promise<Response> {
  const isProducer =
    !method.inputSchema || method.inputSchema.fields.length === 0;

  const { batch: reqBatch } = await readRequestFromBody(body);

  // Get state token from batch metadata
  const tokenBase64 = reqBatch.metadata?.get(STATE_KEY);
  if (!tokenBase64) {
    throw new HttpRpcError("Missing state token in exchange request", 400);
  }

  let unpacked;
  try {
    unpacked = unpackStateToken(tokenBase64, ctx.signingKey, ctx.tokenTtl);
  } catch (error: any) {
    throw new HttpRpcError(`Invalid state token: ${error.message}`, 400);
  }

  const state = ctx.stateSerializer.deserialize(unpacked.stateBytes);

  // Support dynamic output schemas (same as pipe transport)
  const outputSchema = state?.__outputSchema ?? method.outputSchema!;
  const inputSchema = method.inputSchema ?? EMPTY_SCHEMA;
  const effectiveProducer = state?.__isProducer ?? isProducer;

  if (effectiveProducer) {
    return produceStreamResponse(
      method,
      state,
      outputSchema,
      inputSchema,
      ctx,
      null,
      null,
    );
  } else {
    const out = new OutputCollector(outputSchema, false, ctx.serverId, null);

    try {
      await method.exchangeFn!(state, reqBatch, out);
    } catch (error: any) {
      const errBatch = buildErrorBatch(outputSchema, error, ctx.serverId, null);
      return arrowResponse(serializeIpcStream(outputSchema, [errBatch]), 500);
    }

    // Repack updated state into new token
    const stateBytes = ctx.stateSerializer.serialize(state);
    const schemaBytes = serializeSchema(outputSchema);
    const inputSchemaBytes = serializeSchema(inputSchema);
    const token = packStateToken(
      stateBytes,
      schemaBytes,
      inputSchemaBytes,
      ctx.signingKey,
    );

    // Merge token into the data batch's metadata (matching Python behavior).
    // The Python client expects the token on the data batch itself, not a
    // separate zero-row batch.
    const batches: RecordBatch[] = [];
    for (const emitted of out.batches) {
      const batch = emitted.batch;
      if (batch.numRows > 0) {
        // This is the data batch — merge token into its metadata
        const mergedMeta = new Map<string, string>(batch.metadata ?? []);
        mergedMeta.set(STATE_KEY, token);
        batches.push(new RecordBatch(batch.schema, batch.data, mergedMeta));
      } else {
        batches.push(batch);
      }
    }

    return arrowResponse(serializeIpcStream(outputSchema, batches));
  }
}

/** Run the producer loop and build the response. */
async function produceStreamResponse(
  method: MethodDefinition,
  state: any,
  outputSchema: Schema,
  inputSchema: Schema,
  ctx: DispatchContext,
  requestId: string | null,
  headerBytes: Uint8Array | null,
): Promise<Response> {
  const allBatches: RecordBatch[] = [];
  const maxBytes = ctx.maxStreamResponseBytes;
  let estimatedBytes = 0;

  while (true) {
    const out = new OutputCollector(outputSchema, true, ctx.serverId, requestId);

    try {
      await method.producerFn!(state, out);
    } catch (error: any) {
      allBatches.push(buildErrorBatch(outputSchema, error, ctx.serverId, requestId));
      break;
    }

    for (const emitted of out.batches) {
      allBatches.push(emitted.batch);
      if (maxBytes != null) {
        estimatedBytes += emitted.batch.data.byteLength;
      }
    }

    if (out.finished) {
      break;
    }

    // Check byte budget — if exceeded, emit continuation token
    if (maxBytes != null && estimatedBytes >= maxBytes) {
      const stateBytes = ctx.stateSerializer.serialize(state);
      const schemaBytes = serializeSchema(outputSchema);
      const inputSchemaBytes = serializeSchema(inputSchema);
      const token = packStateToken(
        stateBytes,
        schemaBytes,
        inputSchemaBytes,
        ctx.signingKey,
      );
      const tokenMeta = new Map<string, string>();
      tokenMeta.set(STATE_KEY, token);
      allBatches.push(buildEmptyBatch(outputSchema, tokenMeta));
      break;
    }
  }

  const dataBytes = serializeIpcStream(outputSchema, allBatches);
  let responseBody: Uint8Array;
  if (headerBytes) {
    responseBody = concatBytes(headerBytes, dataBytes);
  } else {
    responseBody = dataBytes;
  }
  return arrowResponse(responseBody);
}

function concatBytes(...arrays: Uint8Array[]): Uint8Array {
  const totalLen = arrays.reduce((sum, a) => sum + a.length, 0);
  const result = new Uint8Array(totalLen);
  let offset = 0;
  for (const arr of arrays) {
    result.set(arr, offset);
    offset += arr.length;
  }
  return result;
}
