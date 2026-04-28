// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { RecordBatch, RecordBatchReader, Schema } from "@query-farm/apache-arrow";
import type { AuthContext } from "../auth.js";
import { CANCEL_KEY, STATE_KEY } from "../constants.js";
import { buildDescribeBatch, DESCRIBE_SCHEMA } from "../dispatch/describe.js";
import {
  type ExternalLocationConfig,
  isExternalLocationBatch,
  maybeExternalizeBatch,
  resolveExternalLocation,
} from "../external.js";
import type { MethodDefinition } from "../types.js";
import { OutputCollector } from "../types.js";
import { conformBatchToSchema } from "../util/conform.js";
import { serializeSchema } from "../util/schema.js";
import { applyDefaults, parseRequest } from "../wire/request.js";
import { buildEmptyBatch, buildErrorBatch, buildResultBatch } from "../wire/response.js";
import { appendCookieHeaders, arrowResponse, HttpRpcError, readRequestFromBody, serializeIpcStream } from "./common.js";
import { derivePrincipalKey, packStateToken, unpackStateToken } from "./token.js";
import type { StateSerializer } from "./types.js";

async function deserializeSchema(bytes: Uint8Array): Promise<Schema> {
  const reader = await RecordBatchReader.from(bytes);
  await reader.open();
  return reader.schema!;
}

const EMPTY_SCHEMA = new Schema([]);

export interface DispatchContext {
  signingKey: Uint8Array;
  tokenTtl: number;
  serverId: string;
  /** Producer-only soft wire-cap (deprecated alias for the producer-loop
   *  byte budget). Unary/exchange ignore this. */
  maxStreamResponseBytes?: number;
  /** Soft wire-cap for producer streams; hard wire-cap for unary/exchange.
   *  Externalised payloads do not count toward this. */
  maxResponseBytes?: number;
  /** Hard cap on bytes uploaded to external storage during one HTTP response. */
  maxExternalizedResponseBytes?: number;
  stateSerializer: StateSerializer;
  authContext?: AuthContext;
  externalLocation?: ExternalLocationConfig;
  /** Incoming HTTP request cookies.  Empty/absent on non-HTTP paths. */
  cookies?: ReadonlyMap<string, string>;
}

/** Predict the external upload size if maybeExternalizeBatch ran on this batch
 *  right now. Returns 0 when externalisation would not fire. Mirrors the
 *  threshold logic so a pre-flight check matches the real upload size. */
function predictExternalizeBytes(
  batch: RecordBatch,
  config: ExternalLocationConfig | undefined,
): number {
  if (!config?.storage) return 0;
  if (batch.numRows === 0) return 0;
  const size = batch.data.byteLength;
  const threshold = config.externalizeThresholdBytes ?? 1_048_576;
  if (size < threshold) return 0;
  return size;
}

/** Build an Arrow IPC stream containing only an EXCEPTION batch, wrapped in a
 *  500 response so common.ts/arrowResponse rewrites it to 200 + X-VGI-RPC-Error.
 *  Used for cap-overshoot strict-fail. */
function makeCapErrorResponse(schema: Schema, error: Error, ctx: DispatchContext): Response {
  const errBatch = buildErrorBatch(schema, error, ctx.serverId, null);
  const response = arrowResponse(serializeIpcStream(schema, [errBatch]), 500);
  (response as any).__dispatchError = error;
  return response;
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
  const { schema: reqSchema, batch: reqBatchRaw } = await readRequestFromBody(body);

  // If the client externalized the request payload, fetch the inner batch
  // and re-attach the outer dispatch metadata (method, version, request id)
  // before parsing parameters.  Mirrors the Python _read_request stage-1
  // behaviour in vgi_rpc/rpc/_wire.py.
  let reqBatch = reqBatchRaw;
  let effectiveSchema = reqSchema;
  if (ctx.externalLocation && isExternalLocationBatch(reqBatchRaw)) {
    const resolved = await resolveExternalLocation(reqBatchRaw, ctx.externalLocation);
    const mergedMeta = new Map<string, string>(resolved.metadata ?? []);
    for (const [k, v] of reqBatchRaw.metadata ?? []) {
      // Outer dispatch metadata wins for vgi_rpc.* keys (the inner batch
      // shouldn't carry them but if it does, the outer is authoritative).
      mergedMeta.set(k, v);
    }
    reqBatch = new RecordBatch(resolved.schema, resolved.data, mergedMeta);
    effectiveSchema = resolved.schema;
  }

  const parsed = parseRequest(effectiveSchema, reqBatch);

  if (parsed.methodName !== method.name) {
    throw new HttpRpcError(`Method name in request '${parsed.methodName}' does not match URL '${method.name}'`, 400);
  }

  applyDefaults(parsed.params, method.defaults);

  const out = new OutputCollector(schema, true, ctx.serverId, parsed.requestId, ctx.authContext, ctx.cookies);
  out.enableCookieSink();

  try {
    const result = await method.handler!(parsed.params, out);
    let resultBatch = buildResultBatch(schema, result, ctx.serverId, parsed.requestId);
    if (ctx.externalLocation) {
      // Pre-flight max_externalized_response_bytes BEFORE incurring the
      // upload — operator's intent is "don't emit data beyond this per
      // call," not "emit and then complain." Mirror the Python check.
      const predicted = predictExternalizeBytes(resultBatch, ctx.externalLocation);
      if (
        ctx.maxExternalizedResponseBytes != null &&
        predicted > ctx.maxExternalizedResponseBytes
      ) {
        const overshoot = new Error(
          `Externalised payload exceeds max_externalized_response_bytes (${predicted} > ${ctx.maxExternalizedResponseBytes}) for method '${method.name}'`,
        );
        overshoot.name = "RuntimeError";
        const response = makeCapErrorResponse(schema, overshoot, ctx);
        appendCookieHeaders(response.headers, out.drainResponseCookies());
        return response;
      }
      resultBatch = await maybeExternalizeBatch(resultBatch, ctx.externalLocation);
    }
    const batches = [...out.batches.map((b) => b.batch), resultBatch];
    const body = serializeIpcStream(schema, batches);
    // Hard wire-cap enforcement — overshoot replaces the response with a
    // fresh EXCEPTION-only stream.
    if (ctx.maxResponseBytes != null && body.byteLength > ctx.maxResponseBytes) {
      const overshoot = new Error(
        `HTTP body exceeds max_response_bytes (${body.byteLength} > ${ctx.maxResponseBytes}) for method '${method.name}'`,
      );
      overshoot.name = "RuntimeError";
      const response = makeCapErrorResponse(schema, overshoot, ctx);
      appendCookieHeaders(response.headers, out.drainResponseCookies());
      return response;
    }
    const response = arrowResponse(body);
    appendCookieHeaders(response.headers, out.drainResponseCookies());
    return response;
  } catch (error: any) {
    const errBatch = buildErrorBatch(schema, error, ctx.serverId, parsed.requestId);
    const response = arrowResponse(serializeIpcStream(schema, [errBatch]), 500);
    // Apply any cookies queued before the exception — matches Python's
    // "cookies-on-error" behavior.
    appendCookieHeaders(response.headers, out.drainResponseCookies());
    // Attach the error so the dispatch hook can see it
    (response as any).__dispatchError = error;
    return response;
  }
}

/** Dispatch a stream init HTTP request (producer or exchange). */
export async function httpDispatchStreamInit(
  method: MethodDefinition,
  body: Uint8Array,
  ctx: DispatchContext,
): Promise<Response> {
  const isProducer = !!method.producerFn;
  const outputSchema = method.outputSchema!;
  const inputSchema = method.inputSchema ?? EMPTY_SCHEMA;

  const { schema: reqSchema, batch: reqBatch } = await readRequestFromBody(body);
  const parsed = parseRequest(reqSchema, reqBatch);

  if (parsed.methodName !== method.name) {
    throw new HttpRpcError(`Method name in request '${parsed.methodName}' does not match URL '${method.name}'`, 400);
  }

  applyDefaults(parsed.params, method.defaults);

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
    const response = arrowResponse(serializeIpcStream(errSchema, [errBatch]), 500);
    (response as any).__dispatchError = error;
    return response;
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
        ctx.authContext,
        ctx.cookies,
      );
      const headerValues = method.headerInit(parsed.params, state, headerOut);
      const headerBatch = buildResultBatch(method.headerSchema, headerValues, ctx.serverId, parsed.requestId);
      const headerBatches = [...headerOut.batches.map((b) => b.batch), headerBatch];
      headerBytes = serializeIpcStream(method.headerSchema, headerBatches);
    } catch (error: any) {
      const errBatch = buildErrorBatch(method.headerSchema, error, ctx.serverId, parsed.requestId);
      const response = arrowResponse(serializeIpcStream(method.headerSchema, [errBatch]), 500);
      (response as any).__dispatchError = error;
      return response;
    }
  }

  if (effectiveProducer) {
    // Producer method — produce data inline in the init response.
    // For exchange-registered methods acting as producers (__isProducer),
    // produceStreamResponse falls back to exchangeFn with tick batches.
    return produceStreamResponse(method, state, resolvedOutputSchema, inputSchema, ctx, parsed.requestId, headerBytes);
  } else {
    // Exchange: serialize state into signed token, return zero-row batch with token
    const stateBytes = ctx.stateSerializer.serialize(state);
    const schemaBytes = serializeSchema(resolvedOutputSchema);
    const inputSchemaBytes = serializeSchema(inputSchema);
    const tokenKey = derivePrincipalKey(ctx.signingKey, ctx.authContext?.principal);
    const token = packStateToken(stateBytes, schemaBytes, inputSchemaBytes, tokenKey);

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
  const isProducer = !!method.producerFn;

  const { batch: reqBatch } = await readRequestFromBody(body);

  // Get state token from batch metadata
  const tokenBase64 = reqBatch.metadata?.get(STATE_KEY);
  if (!tokenBase64) {
    throw new HttpRpcError("Missing state token in exchange request", 400);
  }

  // Cancel signal — observed alongside the state token. Must be checked
  // before conformBatchToSchema so that zero-row empty-schema cancel batches
  // don't fail the cast.
  const cancelled = reqBatch.metadata?.get(CANCEL_KEY) != null;

  // Bind verification to the caller's identity — a token signed for principal
  // A will fail HMAC verification when replayed by principal B (or by an
  // anonymous caller, and vice versa).
  const tokenKey = derivePrincipalKey(ctx.signingKey, ctx.authContext?.principal);

  let unpacked: import("./token.js").UnpackedToken;
  try {
    unpacked = unpackStateToken(tokenBase64, tokenKey, ctx.tokenTtl);
  } catch (error: any) {
    throw new HttpRpcError(`Invalid state token: ${error.message}`, 400);
  }

  let state: any;
  try {
    state = ctx.stateSerializer.deserialize(unpacked.stateBytes);
  } catch (error: any) {
    console.error(`[httpDispatchStreamExchange] state deserialize error:`, error.message);
    throw new HttpRpcError(`State deserialization failed: ${error.message}`, 500);
  }

  // Recover schemas from the token (the state itself may not contain
  // Schema objects after JSON round-trip — always prefer the token).
  let outputSchema: Schema;
  if (unpacked.schemaBytes.length > 0) {
    outputSchema = await deserializeSchema(unpacked.schemaBytes);
  } else {
    outputSchema = state?.__outputSchema ?? method.outputSchema!;
  }
  let inputSchema: Schema;
  if (unpacked.inputSchemaBytes.length > 0) {
    inputSchema = await deserializeSchema(unpacked.inputSchemaBytes);
  } else {
    // state.__inputSchema mirrors the __outputSchema pattern — set by
    // dynamic-input exchange methods (e.g. VGI's init, which binds to a
    // user-supplied input shape per invocation). Matches the fix already
    // applied in src/dispatch/stream.ts for the subprocess path.
    inputSchema = state?.__inputSchema ?? method.inputSchema ?? EMPTY_SCHEMA;
  }
  const effectiveProducer = state?.__isProducer ?? isProducer;
  if (process.env.VGI_DISPATCH_DEBUG)
    console.error(
      `[httpDispatchStreamExchange] method=${method.name} effectiveProducer=${effectiveProducer} stateKeys=${Object.keys(state || {})}`,
    );

  if (cancelled) {
    // Client asked for cancellation. Invoke the optional hook once and
    // return an empty IPC stream (no continuation token) so the client
    // knows the stream has ended.
    if (method.onCancel) {
      try {
        await method.onCancel(state);
      } catch (err) {
        console.debug?.(`onCancel hook failed: ${err instanceof Error ? err.message : err}`);
      }
    }
    return arrowResponse(serializeIpcStream(outputSchema, []));
  }

  if (effectiveProducer) {
    // Producer continuation — produce more data inline.
    // For exchange-registered methods, falls back to exchangeFn with tick batches.
    return produceStreamResponse(method, state, outputSchema, inputSchema, ctx, null, null);
  } else {
    // Exchange path — also handles exchange-registered methods acting as
    // producers (__isProducer=true). Use producer mode on the OutputCollector
    // when effectiveProducer so finish() is allowed.
    const out = new OutputCollector(outputSchema, effectiveProducer, ctx.serverId, null, ctx.authContext, ctx.cookies);

    // Cast compatible input types (e.g., decimal→double, int32→int64).
    // Gated on effectiveProducer (not isProducer) so methods that flip to
    // producer mode via state.__isProducer skip the conform entirely — the
    // tick batches they receive have a dummy shape that shouldn't be
    // checked against the declared input schema. Any conformance failure
    // falls through with the original batch; the handler owns input-shape
    // validation if it cares. Mirrors dispatch/stream.ts.
    let conformedBatch = reqBatch;
    if (!effectiveProducer && inputSchema !== EMPTY_SCHEMA && reqBatch.schema !== inputSchema) {
      try {
        conformedBatch = conformBatchToSchema(reqBatch, inputSchema);
      } catch (e) {
        // Field name/count mismatch is a hard contract violation — surface it
        // as an error rather than letting handlers see a wrong-shape batch
        // (mirrors the subprocess dispatch in src/dispatch/stream.ts).
        if (e instanceof TypeError) throw e;
        console.debug?.(`Schema conformance skipped: ${e instanceof Error ? e.message : e}`);
      }
    }

    try {
      if (method.exchangeFn) {
        await method.exchangeFn(state, conformedBatch, out);
      } else {
        await method.producerFn!(state, out);
      }
    } catch (error: any) {
      if (process.env.VGI_DISPATCH_DEBUG)
        console.error(
          `[httpDispatchStreamExchange] exchange handler error:`,
          error.message,
          error.stack?.split("\n").slice(0, 5).join("\n"),
        );
      const errBatch = buildErrorBatch(outputSchema, error, ctx.serverId, null);
      const response = arrowResponse(serializeIpcStream(outputSchema, [errBatch]), 500);
      (response as any).__dispatchError = error;
      return response;
    }

    // Collect emitted batches
    const batches: RecordBatch[] = [];

    if (out.finished) {
      // Stream is done — return data WITHOUT state token.
      // The absence of a token tells the client there's no more data.
      for (const emitted of out.batches) {
        batches.push(emitted.batch);
      }
    } else {
      // More data may follow — repack state into token for next exchange.
      const stateBytes = ctx.stateSerializer.serialize(state);
      const schemaBytes = serializeSchema(outputSchema);
      const inputSchemaBytes = serializeSchema(inputSchema);
      const token = packStateToken(stateBytes, schemaBytes, inputSchemaBytes, tokenKey);

      for (const emitted of out.batches) {
        const batch = emitted.batch;
        if (batch.numRows > 0) {
          const mergedMeta = new Map<string, string>(batch.metadata ?? []);
          mergedMeta.set(STATE_KEY, token);
          batches.push(new RecordBatch(batch.schema, batch.data, mergedMeta));
        } else {
          batches.push(batch);
        }
      }

      // Safety net: if no batch carries a state token (e.g. all rows were
      // filtered out by pushdown filters), emit an empty batch with the
      // token so the client knows to continue exchanging.
      if (!batches.some((b) => b.metadata?.get(STATE_KEY))) {
        const tokenMeta = new Map<string, string>();
        tokenMeta.set(STATE_KEY, token);
        batches.push(buildEmptyBatch(outputSchema, tokenMeta));
      }
    }

    const body = serializeIpcStream(outputSchema, batches);
    // Hard wire-cap enforcement for stream-exchange — overshoot replaces
    // the response with an EXCEPTION-only stream so the client surfaces RpcError.
    if (ctx.maxResponseBytes != null && body.byteLength > ctx.maxResponseBytes) {
      const overshoot = new Error(
        `HTTP body exceeds max_response_bytes (${body.byteLength} > ${ctx.maxResponseBytes}) for method '${method.name}'`,
      );
      overshoot.name = "RuntimeError";
      return makeCapErrorResponse(outputSchema, overshoot, ctx);
    }
    return arrowResponse(body);
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
  // Producer wire cap: prefer the legacy stream-only soft cap when set
  // (lets old callers keep the "one batch per response" hack alive),
  // else fall through to maxResponseBytes (which is hard for unary/
  // exchange but soft for producer — continuation tokens cover overshoot).
  const maxBytes = ctx.maxStreamResponseBytes ?? ctx.maxResponseBytes;
  let estimatedBytes = 0;
  let producerError: Error | undefined;

  while (true) {
    const out = new OutputCollector(outputSchema, true, ctx.serverId, requestId, ctx.authContext, ctx.cookies);

    try {
      if (method.producerFn) {
        await method.producerFn(state, out);
      } else {
        // Exchange-registered method acting as producer (e.g. VGI's "init"
        // method which is registered as exchange but may produce based on
        // the __isProducer state flag). Call exchangeFn with an empty tick
        // batch, matching how the subprocess transport dispatches these.
        const tickBatch = buildEmptyBatch(inputSchema);
        await method.exchangeFn!(state, tickBatch, out);
      }
    } catch (error: any) {
      if (process.env.VGI_DISPATCH_DEBUG)
        console.error(`[produceStreamResponse] error:`, error.message, error.stack?.split("\n").slice(0, 3).join("\n"));
      allBatches.push(buildErrorBatch(outputSchema, error, ctx.serverId, requestId));
      producerError = error instanceof Error ? error : new Error(String(error));
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
      const tokenKey = derivePrincipalKey(ctx.signingKey, ctx.authContext?.principal);
      const token = packStateToken(stateBytes, schemaBytes, inputSchemaBytes, tokenKey);
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
  const response = arrowResponse(responseBody);
  if (producerError) {
    (response as any).__dispatchError = producerError;
  }
  return response;
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
