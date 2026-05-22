// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import {
  conformBatchToSchema,
  deserializeBatch,
  deserializeSchema as facadeDeserializeSchema,
  schema as makeSchema,
  serializeBatch,
  type VgiBatch,
  type VgiSchema,
  withBatchMetadata,
} from "../arrow/index.js";
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
import { OutputCollector, TransportKind } from "../types.js";
import { serializeSchema } from "../util/schema.js";
import { applyDefaults, parseRequest } from "../wire/request.js";
import { buildEmptyBatch, buildErrorBatch, buildResultBatch } from "../wire/response.js";
import { appendCookieHeaders, arrowResponse, HttpRpcError, readRequestFromBody, serializeIpcStream } from "./common.js";
import { packStateToken, unpackStateToken } from "./token.js";
import type { StateSerializer } from "./types.js";

async function deserializeSchema(bytes: Uint8Array): Promise<VgiSchema> {
  return facadeDeserializeSchema(bytes);
}

const EMPTY_SCHEMA = makeSchema([]);

export interface DispatchContext {
  tokenKey: Uint8Array;
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
  /** Transport identifier surfaced to handlers via CallContext.kind.
   *  Defaults to HTTP when unset (the only caller that overrides it is
   *  the AF_UNIX launcher path). */
  kind?: TransportKind;
  /** Per-request sticky-session sink. Installed by the handler when sticky
   *  is enabled and the dispatcher attaches it to the OutputCollector so
   *  `ctx.session` / `ctx.openSession` / `ctx.closeSession` work. */
  stickyContext?: import("../types.js").StickyContext;
}

/** Predict the external upload size if maybeExternalizeBatch ran on this batch
 *  right now. Returns 0 when externalisation would not fire. Mirrors the
 *  threshold logic so a pre-flight check matches the real upload size. */
function predictExternalizeBytes(batch: VgiBatch, config: ExternalLocationConfig | undefined): number {
  if (!config?.storage) return 0;
  if (batch.numRows === 0) return 0;
  // arrow-js exposes `.data.byteLength` for an O(1) batch-bytes estimate;
  // flechette doesn't surface this, but maybeExternalizeBatch will measure
  // exact size on the actual upload path. Best-effort prediction here.
  const size = (batch as any).data?.byteLength ?? 0;
  const threshold = config.externalizeThresholdBytes ?? 1_048_576;
  if (size < threshold) return 0;
  return size;
}

/** Build an Arrow IPC stream containing only an EXCEPTION batch, wrapped in a
 *  500 response so common.ts/arrowResponse rewrites it to 200 + X-VGI-RPC-Error.
 *  Used for cap-overshoot strict-fail. */
function makeCapErrorResponse(schema: VgiSchema, error: Error, ctx: DispatchContext): Response {
  const errBatch = buildErrorBatch(schema, error, ctx.serverId, null);
  const response = arrowResponse(serializeIpcStream(schema, [errBatch]), 500);
  (response as any).__dispatchError = error;
  return response;
}

/** Dispatch a __describe__ request. */
export async function httpDispatchDescribe(
  protocolName: string,
  methods: Map<string, MethodDefinition>,
  serverId: string,
  protocolVersion?: string,
): Promise<Response> {
  const { batch } = await buildDescribeBatch(protocolName, methods, serverId, protocolVersion);
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
    reqBatch = withBatchMetadata(resolved, mergedMeta);
    effectiveSchema = resolved.schema;
  }

  const parsed = parseRequest(effectiveSchema, reqBatch);

  if (parsed.methodName !== method.name) {
    throw new HttpRpcError(`Method name in request '${parsed.methodName}' does not match URL '${method.name}'`, 400);
  }

  applyDefaults(parsed.params, method.defaults);

  const externalizationEnabled = !!ctx.externalLocation?.storage;
  const out = new OutputCollector(
    schema,
    true,
    ctx.serverId,
    parsed.requestId,
    ctx.authContext,
    ctx.cookies,
    ctx.kind ?? TransportKind.HTTP,
    {
      // Unary is one-shot: the entire wire and external budgets are
      // available for this single emit.
      remainingResponseBytes: ctx.maxResponseBytes,
      remainingExternalizedResponseBytes: externalizationEnabled ? ctx.maxExternalizedResponseBytes : undefined,
      externalizationEnabled,
    },
  );
  out.enableCookieSink();
  if (ctx.stickyContext) out.attachStickyContext(ctx.stickyContext);

  try {
    const result = await method.handler!(parsed.params, out);
    let resultBatch = buildResultBatch(schema, result, ctx.serverId, parsed.requestId);
    if (ctx.externalLocation) {
      // Pre-flight max_externalized_response_bytes BEFORE incurring the
      // upload — operator's intent is "don't emit data beyond this per
      // call," not "emit and then complain." Mirror the Python check.
      const predicted = predictExternalizeBytes(resultBatch, ctx.externalLocation);
      if (ctx.maxExternalizedResponseBytes != null && predicted > ctx.maxExternalizedResponseBytes) {
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
  // Mirror the output-schema override for inputs: VGI's `init` registers
  // exchange with a dummy `_tick` inputSchema and binds the real per-call
  // input shape via state.__inputSchema. Without this, the dummy schema
  // gets baked into the state token and conformBatchToSchema rejects the
  // first real input batch on the next exchange round.
  const resolvedInputSchema = state?.__inputSchema ?? inputSchema;
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
        ctx.kind ?? TransportKind.HTTP,
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
    return produceStreamResponse(
      method,
      state,
      resolvedOutputSchema,
      resolvedInputSchema,
      ctx,
      parsed.requestId,
      headerBytes,
    );
  } else {
    // Exchange: serialize state into signed token, return zero-row batch with token
    const stateBytes = ctx.stateSerializer.serialize(state);
    const schemaBytes = serializeSchema(resolvedOutputSchema);
    const inputSchemaBytes = serializeSchema(resolvedInputSchema);
    const token = packStateToken(stateBytes, schemaBytes, inputSchemaBytes, ctx.tokenKey, ctx.authContext?.principal);

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

  // Bind verification to the caller's identity — a token sealed for
  // principal A will fail AEAD decryption when replayed by principal B
  // (or by an anonymous caller, and vice versa).
  let unpacked: import("./token.js").UnpackedToken;
  try {
    unpacked = unpackStateToken(tokenBase64, ctx.tokenKey, ctx.tokenTtl, ctx.authContext?.principal);
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
  let outputSchema: VgiSchema;
  if (unpacked.schemaBytes.length > 0) {
    outputSchema = await deserializeSchema(unpacked.schemaBytes);
  } else {
    outputSchema = state?.__outputSchema ?? method.outputSchema!;
  }
  let inputSchema: VgiSchema;
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
    const externalizationEnabled = !!ctx.externalLocation?.storage;
    const out = new OutputCollector(
      outputSchema,
      effectiveProducer,
      ctx.serverId,
      null,
      ctx.authContext,
      ctx.cookies,
      ctx.kind ?? TransportKind.HTTP,
      {
        // Exchange is lockstep: one process() call, one output batch,
        // one HTTP response.  The whole budget belongs to this emit.
        remainingResponseBytes: ctx.maxResponseBytes,
        remainingExternalizedResponseBytes: externalizationEnabled ? ctx.maxExternalizedResponseBytes : undefined,
        externalizationEnabled,
      },
    );
    if (ctx.stickyContext) out.attachStickyContext(ctx.stickyContext);

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
    const batches: VgiBatch[] = [];

    if (out.finished) {
      // Stream is done — return data WITHOUT state token.
      // The absence of a token tells the client there's no more data.
      for (const emitted of out.batches) {
        // Preserve per-emit metadata (vgi_batch_index,
        // vgi_partition_values#b64) as the RecordBatch custom_metadata.
        if (emitted.metadata && emitted.metadata.size > 0) {
          const md = new Map<string, string>(emitted.batch.metadata ?? []);
          for (const [k, v] of emitted.metadata) md.set(k, v);
          batches.push(withBatchMetadata(emitted.batch, md));
        } else {
          batches.push(emitted.batch);
        }
      }
    } else {
      // More data may follow — repack state into token for next exchange.
      const stateBytes = ctx.stateSerializer.serialize(state);
      const schemaBytes = serializeSchema(outputSchema);
      const inputSchemaBytes = serializeSchema(inputSchema);
      const token = packStateToken(stateBytes, schemaBytes, inputSchemaBytes, ctx.tokenKey, ctx.authContext?.principal);

      for (const emitted of out.batches) {
        const batch = emitted.batch;
        if (batch.numRows > 0) {
          const mergedMeta = new Map<string, string>(batch.metadata ?? []);
          // Fold in per-emit metadata (vgi_batch_index, vgi_partition_values#b64).
          if (emitted.metadata) for (const [k, v] of emitted.metadata) mergedMeta.set(k, v);
          mergedMeta.set(STATE_KEY, token);
          batches.push(withBatchMetadata(batch, mergedMeta));
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
  outputSchema: VgiSchema,
  inputSchema: VgiSchema,
  ctx: DispatchContext,
  requestId: string | null,
  headerBytes: Uint8Array | null,
): Promise<Response> {
  const allBatches: VgiBatch[] = [];
  // Producer wire cap: prefer the legacy stream-only soft cap when set
  // (lets old callers keep the "one batch per response" hack alive),
  // else fall through to maxResponseBytes (which is hard for unary/
  // exchange but soft for producer — continuation tokens cover overshoot).
  const maxBytes = ctx.maxStreamResponseBytes ?? ctx.maxResponseBytes;
  const maxExternalBytes = ctx.maxExternalizedResponseBytes;
  const externalizationEnabled = !!ctx.externalLocation?.storage;
  let estimatedBytes = 0;
  /** Cumulative external-channel bytes across iterations.  External cap is
   *  *hard* — externalised uploads have no continuation-token escape valve. */
  let cumulativeExternalBytes = 0;
  let producerError: Error | undefined;
  /** Set when the external cap is breached; the loop replaces the partial
   *  stream with an EXCEPTION batch and breaks. */
  let externalOvershoot: Error | undefined;

  while (true) {
    // Snapshot per-iteration budgets so the worker can size its emit.
    const remainingWire = maxBytes != null ? Math.max(0, maxBytes - estimatedBytes) : undefined;
    const remainingExternal =
      externalizationEnabled && maxExternalBytes != null
        ? Math.max(0, maxExternalBytes - cumulativeExternalBytes)
        : undefined;

    const out = new OutputCollector(
      outputSchema,
      true,
      ctx.serverId,
      requestId,
      ctx.authContext,
      ctx.cookies,
      ctx.kind ?? TransportKind.HTTP,
      {
        remainingResponseBytes: remainingWire,
        remainingExternalizedResponseBytes: remainingExternal,
        externalizationEnabled,
      },
    );
    if (ctx.stickyContext) out.attachStickyContext(ctx.stickyContext);

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
      let batch = emitted.batch;
      // Externalize before charging wire bytes — externalised payloads
      // ride on the side channel and only the small pointer batch ends
      // up on the wire.  Pre-flight check + cumulative accounting mirror
      // Python's _run_http_producer_turn so a worker exfiltrating big
      // batches via tiny pointer outputs still hits the external cap.
      if (externalizationEnabled && ctx.externalLocation) {
        const predicted = predictExternalizeBytes(batch, ctx.externalLocation);
        if (predicted > 0 && maxExternalBytes != null && cumulativeExternalBytes + predicted > maxExternalBytes) {
          externalOvershoot = new Error(
            `Externalised payload exceeds max_externalized_response_bytes (${cumulativeExternalBytes + predicted} > ${maxExternalBytes}) for method '${method.name}'`,
          );
          externalOvershoot.name = "RuntimeError";
          break;
        }
        if (predicted > 0) {
          batch = await maybeExternalizeBatch(batch, ctx.externalLocation);
          cumulativeExternalBytes += predicted;
        }
      }
      // Preserve per-emit metadata (vgi_batch_index, vgi_partition_values#b64)
      // as the RecordBatch custom_metadata so the C++ extension reads it.
      if (emitted.metadata && emitted.metadata.size > 0) {
        const md = new Map<string, string>(batch.metadata ?? []);
        for (const [k, v] of emitted.metadata) md.set(k, v);
        batch = withBatchMetadata(batch, md);
      }
      allBatches.push(batch);
      if (maxBytes != null) {
        // arrow-js exposes O(1) byteLength via batch.data; flechette has no
        // equivalent. Best-effort estimate via serializeBatch in the latter.
        let sz = (batch as any).data?.byteLength ?? 0;
        if (sz === 0) {
          // Either a zero-row externalisation pointer batch or a flechette
          // batch that doesn't expose `data.byteLength`. The pointer case
          // is real "work done" — the worker's emit became an upload, and
          // we still need to advance the wire-cap loop so it eventually
          // breaks. Use a serialized-size estimate; for pointer batches
          // this captures the metadata-bearing zero-row body, for plain
          // batches it's the actual wire size.
          try {
            sz = serializeBatch(batch).byteLength;
          } catch {
            sz = 0;
          }
          // Producer cancellation contract: the loop MUST make progress
          // every iteration so an externalized infinite producer (e.g.,
          // `cancellable_producer` with externalize-threshold=1) eventually
          // mints a continuation token and lets the client cancel. Charge
          // at least 1 byte when neither byteLength nor serialization
          // gives us a real measurement.
          if (sz === 0) sz = 1;
        }
        estimatedBytes += sz;
      }
    }

    if (externalOvershoot) {
      // Replace the partial stream with a fresh one carrying only the
      // EXCEPTION batch — clients see RpcError before any data, matching
      // the unary/exchange strict-fail contract.
      allBatches.length = 0;
      allBatches.push(buildErrorBatch(outputSchema, externalOvershoot, ctx.serverId, requestId));
      producerError = externalOvershoot;
      break;
    }

    if (out.finished) {
      break;
    }

    // Check byte budget — if exceeded, emit continuation token
    if (maxBytes != null && estimatedBytes >= maxBytes) {
      const stateBytes = ctx.stateSerializer.serialize(state);
      const schemaBytes = serializeSchema(outputSchema);
      const inputSchemaBytes = serializeSchema(inputSchema);
      const token = packStateToken(stateBytes, schemaBytes, inputSchemaBytes, ctx.tokenKey, ctx.authContext?.principal);
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
  // External-cap overshoot is a strict-fail: emit 500 so arrowResponse
  // translates to 200 + X-VGI-RPC-Error.  In-handler producer errors
  // stay 200-with-EXCEPTION-batch (the existing contract — clients see
  // RpcError on body decode but proxies don't get the header signal).
  // Mirrors c5c7091 for the cap-overshoot path only.
  const status = externalOvershoot ? 500 : 200;
  const response = arrowResponse(responseBody, status);
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
