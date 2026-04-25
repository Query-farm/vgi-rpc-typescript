// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { Schema } from "@query-farm/apache-arrow";
import { CANCEL_KEY } from "../constants.js";
import { type ExternalLocationConfig, maybeExternalizeBatch } from "../external.js";
import type { MethodDefinition } from "../types.js";
import { OutputCollector } from "../types.js";
import { conformBatchToSchema } from "../util/conform.js";
import type { IpcStreamReader } from "../wire/reader.js";
import { buildErrorBatch, buildResultBatch } from "../wire/response.js";
import type { IpcStreamWriter } from "../wire/writer.js";

const EMPTY_SCHEMA = new Schema([]);

/**
 * Dispatch a stream RPC call (producer or exchange).
 *
 * Producer streams (empty input schema):
 * - Client sends tick batches (empty schema, 0 rows)
 * - Server reads each tick, calls produce(state, out)
 * - Server writes output batch(es) for each tick
 * - When produce() calls out.finish(), server closes output stream
 *
 * Exchange streams (real input schema):
 * - Client sends data batches
 * - Server reads each batch, calls exchange(state, input, out)
 * - Server writes output batch(es) for each input
 * - Stream ends when client closes input (EOS)
 */
export async function dispatchStream(
  method: MethodDefinition,
  params: Record<string, any>,
  writer: IpcStreamWriter,
  reader: IpcStreamReader,
  serverId: string,
  requestId: string | null,
  externalConfig?: ExternalLocationConfig,
): Promise<void> {
  const isProducer = !!method.producerFn;

  let state: any;
  try {
    if (isProducer) {
      state = await method.producerInit!(params);
    } else {
      state = await method.exchangeInit!(params);
    }
  } catch (error: any) {
    const errSchema = method.headerSchema ?? EMPTY_SCHEMA;
    const errBatch = buildErrorBatch(errSchema, error, serverId, requestId);
    writer.writeStream(errSchema, [errBatch]);
    // Still need to consume the input stream from the client
    const inputSchema = await reader.openNextStream();
    if (inputSchema) {
      while ((await reader.readNextBatch()) !== null) {
        // drain
      }
    }
    return;
  }

  // Support dynamic output schemas: init may return state with __outputSchema
  // to override the method's registered output schema (needed for methods
  // like VGI's "init" that produce different schemas per invocation).
  const outputSchema = state?.__outputSchema ?? method.outputSchema!;

  // Effective producer mode: check state override (VGI "init" is registered as
  // exchange but may act as producer depending on the function type).
  const effectiveProducer = state?.__isProducer ?? isProducer;

  // Write header IPC stream if method has a header schema
  if (method.headerSchema && method.headerInit) {
    try {
      const headerOut = new OutputCollector(method.headerSchema, true, serverId, requestId);
      const headerValues = method.headerInit(params, state, headerOut);
      const headerBatch = buildResultBatch(method.headerSchema, headerValues, serverId, requestId);
      const headerBatches = [...headerOut.batches.map((b) => b.batch), headerBatch];
      writer.writeStream(method.headerSchema, headerBatches);
    } catch (error: any) {
      const errBatch = buildErrorBatch(method.headerSchema, error, serverId, requestId);
      writer.writeStream(method.headerSchema, [errBatch]);
      // Drain input stream so client doesn't hang
      const inputSchema = await reader.openNextStream();
      if (inputSchema) {
        while ((await reader.readNextBatch()) !== null) {}
      }
      return;
    }
  }

  // Open the input IPC stream (ticks or data from client)
  const inputSchema = await reader.openNextStream();
  if (!inputSchema) {
    const errBatch = buildErrorBatch(outputSchema, new Error("Expected input stream but got EOF"), serverId, requestId);
    writer.writeStream(outputSchema, [errBatch]);
    return;
  }

  // Use a single continuous IPC stream for all output (matching Python vgi-rpc).
  // DuckDB exchanges are ping-pong: one input batch → one output batch on the
  // same stream. We use IncrementalStream which writes bytes synchronously.
  const stream = writer.openStream(outputSchema);

  // Expected input schema for casting compatible types (e.g., decimal→double).
  // State.__inputSchema overrides the method-registration schema per call,
  // mirroring the __outputSchema pattern. Used by dynamic-input exchange
  // methods (e.g., VGI's init, which binds to a user-supplied input shape).
  const expectedInputSchema = state?.__inputSchema ?? method.inputSchema;

  try {
    while (true) {
      let inputBatch = await reader.readNextBatch();
      if (!inputBatch) break;

      // Client cancellation: if the input batch carries vgi_rpc.cancel metadata,
      // end the stream cleanly without calling the producer/exchange handler.
      // The onCancel hook (if registered) runs once so state objects can
      // release resources.
      if (inputBatch.metadata?.get(CANCEL_KEY)) {
        if (method.onCancel) {
          try {
            await method.onCancel(state);
          } catch (err) {
            console.debug?.(`onCancel hook failed: ${err instanceof Error ? err.message : err}`);
          }
        }
        break;
      }

      // Cast compatible input types when schema doesn't match exactly.
      // Gated on effectiveProducer (not isProducer) so methods that flip to
      // producer mode via state.__isProducer skip the conform entirely — the
      // tick batches they receive have a dummy shape that shouldn't be checked.
      // Any conformance failure falls through with the original batch; the
      // exchange handler owns input-shape validation if it cares.
      if (expectedInputSchema && !effectiveProducer && inputBatch.schema !== expectedInputSchema) {
        try {
          inputBatch = conformBatchToSchema(inputBatch, expectedInputSchema);
        } catch (e) {
          // Field name/count mismatch (TypeError) is a hard contract violation —
          // propagate as an RpcError so callers see a structured failure instead
          // of a downstream hang or silent garbage. Other conform failures (e.g.
          // type-cast issues for dynamic-input handlers) fall through with the
          // original batch — handlers that bind their input shape per-call
          // should set state.__inputSchema so the conform doesn't run at all.
          if (e instanceof TypeError) throw e;
          console.debug?.(`Schema conformance skipped: ${e instanceof Error ? e.message : e}`);
        }
      }

      const out = new OutputCollector(outputSchema, effectiveProducer, serverId, requestId);

      if (isProducer) {
        await method.producerFn!(state, out);
      } else {
        await method.exchangeFn!(state, inputBatch, out);
      }

      for (const emitted of out.batches) {
        let batch = emitted.batch;
        if (externalConfig) {
          batch = await maybeExternalizeBatch(batch, externalConfig);
        }
        stream.write(batch);
      }

      if (out.finished) {
        break;
      }
    }
  } catch (error: any) {
    stream.write(buildErrorBatch(outputSchema, error, serverId, requestId));
  }

  stream.close();

  // Drain remaining input so transport stays synchronized for next request.
  // Matches Python's _drain_stream() called after every streaming method.
  // Needed when the loop exits early (out.finished, error) while client
  // is still sending batches.
  try {
    while ((await reader.readNextBatch()) !== null) {}
  } catch {
    // Suppress errors during drain (broken pipe, etc.)
  }
}
