// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { Schema } from "apache-arrow";
import type { MethodDefinition } from "../types.js";
import { OutputCollector } from "../types.js";
import type { IpcStreamWriter } from "../wire/writer.js";
import type { IpcStreamReader } from "../wire/reader.js";
import { buildResultBatch, buildErrorBatch } from "../wire/response.js";

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
): Promise<void> {
  const isProducer =
    !method.inputSchema || method.inputSchema.fields.length === 0;

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
      const headerBatch = buildResultBatch(
        method.headerSchema,
        headerValues,
        serverId,
        requestId,
      );
      const headerBatches = [
        ...headerOut.batches.map((b) => b.batch),
        headerBatch,
      ];
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
    const errBatch = buildErrorBatch(
      outputSchema,
      new Error("Expected input stream but got EOF"),
      serverId,
      requestId,
    );
    writer.writeStream(outputSchema, [errBatch]);
    return;
  }

  // Use a single continuous IPC stream for all output (matching Python vgi-rpc).
  // DuckDB exchanges are ping-pong: one input batch → one output batch on the
  // same stream. We use IncrementalStream which writes bytes synchronously.
  const stream = writer.openStream(outputSchema);

  try {
    while (true) {
      const inputBatch = await reader.readNextBatch();
      if (!inputBatch) break;

      const out = new OutputCollector(outputSchema, effectiveProducer, serverId, requestId);

      if (isProducer) {
        await method.producerFn!(state, out);
      } else {
        await method.exchangeFn!(state, inputBatch, out);
      }

      for (const emitted of out.batches) {
        stream.write(emitted.batch);
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
