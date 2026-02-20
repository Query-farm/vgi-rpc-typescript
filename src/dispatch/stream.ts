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
  const outputSchema = method.outputSchema!;

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

  // Open incremental output stream
  const outStream = writer.openStream(outputSchema);

  try {
    // Read input batches one at a time, process, and write output incrementally
    while (true) {
      const inputBatch = await reader.readNextBatch();
      if (!inputBatch) break; // Input stream ended (EOS)

      const out = new OutputCollector(outputSchema, isProducer, serverId, requestId);

      if (isProducer) {
        await method.producerFn!(state, out);
      } else {
        await method.exchangeFn!(state, inputBatch, out);
      }

      // Write emitted batches to the output stream
      for (const emitted of out.batches) {
        outStream.write(emitted.batch);
      }

      if (out.finished) {
        break;
      }
    }
  } catch (error: any) {
    const errBatch = buildErrorBatch(outputSchema, error, serverId, requestId);
    outStream.write(errBatch);
  } finally {
    outStream.close();
  }
}
