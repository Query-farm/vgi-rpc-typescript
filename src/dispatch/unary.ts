import { Schema } from "apache-arrow";
import type { MethodDefinition } from "../types.js";
import { OutputCollector } from "../types.js";
import type { IpcStreamWriter } from "../wire/writer.js";
import { buildResultBatch, buildErrorBatch } from "../wire/response.js";

/**
 * Dispatch a unary RPC call.
 * Calls the handler with parsed params, writes result or error batch.
 * Supports client-directed logging via ctx.clientLog().
 */
export async function dispatchUnary(
  method: MethodDefinition,
  params: Record<string, any>,
  writer: IpcStreamWriter,
  serverId: string,
  requestId: string | null,
): Promise<void> {
  const schema = method.resultSchema;
  const out = new OutputCollector(schema, true, serverId, requestId);

  try {
    const result = await method.handler!(params, out);
    const resultBatch = buildResultBatch(schema, result, serverId, requestId);
    // Collect log batches (from clientLog) + result batch
    const batches = [...out.batches.map((b) => b.batch), resultBatch];
    writer.writeStream(schema, batches);
  } catch (error: any) {
    const batch = buildErrorBatch(schema, error, serverId, requestId);
    writer.writeStream(schema, [batch]);
  }
}
