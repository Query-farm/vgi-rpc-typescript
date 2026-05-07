// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { type ExternalLocationConfig, maybeExternalizeBatch } from "../external.js";
import type { MethodDefinition, TransportKind } from "../types.js";
import { OutputCollector } from "../types.js";
import { buildErrorBatch, buildResultBatch } from "../wire/response.js";
import type { IpcStreamWriter } from "../wire/writer.js";

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
  externalConfig?: ExternalLocationConfig,
  kind?: TransportKind,
): Promise<void> {
  const schema = method.resultSchema;
  const out = new OutputCollector(schema, true, serverId, requestId, undefined, undefined, kind);

  try {
    const result = await method.handler!(params, out);
    let resultBatch = buildResultBatch(schema, result, serverId, requestId);
    if (externalConfig) {
      resultBatch = await maybeExternalizeBatch(resultBatch, externalConfig);
    }
    // Collect log batches (from clientLog) + result batch
    const batches = [...out.batches.map((b) => b.batch), resultBatch];
    writer.writeStream(schema, batches);
  } catch (error: any) {
    const batch = buildErrorBatch(schema, error, serverId, requestId);
    writer.writeStream(schema, [batch]);
  }
}
