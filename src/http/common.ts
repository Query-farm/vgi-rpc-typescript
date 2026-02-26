// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import {
  RecordBatchStreamWriter,
  RecordBatchReader,
  RecordBatch,
  Schema,
  Struct,
  makeData,
} from "apache-arrow";

export const ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream";

export class HttpRpcError extends Error {
  constructor(
    message: string,
    public readonly statusCode: number,
  ) {
    super(message);
    this.name = "HttpRpcError";
  }
}

/**
 * Rebuild a batch's data to match the given schema's field types.
 *
 * Batches deserialized from IPC streams (e.g., from PyArrow) may use generic
 * types (Float) instead of specific ones (Float64).  Arrow-JS's
 * RecordBatchStreamWriter silently drops batches whose child Data types don't
 * match the writer's schema.  Cloning each child Data with the schema's field
 * type fixes the type metadata while preserving the underlying buffers.
 */
function conformBatchToSchema(
  batch: RecordBatch,
  schema: Schema,
): RecordBatch {
  if (batch.numRows === 0) return batch;
  const children = schema.fields.map((f, i) =>
    batch.data.children[i].clone(f.type),
  );
  const structType = new Struct(schema.fields);
  const data = makeData({
    type: structType,
    length: batch.numRows,
    children,
    nullCount: batch.data.nullCount,
    nullBitmap: batch.data.nullBitmap,
  });
  return new RecordBatch(schema, data, batch.metadata);
}

/** Serialize a schema + batches into a complete IPC stream as Uint8Array. */
export function serializeIpcStream(
  schema: Schema,
  batches: RecordBatch[],
): Uint8Array {
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, schema);
  for (const batch of batches) {
    writer.write(conformBatchToSchema(batch, schema));
  }
  writer.close();
  return writer.toUint8Array(true);
}

/** Create a Response with Arrow IPC content type. Casts Uint8Array for TS lib compat. */
export function arrowResponse(body: Uint8Array, status = 200, extraHeaders?: Headers): Response {
  const headers = extraHeaders ?? new Headers();
  headers.set("Content-Type", ARROW_CONTENT_TYPE);
  return new Response(body as unknown as BodyInit, { status, headers });
}

/** Read schema + first batch from an IPC stream body. */
export async function readRequestFromBody(
  body: Uint8Array,
): Promise<{ schema: Schema; batch: RecordBatch }> {
  const reader = await RecordBatchReader.from(body);
  await reader.open();
  const schema = reader.schema;
  if (!schema) {
    throw new HttpRpcError("Empty IPC stream: no schema", 400);
  }
  const batches = reader.readAll();
  if (batches.length === 0) {
    throw new HttpRpcError("IPC stream contains no batches", 400);
  }
  return { schema, batch: batches[0] };
}
