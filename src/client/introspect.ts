// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { RecordBatchReader, RecordBatch, type Schema } from "apache-arrow";
import {
  DESCRIBE_METHOD_NAME,
  PROTOCOL_NAME_KEY,
  DESCRIBE_VERSION_KEY,
} from "../constants.js";
import { ARROW_CONTENT_TYPE } from "../http/common.js";
import { buildRequestIpc, readResponseBatches, dispatchLogOrError } from "./ipc.js";
import { Schema as ArrowSchema } from "apache-arrow";
import type { LogMessage } from "./types.js";

export interface MethodInfo {
  name: string;
  type: "unary" | "stream";
  paramsSchema: Schema;
  resultSchema: Schema;
  inputSchema?: Schema;
  outputSchema?: Schema;
  headerSchema?: Schema;
  doc?: string;
  paramTypes?: Record<string, string>;
  defaults?: Record<string, any>;
}

export interface ServiceDescription {
  protocolName: string;
  methods: MethodInfo[];
}

/** Deserialize a schema from IPC bytes (schema message + EOS). */
async function deserializeSchema(bytes: Uint8Array): Promise<Schema> {
  const reader = await RecordBatchReader.from(bytes);
  await reader.open();
  return reader.schema!;
}

/**
 * Parse a __describe__ response from batches into a ServiceDescription.
 * Reusable across transports (HTTP, pipe, subprocess).
 */
export async function parseDescribeResponse(
  batches: RecordBatch[],
  onLog?: (msg: LogMessage) => void,
): Promise<ServiceDescription> {
  // Find the data batch (skip log/error batches)
  let dataBatch = null;
  for (const batch of batches) {
    if (batch.numRows === 0) {
      dispatchLogOrError(batch, onLog);
      continue;
    }
    dataBatch = batch;
  }

  if (!dataBatch) {
    throw new Error("Empty __describe__ response");
  }

  // Extract metadata from batch
  const meta = dataBatch.metadata;
  const protocolName = meta?.get(PROTOCOL_NAME_KEY) ?? "";

  const methods: MethodInfo[] = [];
  for (let i = 0; i < dataBatch.numRows; i++) {
    const name = dataBatch.getChildAt(0)!.get(i) as string; // name
    const methodType = dataBatch.getChildAt(1)!.get(i) as string; // method_type
    const doc = dataBatch.getChildAt(2)?.get(i) as string | null; // doc
    const hasReturn = dataBatch.getChildAt(3)!.get(i) as boolean; // has_return
    const paramsIpc = dataBatch.getChildAt(4)!.get(i) as Uint8Array; // params_schema_ipc
    const resultIpc = dataBatch.getChildAt(5)!.get(i) as Uint8Array; // result_schema_ipc
    const paramTypesJson = dataBatch.getChildAt(6)?.get(i) as string | null; // param_types_json
    const paramDefaultsJson = dataBatch.getChildAt(7)?.get(i) as string | null; // param_defaults_json
    const hasHeader = dataBatch.getChildAt(8)!.get(i) as boolean; // has_header
    const headerIpc = dataBatch.getChildAt(9)?.get(i) as Uint8Array | null; // header_schema_ipc

    const paramsSchema = await deserializeSchema(paramsIpc);
    const resultSchema = await deserializeSchema(resultIpc);

    let paramTypes: Record<string, string> | undefined;
    if (paramTypesJson) {
      try { paramTypes = JSON.parse(paramTypesJson); } catch {}
    }

    let defaults: Record<string, any> | undefined;
    if (paramDefaultsJson) {
      try { defaults = JSON.parse(paramDefaultsJson); } catch {}
    }

    const info: MethodInfo = {
      name,
      type: methodType as "unary" | "stream",
      paramsSchema,
      resultSchema,
      doc: doc ?? undefined,
      paramTypes,
      defaults,
    };

    // For stream methods, result_schema_ipc actually holds the output schema
    if (methodType === "stream") {
      info.outputSchema = resultSchema;
    }

    if (hasHeader && headerIpc) {
      info.headerSchema = await deserializeSchema(headerIpc);
    }

    methods.push(info);
  }

  return { protocolName, methods };
}

/**
 * Send a __describe__ request and return a ServiceDescription.
 */
export async function httpIntrospect(
  baseUrl: string,
  options?: { prefix?: string },
): Promise<ServiceDescription> {
  const prefix = options?.prefix ?? "/vgi";
  const emptySchema = new ArrowSchema([]);
  const body = buildRequestIpc(emptySchema, {}, DESCRIBE_METHOD_NAME);

  const response = await fetch(`${baseUrl}${prefix}/${DESCRIBE_METHOD_NAME}`, {
    method: "POST",
    headers: { "Content-Type": ARROW_CONTENT_TYPE },
    body: body as unknown as BodyInit,
  });

  const responseBody = new Uint8Array(await response.arrayBuffer());
  const { batches } = await readResponseBatches(responseBody);

  return parseDescribeResponse(batches);
}
