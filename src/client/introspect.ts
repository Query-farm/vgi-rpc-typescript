// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { Schema as ArrowSchema, type RecordBatch, type Schema } from "@query-farm/apache-arrow";
import { deserializeSchema as deserializeSchemaImpl } from "#vgi-rpc-arrow";
import { DESCRIBE_METHOD_NAME, PROTOCOL_NAME_KEY, PROTOCOL_VERSION_KEY } from "../constants.js";
import { RpcError } from "../errors.js";
import { ARROW_CONTENT_TYPE } from "../http/common.js";
import { buildRequestIpc, dispatchLogOrError, readResponseBatches } from "./ipc.js";
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
  /** Application protocol surface version surfaced by the server's
   *  __describe__ response. Empty string when the server did not declare
   *  a `protocolVersion`. */
  protocolVersion: string;
  methods: MethodInfo[];
}

/**
 * Deserialize a schema from IPC bytes (schema message + EOS).
 *
 * Must dispatch via `#vgi-rpc-arrow` so the resulting type instances are
 * the same impl (apache-arrow / flechette) as the rest of the active
 * backend. Using apache-arrow's `RecordBatchReader` directly here used to
 * silently mix impls: in browser builds the backend is flechette, and a
 * flechette builder receiving an apache-arrow `Binary` type defaults to
 * the wrong offsets buffer (Uint8Array instead of Int32Array) and emits
 * a 0-byte value where a populated binary column was expected. The
 * downstream symptom is "Tried reading schema message, was null or
 * length 0" from the server when it tries to open the (empty) binary
 * column as a nested IPC stream. See test/client/ipc-cross-impl.test.ts.
 */
function deserializeSchema(bytes: Uint8Array): Schema {
  return deserializeSchemaImpl(bytes) as unknown as Schema;
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
  const protocolVersion = meta?.get(PROTOCOL_VERSION_KEY) ?? "";

  // Slim DESCRIBE_VERSION 4 wire format (see dispatch/describe.ts):
  //   0:name 1:method_type 2:has_return 3:params_schema_ipc
  //   4:result_schema_ipc 5:has_header 6:header_schema_ipc 7:is_exchange
  const methods: MethodInfo[] = [];
  for (let i = 0; i < dataBatch.numRows; i++) {
    const name = dataBatch.getChildAt(0)!.get(i) as string;
    const methodType = dataBatch.getChildAt(1)!.get(i) as string;
    const _hasReturn = dataBatch.getChildAt(2)!.get(i) as boolean;
    const paramsIpc = dataBatch.getChildAt(3)!.get(i) as Uint8Array;
    const resultIpc = dataBatch.getChildAt(4)!.get(i) as Uint8Array;
    const hasHeader = dataBatch.getChildAt(5)!.get(i) as boolean;
    const headerIpc = dataBatch.getChildAt(6)?.get(i) as Uint8Array | null;
    // is_exchange (index 7) currently unused on the client side.

    const paramsSchema = await deserializeSchema(paramsIpc);
    const resultSchema = await deserializeSchema(resultIpc);

    const info: MethodInfo = {
      name,
      type: methodType as "unary" | "stream",
      paramsSchema,
      resultSchema,
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

  return { protocolName, protocolVersion, methods };
}

/**
 * Send a __describe__ request and return a ServiceDescription.
 */
export async function httpIntrospect(
  baseUrl: string,
  options?: {
    prefix?: string;
    authorization?: string;
    compressionLevel?: number;
    compressFn?: (data: Uint8Array, level: number) => Promise<Uint8Array>;
    decompressFn?: (data: Uint8Array) => Promise<Uint8Array>;
  },
): Promise<ServiceDescription> {
  const prefix = options?.prefix ?? "";
  const emptySchema = new ArrowSchema([]);
  const body = buildRequestIpc(emptySchema, {}, DESCRIBE_METHOD_NAME);

  const headers: Record<string, string> = { "Content-Type": ARROW_CONTENT_TYPE };
  if (options?.authorization) {
    headers.Authorization = options.authorization;
  }

  const level = options?.compressionLevel;
  const compressFn = options?.compressFn;
  const decompressFn = options?.decompressFn;
  let sendBody: Uint8Array = body;
  if (level != null && compressFn) {
    headers["Content-Encoding"] = "zstd";
    sendBody = await compressFn(body, level);
  }
  if (level != null && decompressFn) {
    headers["Accept-Encoding"] = "zstd";
  }

  const response = await fetch(`${baseUrl}${prefix}/${DESCRIBE_METHOD_NAME}`, {
    method: "POST",
    headers,
    body: sendBody as unknown as BodyInit,
  });
  if (response.status === 401) {
    throw new RpcError("AuthenticationError", "Authentication required", "");
  }

  let responseBody = new Uint8Array(await response.arrayBuffer());
  if (response.headers.get("Content-Encoding") === "zstd" && decompressFn) {
    responseBody = new Uint8Array(await decompressFn(responseBody));
  }
  const { batches } = await readResponseBatches(responseBody);

  return parseDescribeResponse(batches);
}
