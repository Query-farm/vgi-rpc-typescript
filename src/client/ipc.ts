// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import {
  RecordBatch,
  RecordBatchReader,
  Schema,
  DataType,
  Float64,
  Int64,
  Utf8,
  Bool,
  Binary,
  vectorFromArray,
  makeData,
  Struct,
} from "apache-arrow";
import {
  RPC_METHOD_KEY,
  REQUEST_VERSION_KEY,
  REQUEST_VERSION,
  LOG_LEVEL_KEY,
  LOG_MESSAGE_KEY,
  LOG_EXTRA_KEY,
} from "../constants.js";
import { RpcError } from "../errors.js";
import { serializeIpcStream } from "../http/common.js";
import { IpcStreamReader, type StreamMessage } from "../wire/reader.js";
import type { LogMessage } from "./types.js";

/** Infer an Arrow DataType from a JS value. */
export function inferArrowType(value: any): DataType {
  if (typeof value === "string") return new Utf8();
  if (typeof value === "boolean") return new Bool();
  if (typeof value === "bigint") return new Int64();
  if (typeof value === "number") return new Float64();
  if (value instanceof Uint8Array) return new Binary();
  return new Utf8(); // fallback
}

/**
 * Recursively coerce JS values to match Arrow type expectations.
 * Converts numbers to BigInt for Int64 fields, and recurses into Map/List types.
 */
function coerceForArrow(type: DataType, value: any): any {
  if (value == null) return value;

  // Int64: convert number → BigInt
  if (DataType.isInt(type) && (type as any).bitWidth === 64) {
    if (typeof value === "number") return BigInt(value);
    return value;
  }

  // Map_: coerce map values recursively
  if (DataType.isMap(type)) {
    if (value instanceof Map) {
      const entriesField = (type as any).children[0];
      const valueType = entriesField.type.children[1].type;
      const coerced = new Map();
      for (const [k, v] of value) {
        coerced.set(k, coerceForArrow(valueType, v));
      }
      return coerced;
    }
    return value;
  }

  // List: coerce elements recursively
  if (DataType.isList(type)) {
    if (Array.isArray(value)) {
      const elemType = (type as any).children[0].type;
      return value.map((v: any) => coerceForArrow(elemType, v));
    }
    return value;
  }

  return value;
}

/**
 * Build a 1-row Arrow IPC request batch with method metadata.
 */
export function buildRequestIpc(
  schema: Schema,
  params: Record<string, any>,
  method: string,
): Uint8Array {
  const metadata = new Map<string, string>();
  metadata.set(RPC_METHOD_KEY, method);
  metadata.set(REQUEST_VERSION_KEY, REQUEST_VERSION);

  if (schema.fields.length === 0) {
    const structType = new Struct(schema.fields);
    const data = makeData({
      type: structType,
      length: 1,
      children: [],
      nullCount: 0,
    });
    const batch = new RecordBatch(schema, data, metadata);
    return serializeIpcStream(schema, [batch]);
  }

  const children = schema.fields.map((f) => {
    const val = coerceForArrow(f.type, params[f.name]);
    return vectorFromArray([val], f.type).data[0];
  });

  const structType = new Struct(schema.fields);
  const data = makeData({
    type: structType,
    length: 1,
    children,
    nullCount: 0,
  });

  const batch = new RecordBatch(schema, data, metadata);
  return serializeIpcStream(schema, [batch]);
}

/**
 * Read schema + all batches from an IPC stream body.
 */
export async function readResponseBatches(
  body: Uint8Array,
): Promise<{ schema: Schema; batches: RecordBatch[] }> {
  const reader = await RecordBatchReader.from(body);
  await reader.open();
  const schema = reader.schema;
  if (!schema) {
    throw new RpcError("ProtocolError", "Empty IPC stream: no schema", "");
  }
  const batches = reader.readAll();
  return { schema, batches };
}

/**
 * Check if a zero-row batch carries log/error metadata.
 * If EXCEPTION → throw RpcError.
 * If other level → call onLog.
 * Returns true if the batch was consumed as a log/error.
 */
export function dispatchLogOrError(
  batch: RecordBatch,
  onLog?: (msg: LogMessage) => void,
): boolean {
  const meta = batch.metadata;
  if (!meta) return false;

  const level = meta.get(LOG_LEVEL_KEY);
  if (!level) return false;

  const message = meta.get(LOG_MESSAGE_KEY) ?? "";

  if (level === "EXCEPTION") {
    const extraStr = meta.get(LOG_EXTRA_KEY);
    let errorType = "RpcError";
    let errorMessage = message;
    let traceback = "";
    if (extraStr) {
      try {
        const extra = JSON.parse(extraStr);
        errorType = extra.exception_type ?? "RpcError";
        errorMessage = extra.exception_message ?? message;
        traceback = extra.traceback ?? "";
      } catch {}
    }
    throw new RpcError(errorType, errorMessage, traceback);
  }

  if (onLog) {
    const extraStr = meta.get(LOG_EXTRA_KEY);
    let extra: Record<string, any> | undefined;
    if (extraStr) {
      try {
        extra = JSON.parse(extraStr);
      } catch {}
    }
    onLog({ level, message, extra });
  }

  return true;
}

/**
 * Extract all rows from a batch as Record<string, any>[].
 * Converts BigInt to Number when safe.
 */
export function extractBatchRows(batch: RecordBatch): Record<string, any>[] {
  const rows: Record<string, any>[] = [];
  for (let r = 0; r < batch.numRows; r++) {
    const row: Record<string, any> = {};
    for (let i = 0; i < batch.schema.fields.length; i++) {
      const field = batch.schema.fields[i];
      let value = batch.getChildAt(i)?.get(r);
      if (typeof value === "bigint") {
        if (
          value >= BigInt(Number.MIN_SAFE_INTEGER) &&
          value <= BigInt(Number.MAX_SAFE_INTEGER)
        ) {
          value = Number(value);
        }
      }
      row[field.name] = value;
    }
    rows.push(row);
  }
  return rows;
}

/**
 * Read sequential IPC streams from a response body.
 * Returns an IpcStreamReader for reading header + data streams.
 */
export async function readSequentialStreams(
  body: Uint8Array,
): Promise<IpcStreamReader> {
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(body);
      controller.close();
    },
  });
  return IpcStreamReader.create(stream);
}
