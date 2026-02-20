import { DataType, type Schema, type RecordBatch } from "apache-arrow";
import {
  RPC_METHOD_KEY,
  REQUEST_VERSION_KEY,
  REQUEST_VERSION,
  REQUEST_ID_KEY,
} from "../constants.js";
import { RpcError, VersionError } from "../errors.js";

export interface ParsedRequest {
  methodName: string;
  requestVersion: string;
  requestId: string | null;
  schema: Schema;
  params: Record<string, any>;
  rawMetadata: Map<string, string>;
}

/**
 * Parse a request from a RecordBatch with metadata.
 * Extracts method name, version, and params from the batch.
 */
export function parseRequest(
  schema: Schema,
  batch: RecordBatch,
): ParsedRequest {
  const metadata: Map<string, string> = batch.metadata ?? new Map();

  const methodName = metadata.get(RPC_METHOD_KEY);
  if (methodName === undefined) {
    throw new RpcError(
      "ProtocolError",
      "Missing 'vgi_rpc.method' in request batch custom_metadata. " +
        "Each request batch must carry a 'vgi_rpc.method' key in its Arrow IPC custom_metadata " +
        "with the method name as a UTF-8 string.",
      "",
    );
  }

  const version = metadata.get(REQUEST_VERSION_KEY);
  if (version === undefined) {
    throw new VersionError(
      "Missing 'vgi_rpc.request_version' in request batch custom_metadata. " +
        `Set the 'vgi_rpc.request_version' custom_metadata value to '${REQUEST_VERSION}'.`,
    );
  }
  if (version !== REQUEST_VERSION) {
    throw new VersionError(
      `Unsupported request version '${version}', expected '${REQUEST_VERSION}'. ` +
        `Set the 'vgi_rpc.request_version' custom_metadata value to '${REQUEST_VERSION}'.`,
    );
  }

  const requestId = metadata.get(REQUEST_ID_KEY) ?? null;

  // Extract params from single-row batch
  const params: Record<string, any> = {};
  if (schema.fields.length > 0 && batch.numRows !== 1) {
    throw new RpcError(
      "ProtocolError",
      `Expected 1 row in request batch, got ${batch.numRows}. ` +
        "Each parameter is a column (not a row). The batch should have exactly 1 row.",
      "",
    );
  }

  for (let i = 0; i < schema.fields.length; i++) {
    const field = schema.fields[i];
    // Map_ columns have a broken .get() in arrow-js — pass through raw Data
    if (DataType.isMap(field.type)) {
      params[field.name] = batch.getChildAt(i)!.data[0];
      continue;
    }
    let value = batch.getChildAt(i)?.get(0);
    // Convert BigInt to Number when safe
    if (typeof value === "bigint") {
      if (
        value >= BigInt(Number.MIN_SAFE_INTEGER) &&
        value <= BigInt(Number.MAX_SAFE_INTEGER)
      ) {
        value = Number(value);
      }
    }
    params[field.name] = value;
  }

  return {
    methodName,
    requestVersion: version,
    requestId,
    schema,
    params,
    rawMetadata: metadata,
  };
}
