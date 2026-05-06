// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { sha256Hex } from "../util/web-crypto.js";
import {
  Binary,
  Bool,
  Field,
  makeData,
  RecordBatch,
  Schema,
  Struct,
  Utf8,
  vectorFromArray,
} from "@query-farm/apache-arrow";
import {
  DESCRIBE_VERSION,
  DESCRIBE_VERSION_KEY,
  PROTOCOL_HASH_KEY,
  PROTOCOL_NAME_KEY,
  REQUEST_VERSION,
  REQUEST_VERSION_KEY,
  SERVER_ID_KEY,
} from "../constants.js";
import type { MethodDefinition } from "../types.js";
import { serializeSchema } from "../util/schema.js";

/**
 * Slim DESCRIBE_VERSION 4 schema. Python-flavoured fields (doc,
 * param_types_json, param_defaults_json, param_docs_json) are not on the
 * wire — Arrow IPC schema bytes are the authoritative type information;
 * everything else is source-level metadata that callers should consult the
 * Protocol class for.
 */
export const DESCRIBE_SCHEMA = new Schema([
  new Field("name", new Utf8(), false),
  new Field("method_type", new Utf8(), false),
  new Field("has_return", new Bool(), false),
  new Field("params_schema_ipc", new Binary(), false),
  new Field("result_schema_ipc", new Binary(), false),
  new Field("has_header", new Bool(), false),
  new Field("header_schema_ipc", new Binary(), true),
  new Field("is_exchange", new Bool(), true),
]);

/**
 * Compute the SHA-256 hex digest of the canonical describe payload. Mirrors
 * Python's vgi_rpc.introspect.compute_protocol_hash byte-for-byte, so two
 * implementations of the same Protocol that produce identical Arrow IPC
 * schema bytes for params/result/header will hash to the same value.
 */
async function computeProtocolHash(
  protocolName: string,
  rows: ReadonlyArray<{
    name: string;
    methodType: string;
    hasReturn: boolean;
    hasHeader: boolean;
    isExchange: boolean | null;
    paramsIpc: Uint8Array;
    resultIpc: Uint8Array;
    headerIpc: Uint8Array | null;
  }>,
): Promise<string> {
  // Web Crypto's `subtle.digest` takes a single buffer, so we accumulate the
  // canonical byte stream into a single Uint8Array and hash once. The byte
  // sequence below must remain byte-for-byte identical to the Python
  // implementation in vgi_rpc.introspect.compute_protocol_hash.
  const enc = new TextEncoder();
  const parts: Uint8Array[] = [];
  const push = (v: string | Uint8Array) =>
    parts.push(typeof v === "string" ? enc.encode(v) : v);

  push("vgi_rpc.describe.v");
  push(DESCRIBE_VERSION);
  push("|");
  push(REQUEST_VERSION);
  push("|");
  push(protocolName);
  push("|");
  for (const r of rows) {
    push(Uint8Array.of(0x1f));
    push(r.name);
    push(Uint8Array.of(0x1e));
    push(r.methodType);
    push(Uint8Array.of(0x1e));
    push(r.hasReturn ? "1" : "0");
    push(Uint8Array.of(0x1e));
    push(r.hasHeader ? "1" : "0");
    push(Uint8Array.of(0x1e));
    push(r.isExchange === null ? "-" : r.isExchange ? "1" : "0");
    push(Uint8Array.of(0x1e));
    push(r.paramsIpc);
    push(Uint8Array.of(0x1e));
    push(r.resultIpc);
    push(Uint8Array.of(0x1e));
    if (r.headerIpc) push(r.headerIpc);
  }

  let total = 0;
  for (const p of parts) total += p.length;
  const buf = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    buf.set(p, off);
    off += p.length;
  }
  return sha256Hex(buf);
}

/**
 * Build the __describe__ response batch and metadata.
 */
export async function buildDescribeBatch(
  protocolName: string,
  methods: Map<string, MethodDefinition>,
  serverId: string,
): Promise<{ batch: RecordBatch; metadata: Map<string, string> }> {
  // Sort methods by name for consistent ordering
  const sortedEntries = [...methods.entries()].sort(([a], [b]) => a.localeCompare(b));

  const names: (string | null)[] = [];
  const methodTypes: (string | null)[] = [];
  const hasReturns: boolean[] = [];
  const paramsSchemas: (Uint8Array | null)[] = [];
  const resultSchemas: (Uint8Array | null)[] = [];
  const hasHeaders: boolean[] = [];
  const headerSchemas: (Uint8Array | null)[] = [];
  const isExchanges: (boolean | null)[] = [];

  const hashRows: Array<{
    name: string;
    methodType: string;
    hasReturn: boolean;
    hasHeader: boolean;
    isExchange: boolean | null;
    paramsIpc: Uint8Array;
    resultIpc: Uint8Array;
    headerIpc: Uint8Array | null;
  }> = [];

  for (const [name, method] of sortedEntries) {
    names.push(name);
    methodTypes.push(method.type);

    const hasReturn = method.type === "unary" && method.resultSchema.fields.length > 0;
    hasReturns.push(hasReturn);

    const paramsIpc = serializeSchema(method.paramsSchema);
    const resultIpc = serializeSchema(method.resultSchema);
    paramsSchemas.push(paramsIpc);
    resultSchemas.push(resultIpc);

    const hasHeader = !!method.headerSchema;
    hasHeaders.push(hasHeader);
    const headerIpc = method.headerSchema ? serializeSchema(method.headerSchema) : null;
    headerSchemas.push(headerIpc);

    let isExchange: boolean | null;
    if (method.exchangeFn) isExchange = true;
    else if (method.producerFn) isExchange = false;
    else isExchange = null;
    isExchanges.push(isExchange);

    hashRows.push({
      name,
      methodType: method.type,
      hasReturn,
      hasHeader,
      isExchange,
      paramsIpc,
      resultIpc,
      headerIpc,
    });
  }

  const nameArr = vectorFromArray(names, new Utf8());
  const methodTypeArr = vectorFromArray(methodTypes, new Utf8());
  const hasReturnArr = vectorFromArray(hasReturns, new Bool());
  const paramsSchemaArr = vectorFromArray(paramsSchemas, new Binary());
  const resultSchemaArr = vectorFromArray(resultSchemas, new Binary());
  const hasHeaderArr = vectorFromArray(hasHeaders, new Bool());
  const headerSchemaArr = vectorFromArray(headerSchemas, new Binary());
  const isExchangeArr = vectorFromArray(isExchanges, new Bool());

  const children = [
    nameArr.data[0],
    methodTypeArr.data[0],
    hasReturnArr.data[0],
    paramsSchemaArr.data[0],
    resultSchemaArr.data[0],
    hasHeaderArr.data[0],
    headerSchemaArr.data[0],
    isExchangeArr.data[0],
  ];

  const structType = new Struct(DESCRIBE_SCHEMA.fields);
  const data = makeData({
    type: structType,
    length: sortedEntries.length,
    children,
    nullCount: 0,
  });

  const protocolHash = await computeProtocolHash(protocolName, hashRows);

  const metadata = new Map<string, string>();
  metadata.set(PROTOCOL_NAME_KEY, protocolName);
  metadata.set(REQUEST_VERSION_KEY, REQUEST_VERSION);
  metadata.set(DESCRIBE_VERSION_KEY, DESCRIBE_VERSION);
  metadata.set(PROTOCOL_HASH_KEY, protocolHash);
  metadata.set(SERVER_ID_KEY, serverId);

  const batch = new RecordBatch(DESCRIBE_SCHEMA, data, metadata);

  return { batch, metadata };
}
