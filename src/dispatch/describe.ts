// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { sha256Hex } from "../util/web-crypto.js";
import {
  type VgiBatch,
  schema as makeSchema,
  field,
  utf8,
  bool,
  binary,
  batchFromColumns,
  withBatchMetadata,
} from "../arrow/index.js";
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
export const DESCRIBE_SCHEMA = makeSchema([
  field("name", utf8(), false),
  field("method_type", utf8(), false),
  field("has_return", bool(), false),
  field("params_schema_ipc", binary(), false),
  field("result_schema_ipc", binary(), false),
  field("has_header", bool(), false),
  field("header_schema_ipc", binary(), true),
  field("is_exchange", bool(), true),
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
): Promise<{ batch: VgiBatch; metadata: Map<string, string> }> {
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

  const baseBatch = batchFromColumns(DESCRIBE_SCHEMA, {
    name: names,
    method_type: methodTypes,
    has_return: hasReturns,
    params_schema_ipc: paramsSchemas,
    result_schema_ipc: resultSchemas,
    has_header: hasHeaders,
    header_schema_ipc: headerSchemas,
    is_exchange: isExchanges,
  });

  const protocolHash = await computeProtocolHash(protocolName, hashRows);

  const metadata = new Map<string, string>();
  metadata.set(PROTOCOL_NAME_KEY, protocolName);
  metadata.set(REQUEST_VERSION_KEY, REQUEST_VERSION);
  metadata.set(DESCRIBE_VERSION_KEY, DESCRIBE_VERSION);
  metadata.set(PROTOCOL_HASH_KEY, protocolHash);
  metadata.set(SERVER_ID_KEY, serverId);

  const batch = withBatchMetadata(baseBatch, metadata);
  return { batch, metadata };
}
