// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { Schema as ArrowSchema, type RecordBatch, type Schema } from "@query-farm/apache-arrow";
import { deserializeSchema as deserializeSchemaImpl } from "#vgi-rpc-arrow";
import { DEFAULT_ACCEPTED_MAX_RESPONSE_BYTES } from "#vgi-rpc-client-response-budget";
import { RESERVED_PROTOCOL_PREFIX } from "../binding.js";
import { RpcError } from "../errors.js";
import { clientAcceptEncoding, VGI_ACCEPT_ENCODING_HEADER } from "../http/codec.js";
import { ARROW_CONTENT_TYPE, rpcPath } from "../http/common.js";
import { ACCEPT_MAX_RESPONSE_BYTES_HEADER, minPositive, optionalResponseBudget } from "../http/response-budget.js";
import {
  decodeProtocolList,
  decodeServiceDescription,
  type MethodInfoDesc,
  type ProtocolListDesc,
  REFLECTION_DESCRIBE,
  REFLECTION_DESCRIBE_PARAMS,
  REFLECTION_LIST_PROTOCOLS,
  REFLECTION_PROTOCOL_NAME,
  type ServiceDescriptionDesc,
} from "../reflection.js";
import { discoverHttpCapabilities, requireResponseBudgetSupport } from "./capabilities.js";
import { decodeResponseBody, readResponseBodyBounded } from "./decode.js";
import { buildRequestIpc, dispatchLogOrError, readResponseBatches } from "./ipc.js";
import type { LogMessage } from "./types.js";

/** Describes a single RPC method as reported by `vgi_rpc.Reflection.v1`. */
export interface MethodInfo {
  /** The method name as invoked by {@link RpcClient.call} / {@link RpcClient.stream}. */
  name: string;
  /** Whether the method is a single request/response (`unary`) or a streaming method (`stream`). */
  type: "unary" | "stream";
  /** Arrow schema of the call parameters. */
  paramsSchema: Schema;
  /** Arrow schema of a unary result; empty for a stream, whose per-batch
   *  output schema arrives with the stream itself. */
  resultSchema: Schema;
  /** Arrow schema of the per-batch input rows for exchange streams, when available. */
  inputSchema?: Schema;
  /** Arrow schema of the per-batch output rows for stream methods, when available. */
  outputSchema?: Schema;
  /** Arrow schema of the stream's one-time header row, when the method declares one. */
  headerSchema?: Schema;
  /** What the stream does: `"producer"`, `"exchange"`, or `"unknown"` where
   *  the server cannot say. Empty for a unary method. */
  streamKind?: string;
  /** Human-readable documentation for the method, if the server provides it. */
  doc?: string;
  /** Per-parameter human-readable type names, if the server provides them. */
  paramTypes?: Record<string, string>;
  /** Default values applied to omitted parameters before a call is sent. */
  defaults?: Record<string, any>;
}

/** One protocol's surface, as reported by `vgi_rpc.Reflection.v1`. */
export interface ServiceDescription {
  /** The described protocol's wire name -- the routing key every request carries. */
  protocolName: string;
  /** The protocol's declared semver, or `""` when it declares none. */
  protocolVersion: string;
  /** The canonical digest of this protocol's description: identical in every
   *  port that hosts the same protocol. */
  protocolHash: string;
  /** Every protocol the server hosts, reflection included. Comes from the
   *  `list_protocols` hop, so it is empty when the caller named a protocol and
   *  the bootstrap hop was skipped. */
  hostedProtocols: string[];
  /** Every method of the described protocol. */
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
  if (bytes.length === 0) return new ArrowSchema([]);
  return deserializeSchemaImpl(bytes) as unknown as Schema;
}

/** Turn one wire method description into this module's client-side shape. */
function adaptMethod(wire: MethodInfoDesc): MethodInfo {
  const type = wire.method_type === "stream" ? "stream" : "unary";
  const info: MethodInfo = {
    name: wire.name,
    type,
    paramsSchema: deserializeSchema(wire.params_schema_ipc),
    resultSchema: deserializeSchema(wire.result_schema_ipc),
  };
  if (type === "stream") info.streamKind = wire.stream_kind;
  if (wire.has_header) info.headerSchema = deserializeSchema(wire.header_schema_ipc);
  return info;
}

/**
 * Present a reflection reply in this module's client-side shape.
 *
 * {@link ServiceDescription} is a *client-side view*, not a wire format -- it
 * was only ever the latter by accident of there having been one encoding.
 * Keeping it means the client, its callers and its tests move to reflection
 * without any of them changing shape.
 */
export function adaptServiceDescription(wire: ServiceDescriptionDesc, listing?: ProtocolListDesc): ServiceDescription {
  return {
    protocolName: wire.protocol,
    protocolVersion: wire.protocol_version,
    protocolHash: wire.protocol_hash,
    hostedProtocols: listing ? listing.protocols.map((p) => p.protocol) : [],
    methods: wire.methods.map(adaptMethod),
  };
}

/** Pick the protocol to describe out of what a server says it hosts.
 *
 *  The first one that is not framework-reserved: reflection and identity are
 *  co-hosted on every server, so "the protocol" a caller means is the
 *  application's, and a client that took the literal first would describe
 *  reflection to itself on half the fleet. */
export function pickApplicationProtocol(listing: ProtocolListDesc): string {
  const application = listing.protocols.find((p) => !p.protocol.startsWith(RESERVED_PROTOCOL_PREFIX));
  if (!application) {
    throw new RpcError(
      "ProtocolError",
      `Server ${listing.server_id} hosts no application protocol: it offers only ` +
        `[${listing.protocols.map((p) => p.protocol).join(", ")}]. Name a protocol explicitly ` +
        `to describe one of those.`,
      "",
    );
  }
  return application.protocol;
}

/** Extract the `result` bytes from a unary reply's batches.
 *
 *  A structured return rides as serialized bytes in a single `result` binary
 *  column -- the framework's ordinary unary convention. Reflection is an
 *  ordinary protocol now, so its replies are subject to it like any other
 *  method's. */
export function reflectionResult(batches: RecordBatch[], onLog?: (msg: LogMessage) => void): Uint8Array {
  let dataBatch: RecordBatch | null = null;
  for (const batch of batches) {
    if (batch.numRows === 0) {
      dispatchLogOrError(batch, onLog);
      continue;
    }
    dataBatch = batch;
  }
  if (!dataBatch) {
    throw new RpcError("ProtocolError", `Empty '${REFLECTION_PROTOCOL_NAME}' response`, "");
  }
  const column = dataBatch.getChild("result") ?? dataBatch.getChildAt(0);
  const value = column?.get(0);
  if (!(value instanceof Uint8Array)) {
    throw new RpcError(
      "ProtocolError",
      `A '${REFLECTION_PROTOCOL_NAME}' reply carried no 'result' column of bytes`,
      "",
    );
  }
  return value;
}

/** Build the request body for one reflection call. */
export function reflectionRequest(method: string, protocol?: string): Uint8Array {
  if (method === REFLECTION_DESCRIBE) {
    return buildRequestIpc(
      REFLECTION_DESCRIBE_PARAMS as unknown as Schema,
      { protocol },
      REFLECTION_DESCRIBE,
      // Reflection declares no `protocolVersion`, and its binding is exempt
      // from the gate anyway: this is what a version-mismatched client calls
      // to learn *what* mismatched.
      { protocol: REFLECTION_PROTOCOL_NAME },
    );
  }
  return buildRequestIpc(new ArrowSchema([]), {}, method, { protocol: REFLECTION_PROTOCOL_NAME });
}

/**
 * Describe a server's protocol over HTTP via `vgi_rpc.Reflection.v1`.
 *
 * Two round trips: `list_protocols` to learn what the server hosts, then
 * `describe` on one of them. The first is unavoidable once a server may host
 * several protocols -- there is no longer a single "the" protocol to ask about
 * without asking. Pass `protocol` to skip it.
 */
export async function httpIntrospect(
  rawBaseUrl: string,
  options?: {
    prefix?: string;
    /** Which protocol to describe. Defaults to the first hosted one that is
     *  not framework-reserved, which costs the `list_protocols` hop. */
    protocol?: string;
    authorization?: string;
    compressionLevel?: number;
    compressFn?: (data: Uint8Array, level: number) => Promise<Uint8Array>;
    decompressFn?: (data: Uint8Array) => Promise<Uint8Array>;
    fetch?: typeof globalThis.fetch;
    acceptedMaxResponseBytes?: number;
    /** @internal The owning HttpRpcClient already completed discovery. */
    responseBudgetVerified?: boolean;
  },
): Promise<ServiceDescription> {
  // See httpConnect: a base URL ending in "/" would produce a doubled slash.
  const baseUrl = rawBaseUrl.replace(/\/+$/, "");
  const prefix = options?.prefix ?? "";

  const headers: Record<string, string> = { "Content-Type": ARROW_CONTENT_TYPE };
  if (options?.authorization) {
    headers.Authorization = options.authorization;
  }

  const level = options?.compressionLevel;
  const compressFn = options?.compressFn;
  const decompressFn = options?.decompressFn;
  if (level != null && compressFn) {
    headers["Content-Encoding"] = "zstd";
  }
  if (level != null && decompressFn) {
    headers["Accept-Encoding"] = "zstd";
  }
  // See connect.ts: states what we can decode, regardless of request compression.
  headers[VGI_ACCEPT_ENCODING_HEADER] = clientAcceptEncoding(decompressFn != null);
  const maxResponse = options?.acceptedMaxResponseBytes ?? DEFAULT_ACCEPTED_MAX_RESPONSE_BYTES;
  optionalResponseBudget(maxResponse, "acceptedMaxResponseBytes");
  let responseLimit = maxResponse;
  if (!options?.responseBudgetVerified) {
    const capabilities = await discoverHttpCapabilities(
      baseUrl,
      prefix,
      options?.authorization,
      maxResponse,
      options?.fetch ?? globalThis.fetch,
    );
    if (!capabilities.acceptMaxResponseBytesSupport) {
      throw new RpcError(
        "ProtocolError",
        "Server must advertise VGI-Accept-Max-Response-Bytes-Support: true before RPC dispatch",
        "",
      );
    }
    responseLimit = minPositive(maxResponse, capabilities.maxResponseBytes ?? undefined) ?? maxResponse;
  }
  headers[ACCEPT_MAX_RESPONSE_BYTES_HEADER] = String(maxResponse);

  const fetchFn = options?.fetch ?? globalThis.fetch;

  /** One unary reflection call, returning its `result` bytes. */
  async function call(method: string, protocol?: string): Promise<Uint8Array> {
    const body = reflectionRequest(method, protocol);
    const sendBody = level != null && compressFn ? await compressFn(body, level) : body;
    const response = await fetchFn(baseUrl + rpcPath(REFLECTION_PROTOCOL_NAME, method, { prefix }), {
      method: "POST",
      headers,
      body: sendBody as unknown as BodyInit,
    });
    if (response.status === 401) {
      throw new RpcError("AuthenticationError", "Authentication required", "");
    }
    const responseCapabilities = requireResponseBudgetSupport(response.headers);
    responseLimit = minPositive(responseLimit, responseCapabilities.maxResponseBytes ?? undefined) ?? responseLimit;

    const rawBody = await readResponseBodyBounded(response, responseLimit);
    const decoded = new Uint8Array(await decodeResponseBody(response.headers, rawBody, decompressFn, responseLimit));
    const { batches } = await readResponseBatches(decoded);
    return reflectionResult(batches);
  }

  let listing: ProtocolListDesc | undefined;
  let protocol = options?.protocol;
  if (!protocol) {
    listing = decodeProtocolList(await call(REFLECTION_LIST_PROTOCOLS));
    protocol = pickApplicationProtocol(listing);
  }
  return adaptServiceDescription(decodeServiceDescription(await call(REFLECTION_DESCRIBE, protocol)), listing);
}
