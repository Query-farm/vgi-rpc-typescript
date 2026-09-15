// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// `vgi_rpc.Reflection.v1` -- discovery as an ordinary co-hosted protocol.
//
// Introspection used to be a hardcoded method name, `__describe__`, answered
// from a pre-built batch before dispatch. That made it a thing every port had
// to hand-implement, in a bespoke format, outside the machinery that serves
// every other method -- which is how the ports drifted. Here it is a protocol
// like any other.
//
// Following gRPC's reflection service and D-Bus's `org.freedesktop.DBus`, it is
// co-hosted rather than special-cased. Its own major version sits in its name,
// so an incompatible reflection is a routing failure a client can act on rather
// than a mis-parse.
//
// Exempt from the `protocol_version` gate: this is the protocol a
// version-mismatched client calls to learn *what* mismatched, and gating it
// would deny the client the diagnosis it came for.
//
// Minor skew must be survivable, which means a decoder reads by field name,
// ignores columns it does not know, and defaults columns that are absent -- and
// errors on an absent field that has no default, since zero-filling a required
// field hands a client a description that is wrong rather than absent. One rule
// follows and binds every port: a field added in a minor version must carry a
// default.

import {
  batchFromColumns,
  binary,
  bool,
  field,
  list,
  schema as makeSchema,
  serializeBatch,
  serializeSchema,
  struct,
  utf8,
} from "./arrow/index.js";
import type { VgiSchema } from "./arrow/types.js";
import { ProtocolNotSupportedError } from "./binding.js";
import { REQUEST_VERSION } from "./constants.js";
import { Protocol } from "./protocol.js";
import { computeProtocolHash, type HashMethod } from "./protocol-hash.js";
import type { MethodDefinition } from "./types.js";
import { MethodType } from "./types.js";

/** The wire name of the reflection protocol.
 *
 *  Fixed, and the one protocol name a client may know a priori: it is the
 *  bootstrap, so there is nothing to discover it with. */
export const REFLECTION_PROTOCOL_NAME = "vgi_rpc.Reflection.v1";

/** Values {@link MethodInfoDesc.idempotency} may take, borrowed from gRPC's
 *  `idempotency_level`.
 *
 *  With an HTTP transport and a policy proxy in the path, retries *will*
 *  happen; without this nothing on the wire said what was safe to retry.
 *  `unknown` is the default and means a caller must assume the worst. */
export const IDEMPOTENCY_LEVELS = ["unknown", "no_side_effects", "idempotent"] as const;

/** What a stream method does, when that is knowable.
 *
 *  Whether a stream is an exchange is decided by the implementation, not by the
 *  protocol definition, so a server describing its own surface often cannot say
 *  -- `unknown` is the honest answer and is spelled rather than left null. */
export const STREAM_KINDS = ["unknown", "producer", "exchange"] as const;

/** One method's wire surface.
 *
 *  Schemas travel as serialized Arrow IPC rather than as a structural
 *  description: a client's whole purpose in asking is to get a schema it can
 *  hand to its own Arrow implementation, and IPC is the one representation
 *  every port already reads. The *hash* is what compares across ports, and it
 *  is taken over the decoded structure precisely so these bytes need not
 *  match. */
export interface MethodInfoDesc {
  name: string;
  method_type: string;
  has_return: boolean;
  has_header: boolean;
  /** `""` for unary; otherwise one of {@link STREAM_KINDS}. */
  stream_kind: string;
  /** Empty rather than null when absent: a nullable column costs every port a
   *  null check on a value it will only ever treat as absent. */
  params_schema_ipc: Uint8Array;
  result_schema_ipc: Uint8Array;
  header_schema_ipc: Uint8Array;
  idempotency: string;
  deprecated: boolean;
  deprecation_message: string;
}

/** One hosted protocol without its methods.
 *
 *  Enough to decide whether to fetch the full description: a client that
 *  already knows a hash can skip the round trip entirely. */
export interface ProtocolSummaryDesc {
  protocol: string;
  protocol_version: string;
  protocol_hash: string;
  deprecated: boolean;
  deprecation_message: string;
  features: string[];
}

/** One protocol's full description.
 *
 *  Carries no server identity: two processes serving the same protocol must
 *  describe it identically, or the description is not a property of the
 *  protocol. Server identity lives on {@link ProtocolListDesc}, which is a
 *  statement about a server. */
export interface ServiceDescriptionDesc extends ProtocolSummaryDesc {
  methods: MethodInfoDesc[];
}

/** What this server hosts. */
export interface ProtocolListDesc {
  server_id: string;
  server_version: string;
  request_version: string;
  protocols: ProtocolSummaryDesc[];
}

// The payload schemas, mirroring the Python reference field for field.
//
// A generated schema rather than a hand-built batch plus metadata keys: that is
// the whole point of reflection being a protocol, and it is what keeps six
// ports from each maintaining their own describe format.
//
// Decoding is tolerant by contract -- by field name, ignoring unknown columns,
// defaulting absent ones that have defaults -- so a field added in a minor
// version must carry a default, or the addition is a breaking change wearing a
// minor version number.

const PROTOCOL_SUMMARY_FIELDS = [
  field("protocol", utf8(), false),
  field("protocol_version", utf8(), false),
  field("protocol_hash", utf8(), false),
  field("deprecated", bool(), false),
  field("deprecation_message", utf8(), false),
  field("features", list(field("item", utf8(), true)), false),
];

const METHOD_INFO_FIELDS = [
  field("name", utf8(), false),
  field("method_type", utf8(), false),
  field("has_return", bool(), false),
  field("has_header", bool(), false),
  field("stream_kind", utf8(), false),
  field("params_schema_ipc", binary(), false),
  field("result_schema_ipc", binary(), false),
  field("header_schema_ipc", binary(), false),
  field("idempotency", utf8(), false),
  field("deprecated", bool(), false),
  field("deprecation_message", utf8(), false),
];

/** @internal */
export const PROTOCOL_LIST_SCHEMA = makeSchema([
  field("server_id", utf8(), false),
  field("server_version", utf8(), false),
  field("request_version", utf8(), false),
  field("protocols", list(field("item", struct(PROTOCOL_SUMMARY_FIELDS), true)), false),
]);

/** @internal */
export const SERVICE_DESCRIPTION_SCHEMA = makeSchema([
  ...PROTOCOL_SUMMARY_FIELDS,
  field("methods", list(field("item", struct(METHOD_INFO_FIELDS), true)), false),
]);

/** Encode one reflection payload as a single-row Arrow IPC stream.
 *
 *  The framework's ordinary convention for a structured return: the value rides
 *  as serialized bytes in a `result` binary column, and this is the nested
 *  stream inside it. */
export function encodeReflectionPayload(value: object, schema: ReturnType<typeof makeSchema>): Uint8Array {
  const columns: Record<string, unknown[]> = {};
  for (const f of schema.fields) {
    columns[f.name] = [(value as Record<string, unknown>)[f.name]];
  }
  return serializeBatch(batchFromColumns(schema, columns as Record<string, any[]>));
}

/** Serialize a schema, or return empty bytes when there is none. */
function schemaIpc(schema: VgiSchema | undefined | null): Uint8Array {
  if (!schema) return new Uint8Array(0);
  return serializeSchema(schema);
}

/** Return the stream kind, or `""` for a unary method. */
export function streamKindFor(method: MethodDefinition): string {
  if (method.type === MethodType.UNARY) return "";
  if (method.exchangeFn) return "exchange";
  if (method.producerFn) return "producer";
  return "unknown";
}

/** Whether a method returns a value to its caller.
 *
 *  A stream's `resultSchema` is the (empty) protocol-level return, not
 *  something the caller receives, and a void unary method carries a zero-field
 *  schema rather than an absent one -- so neither the presence check nor the
 *  method type alone is the question being asked. */
export function unaryHasReturn(method: MethodDefinition): boolean {
  return method.type === MethodType.UNARY && (method.resultSchema?.fields.length ?? 0) > 0;
}

/** Describe one method for the wire. */
export function describeMethod(method: MethodDefinition): MethodInfoDesc {
  return {
    name: method.name,
    method_type: method.type === MethodType.UNARY ? "unary" : "stream",
    has_return: unaryHasReturn(method),
    has_header: method.headerSchema !== undefined,
    stream_kind: streamKindFor(method),
    params_schema_ipc: schemaIpc(method.paramsSchema),
    result_schema_ipc: unaryHasReturn(method) ? schemaIpc(method.resultSchema) : new Uint8Array(0),
    header_schema_ipc: schemaIpc(method.headerSchema),
    idempotency: "unknown",
    deprecated: false,
    deprecation_message: "",
  };
}

/** Compute one binding's canonical fingerprint. */
export async function bindingHash(name: string, methods: ReadonlyMap<string, MethodDefinition>): Promise<string> {
  const hashMethods: HashMethod[] = [];
  for (const method of methods.values()) {
    const entry: HashMethod = {
      name: method.name,
      methodType: method.type === MethodType.UNARY ? "unary" : "stream",
      hasReturn: unaryHasReturn(method),
      hasHeader: method.headerSchema !== undefined,
      paramsFields: method.paramsSchema?.fields ?? [],
    };
    if (entry.hasReturn) entry.resultFields = method.resultSchema?.fields ?? [];
    if (entry.hasHeader) entry.headerFields = method.headerSchema?.fields ?? [];
    hashMethods.push(entry);
  }
  return computeProtocolHash(name, hashMethods);
}

/** Build the reflection protocol for `server`.
 *
 *  Its two methods return their payloads as serialized Arrow IPC in a single
 *  `result` binary column -- the framework's ordinary convention for a
 *  structured return. Reflection is an ordinary protocol now, so it is subject
 *  to that convention like everything else. */
export function buildReflectionProtocol(deps: {
  /** Every hosted protocol, keyed by wire name, primary first. */
  listBindings: () => Map<
    string,
    { name: string; protocol: { protocolVersion: string; getMethods(): ReadonlyMap<string, MethodDefinition> } }
  >;
  /** This protocol's canonical fingerprint, by wire name. */
  hashFor: (name: string) => Promise<string>;
  serverId: () => string;
  serverVersion: () => string;
}): Protocol {
  const p = new Protocol(REFLECTION_PROTOCOL_NAME);

  p.unary("list_protocols", {
    params: {},
    result: { result: binary() },
    doc: "Return every protocol this server hosts, with versions and hashes.",
    handler: async () => {
      const all = deps.listBindings();
      const names = [...all.keys()].sort();
      const protocols: ProtocolSummaryDesc[] = [];
      for (const name of names) {
        const b = all.get(name)!;
        protocols.push({
          protocol: b.name,
          protocol_version: b.protocol.protocolVersion ?? "",
          protocol_hash: await deps.hashFor(name),
          deprecated: false,
          deprecation_message: "",
          features: [],
        });
      }
      const listing: ProtocolListDesc = {
        server_id: deps.serverId(),
        server_version: deps.serverVersion(),
        request_version: REQUEST_VERSION,
        protocols,
      };
      return { result: encodeReflectionPayload(listing, PROTOCOL_LIST_SCHEMA) };
    },
  });

  p.unary("describe", {
    params: { protocol: utf8() },
    result: { result: binary() },
    doc: "Return one protocol's full description.",
    handler: async (params) => {
      const protocol = String(params.protocol ?? "");
      const all = deps.listBindings();
      const b = all.get(protocol);
      if (!b) {
        throw ProtocolNotSupportedError.notHosted(protocol, [...all.keys()].sort());
      }
      const methods = [...b.protocol.getMethods().values()]
        // Sorted so two ports iterating differently-ordered maps still agree.
        .sort((x, y) => (x.name < y.name ? -1 : x.name > y.name ? 1 : 0))
        .map(describeMethod);
      const desc: ServiceDescriptionDesc = {
        protocol: b.name,
        protocol_version: b.protocol.protocolVersion ?? "",
        protocol_hash: await deps.hashFor(protocol),
        deprecated: false,
        deprecation_message: "",
        features: [],
        methods,
      };
      return { result: encodeReflectionPayload(desc, SERVICE_DESCRIPTION_SCHEMA) };
    },
  });

  return p;
}
