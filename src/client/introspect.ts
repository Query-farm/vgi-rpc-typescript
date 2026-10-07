// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { Schema as ArrowSchema, type RecordBatch, type Schema } from "@query-farm/apache-arrow";
import { deserializeSchema as deserializeSchemaImpl, schema as makeSchema } from "#vgi-rpc-arrow";
import { DEFAULT_ACCEPTED_MAX_RESPONSE_BYTES } from "#vgi-rpc-client-response-budget";
import { RESERVED_PROTOCOL_PREFIX } from "../binding.js";
import { RpcError } from "../errors.js";
import { type ExternalLocationConfig, isExternalLocationBatch, resolveExternalLocation } from "../external.js";
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
import type { RpcClient } from "./connect.js";
import { decodeResponseBody, readResponseBodyBounded } from "./decode.js";
import { buildRequestIpc, dispatchLogOrError, readResponseBatches } from "./ipc.js";
import type { LogMessage } from "./types.js";

/** Describes a single RPC method as reported by `vgi_rpc.Reflection.v1`. */
export interface MethodInfo {
  /** The method name as invoked by {@link RpcClient.call} / {@link RpcClient.stream}. */
  name: string;
  /** Whether the method is a single request/response (`unary`) or a streaming method (`stream`). */
  type: "unary" | "stream";
  /** Whether the method returns a value at all. `false` for a void method,
   *  whose reply carries no data batch. Absent when the description came from
   *  a caller-supplied literal rather than from the server. */
  hasReturn?: boolean;
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
  /** The serving process's opaque identity, from the `list_protocols` hop.
   *
   *  A property of the *server*, not of the protocol — two processes serving
   *  one protocol describe it identically — so it is absent when the caller
   *  named a protocol and the bootstrap hop was skipped. */
  serverId?: string;
  /** The wire request-framing version the server reports, from the same hop
   *  and absent under the same condition. */
  requestVersion?: string;
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
    hasReturn: wire.has_return,
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
    serverId: listing?.server_id,
    requestVersion: listing?.request_version,
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
export async function reflectionResult(
  batches: RecordBatch[],
  onLog?: (msg: LogMessage) => void,
  externalConfig?: ExternalLocationConfig | null,
): Promise<Uint8Array> {
  let dataBatch: RecordBatch | null = null;
  for (const batch of batches) {
    if (batch.numRows === 0) {
      // Reflection is an ordinary protocol, so a server that externalizes its
      // responses returns a pointer batch for it like any other method — and
      // a client that skipped it introspected nothing and reported an empty
      // reflection reply, which reads as a server fault rather than a client
      // one.
      if (isExternalLocationBatch(batch as never)) {
        dataBatch = (await resolveExternalLocation(batch as never, externalConfig, onLog)) as never;
        continue;
      }
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
  return buildRequestIpc(makeSchema([]) as unknown as Schema, {}, method, { protocol: REFLECTION_PROTOCOL_NAME });
}

/** Options shared by {@link httpIntrospect} and an HTTP client's reflection hook. */
interface HttpReflectionOptions {
  prefix?: string;
  /** External storage config, for a server that externalizes its replies —
   *  reflection's included. */
  externalLocation?: ExternalLocationConfig | null;
  authorization?: string;
  compressionLevel?: number;
  compressFn?: (data: Uint8Array, level: number) => Promise<Uint8Array>;
  decompressFn?: (data: Uint8Array) => Promise<Uint8Array>;
  fetch?: typeof globalThis.fetch;
  acceptedMaxResponseBytes?: number;
  /** @internal The owning HttpRpcClient already completed discovery. */
  responseBudgetVerified?: boolean;
}

/**
 * Build the function that makes one unary reflection call over HTTP and
 * returns its `result` bytes. Shared by {@link httpIntrospect} and by
 * {@link httpConnect}'s reflection hook, so both send the same headers, honour
 * the same response budget and classify a bare 404 the same way.
 *
 * @internal
 */
export async function httpReflectionCaller(
  rawBaseUrl: string,
  options?: HttpReflectionOptions,
): Promise<ReflectionCall> {
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

  return async (method: string, protocol?: string): Promise<Uint8Array> => {
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
    // A server older than protocol-scoped routes has no route for reflection
    // at all and answers a bare 404 -- no Arrow body, no capability headers.
    // Report it as the HTTP error it is, so it can be classified as "not
    // hosted" rather than surfacing as a missing-capability protocol error.
    if (response.status === 404 && !(response.headers.get("Content-Type") ?? "").startsWith(ARROW_CONTENT_TYPE)) {
      const text = await response.text().catch(() => "");
      throw new RpcError("HttpError", `HTTP 404: ${text.slice(0, 200)}`, "");
    }
    const responseCapabilities = requireResponseBudgetSupport(response.headers);
    responseLimit = minPositive(responseLimit, responseCapabilities.maxResponseBytes ?? undefined) ?? responseLimit;

    const rawBody = await readResponseBodyBounded(response, responseLimit);
    const decoded = new Uint8Array(await decodeResponseBody(response.headers, rawBody, decompressFn, responseLimit));
    const { batches } = await readResponseBatches(decoded);
    return reflectionResult(batches, undefined, options?.externalLocation);
  };
}

/**
 * Describe a server's protocol over HTTP via `vgi_rpc.Reflection.v1`.
 *
 * Two round trips: `list_protocols` to learn what the server hosts, then
 * `describe` on one of them. The first is unavoidable once a server may host
 * several protocols -- there is no longer a single "the" protocol to ask about
 * without asking. Pass `protocol` to skip it.
 *
 * To ask through a client you already hold, use {@link listProtocols} and
 * {@link describeProtocol} instead.
 */
export async function httpIntrospect(
  rawBaseUrl: string,
  options?: HttpReflectionOptions & {
    /** Which protocol to describe. Defaults to the first hosted one that is
     *  not framework-reserved, which costs the `list_protocols` hop. */
    protocol?: string;
  },
): Promise<ServiceDescription> {
  const call = await httpReflectionCaller(rawBaseUrl, options);
  let listing: ProtocolListDesc | undefined;
  let protocol = options?.protocol;
  if (!protocol) {
    listing = decodeProtocolList(await call(REFLECTION_LIST_PROTOCOLS));
    protocol = pickApplicationProtocol(listing);
  }
  return adaptServiceDescription(decodeServiceDescription(await call(REFLECTION_DESCRIBE, protocol)), listing);
}

// ---------------------------------------------------------------------------
// listProtocols / describeProtocol -- reflection over a held connection
// ---------------------------------------------------------------------------

/** One unary reflection call on a client's own connection, returning the
 *  reply's `result` bytes. @internal */
export type ReflectionCall = (method: string, protocol?: string) => Promise<Uint8Array>;

/**
 * Where a client keeps its reflection hook.
 *
 * A registry symbol rather than a module-local one: the package ships several
 * independently bundled entry points (`.`, `./connect`), and a client built by
 * one must still be reachable from {@link listProtocols} imported from the
 * other. Not part of the public API -- the public route is
 * {@link listProtocols} / {@link describeProtocol}.
 *
 * @internal
 */
export const REFLECTION_CALL: unique symbol = Symbol.for("@query-farm/vgi-rpc/reflection-call") as never;

/** Install a client's reflection hook, non-enumerable so it stays out of the
 *  client's visible surface. @internal */
export function attachReflectionCall<T extends object>(client: T, call: ReflectionCall): T {
  Object.defineProperty(client, REFLECTION_CALL, { value: call, enumerable: false, configurable: false });
  return client;
}

/** One protocol a server hosts, as `vgi_rpc.Reflection.v1` lists it.
 *
 *  Returned by {@link listProtocols} in the server's order: application
 *  protocols in registration order (the primary first), then the framework's
 *  own (`vgi_rpc.Reflection.v1`, and `vgi_rpc.Identity.v1` on an HTTP server
 *  that hosts it). Frozen. */
export interface HostedProtocol {
  /** The protocol's wire name -- its routing key, carrying its major version,
   *  e.g. `"vgi_rpc.Reflection.v1"`. */
  readonly name: string;
  /** Its declared semver, or `""` when it declares none. */
  readonly version: string;
  /** SHA-256 of its canonical description, as 64 lowercase hex characters.
   *  Equal hashes mean an identical wire surface in any port, so a caller
   *  holding a cached description for this hash can skip
   *  {@link describeProtocol}. */
  readonly hash: string;
  /** Whether callers should migrate off this protocol. Default `false`. */
  readonly deprecated: boolean;
  /** What to migrate to; `""` unless {@link deprecated}. */
  readonly deprecationMessage: string;
  /** Capability tokens the protocol announces. Default empty. */
  readonly features: readonly string[];
}

/**
 * The server does not host `vgi_rpc.Reflection.v1`.
 *
 * Thrown by {@link listProtocols} and {@link describeProtocol} when the server
 * answers the reflection call with "not hosted" rather than with a listing: a
 * TypeScript server built with `enableDescribe: false` (its default is
 * `true`), a Python server built without `enable_describe=True` (its default),
 * or one that predates reflection. Such a server still serves its own
 * protocol, so this is a statement about discovery, not about the connection
 * -- the connection remains usable. No listing is ever inferred.
 *
 * A subclass of {@link RpcError} carrying the server's original error fields,
 * so code that already catches `RpcError` keeps working.
 */
export class ReflectionNotSupportedError extends RpcError {
  constructor(
    errorType: string,
    errorMessage: string,
    remoteTraceback: string,
    model: ConstructorParameters<typeof RpcError>[3] = {},
  ) {
    super(errorType, errorMessage, remoteTraceback, model);
    this.name = "ReflectionNotSupportedError";
  }

  /** Wrap the server's "not hosted" answer, keeping every field. */
  static fromRpcError(error: RpcError): ReflectionNotSupportedError {
    const wrapped = new ReflectionNotSupportedError(error.errorType, error.errorMessage, error.remoteTraceback, {
      errorCode: error.errorCode,
      errorKind: error.errorKind,
      errorDetails: error.errorDetails,
      requestId: error.requestId,
    });
    (wrapped as { cause?: unknown }).cause = error;
    return wrapped;
  }
}

/** `errorKind` values meaning "this server does not answer reflection". */
const NOT_HOSTED_KINDS = new Set(["protocol_not_supported", "method_not_implemented"]);
/** Remote exception names for the same, from servers that send no error kind. */
const NOT_HOSTED_TYPES = new Set(["ProtocolNotSupportedError", "MethodNotImplementedError"]);

/**
 * Whether `error` says the server does not host reflection at all.
 *
 * Only meaningful for `list_protocols`, which is always hosted when reflection
 * is: a "not supported" answer to it can only be about the protocol.
 * (`describe` answers `protocol_not_supported` for an unknown *argument*,
 * which is why {@link describeProtocol} lists first.)
 */
function reflectionNotHosted(error: RpcError): boolean {
  if (NOT_HOSTED_KINDS.has(error.errorKind) || error.errorCode === "UNIMPLEMENTED") return true;
  if (NOT_HOSTED_TYPES.has(error.errorType)) return true;
  return error.errorType === "HttpError" && error.errorMessage.startsWith("HTTP 404");
}

/** An {@link RpcError} by shape rather than by class: the package's entry
 *  points (`.`, `./connect`) are bundled separately, so a client from one
 *  throws an `RpcError` that is not `instanceof` the other's. */
function isRpcErrorLike(error: unknown): error is RpcError {
  if (typeof error !== "object" || error === null) return false;
  const e = error as Partial<RpcError>;
  return typeof e.errorType === "string" && typeof e.errorMessage === "string" && typeof e.errorKind === "string";
}

/** The reflection hook of `target`, or a TypeError naming what is accepted. */
function reflectionCallOf(target: object): ReflectionCall {
  const call = (target as { [REFLECTION_CALL]?: ReflectionCall })[REFLECTION_CALL];
  if (typeof call !== "function") {
    throw new TypeError(
      "cannot reach reflection through this object: pass a client returned by httpConnect, " +
        "httpConnectSocks5h, httpiConnect, pipeConnect, subprocessConnect, tcpConnect, " +
        "tcpConnectSocks5h or irohConnect",
    );
  }
  return call;
}

/** Call `list_protocols`, classifying "not hosted". */
async function listOn(call: ReflectionCall): Promise<ProtocolListDesc> {
  let bytes: Uint8Array;
  try {
    bytes = await call(REFLECTION_LIST_PROTOCOLS);
  } catch (error) {
    if (isRpcErrorLike(error) && error.name !== "ReflectionNotSupportedError" && reflectionNotHosted(error)) {
      throw ReflectionNotSupportedError.fromRpcError(error);
    }
    throw error;
  }
  return decodeProtocolList(bytes);
}

/**
 * List the protocols a server hosts, over a client the caller already holds.
 *
 * One round trip -- `vgi_rpc.Reflection.v1.list_protocols` -- on `target`'s
 * own connection; nothing new is opened and nothing is closed. Over HTTP the
 * call shares the client's fetch, prefix, authorization, session scope,
 * compression and response budget; over a byte-stream transport (pipe,
 * subprocess, TCP, Iroh) it shares the client's stream, which the server
 * demultiplexes by each request's protocol key -- so, like any other call on
 * such a client, it cannot run while a stream is open on it.
 *
 * `target` may be bound to any protocol the server hosts; only its connection
 * matters.
 *
 * @returns One {@link HostedProtocol} per hosted protocol, in the server's order.
 * @throws {ReflectionNotSupportedError} The server does not host reflection.
 *   The connection is still usable.
 * @throws {RpcError} Any other server error, or a transport failure.
 */
export async function listProtocols(target: RpcClient): Promise<HostedProtocol[]> {
  const listing = await listOn(reflectionCallOf(target));
  return listing.protocols.map((p) =>
    Object.freeze({
      name: p.protocol,
      version: p.protocol_version ?? "",
      hash: p.protocol_hash,
      deprecated: p.deprecated ?? false,
      deprecationMessage: p.deprecation_message ?? "",
      features: Object.freeze([...(p.features ?? [])]),
    }),
  );
}

/**
 * Describe one hosted protocol, over a client the caller already holds.
 *
 * Two round trips on `target`'s connection: `list_protocols` (for the server
 * identity the description carries, and to tell "no reflection" apart from
 * "no such protocol"), then `describe(name)`. The connection rules are those
 * of {@link listProtocols}.
 *
 * @param name The protocol's wire name, as {@link listProtocols} reports it.
 * @throws {ReflectionNotSupportedError} The server does not host reflection.
 * @throws {RpcError} The server does not host `name` (`errorKind`
 *   `"protocol_not_supported"`), answered with another error, or the
 *   transport failed.
 */
export async function describeProtocol(target: RpcClient, name: string): Promise<ServiceDescription> {
  const call = reflectionCallOf(target);
  const listing = await listOn(call);
  return adaptServiceDescription(decodeServiceDescription(await call(REFLECTION_DESCRIBE, name)), listing);
}
