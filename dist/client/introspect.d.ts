import { type RecordBatch, type Schema } from "@query-farm/apache-arrow";
import { type ExternalLocationConfig } from "../external.js";
import { type ProtocolListDesc, type ServiceDescriptionDesc } from "../reflection.js";
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
 * Present a reflection reply in this module's client-side shape.
 *
 * {@link ServiceDescription} is a *client-side view*, not a wire format -- it
 * was only ever the latter by accident of there having been one encoding.
 * Keeping it means the client, its callers and its tests move to reflection
 * without any of them changing shape.
 */
export declare function adaptServiceDescription(wire: ServiceDescriptionDesc, listing?: ProtocolListDesc): ServiceDescription;
/** Pick the protocol to describe out of what a server says it hosts.
 *
 *  The first one that is not framework-reserved: reflection and identity are
 *  co-hosted on every server, so "the protocol" a caller means is the
 *  application's, and a client that took the literal first would describe
 *  reflection to itself on half the fleet. */
export declare function pickApplicationProtocol(listing: ProtocolListDesc): string;
/** Extract the `result` bytes from a unary reply's batches.
 *
 *  A structured return rides as serialized bytes in a single `result` binary
 *  column -- the framework's ordinary unary convention. Reflection is an
 *  ordinary protocol now, so its replies are subject to it like any other
 *  method's. */
export declare function reflectionResult(batches: RecordBatch[], onLog?: (msg: LogMessage) => void, externalConfig?: ExternalLocationConfig | null): Promise<Uint8Array>;
/** Build the request body for one reflection call. */
export declare function reflectionRequest(method: string, protocol?: string): Uint8Array;
/**
 * Describe a server's protocol over HTTP via `vgi_rpc.Reflection.v1`.
 *
 * Two round trips: `list_protocols` to learn what the server hosts, then
 * `describe` on one of them. The first is unavoidable once a server may host
 * several protocols -- there is no longer a single "the" protocol to ask about
 * without asking. Pass `protocol` to skip it.
 */
export declare function httpIntrospect(rawBaseUrl: string, options?: {
    prefix?: string;
    /** Which protocol to describe. Defaults to the first hosted one that is
     *  not framework-reserved, which costs the `list_protocols` hop. */
    protocol?: string;
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
}): Promise<ServiceDescription>;
//# sourceMappingURL=introspect.d.ts.map