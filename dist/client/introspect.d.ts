import { type RecordBatch, type Schema } from "@query-farm/apache-arrow";
import type { LogMessage } from "./types.js";
/** Describes a single RPC method as reported by the server's `__describe__` response. */
export interface MethodInfo {
    /** The method name as invoked by {@link RpcClient.call} / {@link RpcClient.stream}. */
    name: string;
    /** Whether the method is a single request/response (`unary`) or a streaming method (`stream`). */
    type: "unary" | "stream";
    /** Arrow schema of the call parameters. */
    paramsSchema: Schema;
    /** Arrow schema of a unary result; for stream methods this holds the per-batch output schema. */
    resultSchema: Schema;
    /** Arrow schema of the per-batch input rows for exchange streams, when available. */
    inputSchema?: Schema;
    /** Arrow schema of the per-batch output rows for stream methods, when available. */
    outputSchema?: Schema;
    /** Arrow schema of the stream's one-time header row, when the method declares one. */
    headerSchema?: Schema;
    /** Human-readable documentation for the method, if the server provides it. */
    doc?: string;
    /** Per-parameter human-readable type names, if the server provides them. */
    paramTypes?: Record<string, string>;
    /** Default values applied to omitted parameters before a call is sent. */
    defaults?: Record<string, any>;
}
/** The full set of methods and protocol metadata reported by a server's `__describe__`. */
export interface ServiceDescription {
    /** The server's declared protocol/service name. */
    protocolName: string;
    /** Application protocol surface version surfaced by the server's
     *  __describe__ response. Empty string when the server did not declare
     *  a `protocolVersion`. */
    protocolVersion: string;
    /** Every method the server exposes (excluding the built-in `__describe__`). */
    methods: MethodInfo[];
}
/**
 * Parse a __describe__ response from batches into a ServiceDescription.
 * Reusable across transports (HTTP, pipe, subprocess).
 */
export declare function parseDescribeResponse(batches: RecordBatch[], onLog?: (msg: LogMessage) => void): Promise<ServiceDescription>;
/**
 * Send a __describe__ request and return a ServiceDescription.
 */
export declare function httpIntrospect(rawBaseUrl: string, options?: {
    prefix?: string;
    authorization?: string;
    compressionLevel?: number;
    compressFn?: (data: Uint8Array, level: number) => Promise<Uint8Array>;
    decompressFn?: (data: Uint8Array) => Promise<Uint8Array>;
}): Promise<ServiceDescription>;
//# sourceMappingURL=introspect.d.ts.map