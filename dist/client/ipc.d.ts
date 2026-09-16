import { DataType, type RecordBatch, type Schema } from "@query-farm/apache-arrow";
import { IpcStreamReader } from "../wire/reader.js";
export { dispatchLogOrError } from "../log-batch.js";
/** Infer an Arrow DataType from a JS value. */
export declare function inferArrowType(value: any): DataType;
/**
 * Build a 1-row Arrow IPC request batch with method metadata.
 *
 * `options.protocol` is the routing key -- which protocol the method belongs
 * to. The wire protocol requires it on every request, including against a
 * server hosting exactly one protocol, so omitting it produces a request the
 * server refuses with `protocol_not_specified`.
 *
 * When `options.protocolVersion` is non-empty, the value is emitted as
 * `vgi_rpc.protocol_version` so servers that declare a Protocol-level
 * version validate the request at the dispatch boundary.
 */
export declare function buildRequestIpc(schema: Schema, params: Record<string, any>, method: string, options?: {
    protocolVersion?: string;
    protocol?: string;
}): Uint8Array;
/**
 * Read schema + all batches from an IPC stream body.
 */
export declare function readResponseBatches(body: Uint8Array): Promise<{
    schema: Schema;
    batches: RecordBatch[];
}>;
/**
 * Extract all rows from a batch as Record<string, any>[].
 * Converts BigInt to Number when safe.
 */
export declare function extractBatchRows(batch: RecordBatch): Record<string, any>[];
/**
 * Read sequential IPC streams from a response body.
 * Returns an IpcStreamReader for reading header + data streams.
 */
export declare function readSequentialStreams(body: Uint8Array): Promise<IpcStreamReader>;
//# sourceMappingURL=ipc.d.ts.map