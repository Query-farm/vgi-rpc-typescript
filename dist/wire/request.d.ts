import { type VgiBatch, type VgiSchema } from "../arrow/index.js";
export interface ParsedRequest {
    methodName: string;
    requestVersion: string;
    requestId: string | null;
    schema: VgiSchema;
    params: Record<string, any>;
    rawMetadata: Map<string, string>;
}
/**
 * Enforce the registered parameter contract before handing values to user code.
 *
 * Arrow Schema object identity is not meaningful across an IPC boundary, so
 * compare the pieces of the field contract explicitly. `DataType#toString()`
 * is Arrow's canonical, recursive rendering and includes type parameters such
 * as integer width/sign, timestamp unit/timezone, decimal precision/scale,
 * and nested child fields.
 */
export declare function validateRequestSchema(actual: VgiSchema, expected: VgiSchema, methodName: string): void;
/**
 * Parse a request from a RecordBatch with metadata.
 * Extracts method name, version, and params from the batch.
 */
export declare function parseRequest(schema: VgiSchema, batch: VgiBatch): ParsedRequest;
/**
 * Fill in `defaults` for any params that arrived as null/undefined.
 * The slim DESCRIBE_VERSION 4 wire format no longer carries defaults to the
 * client, so default substitution must happen server-side: the client sends
 * a null in any column it didn't supply, and dispatch swaps in the registered
 * default before invoking the handler.
 */
export declare function applyDefaults(params: Record<string, any>, defaults: Record<string, any> | undefined): Record<string, any>;
//# sourceMappingURL=request.d.ts.map