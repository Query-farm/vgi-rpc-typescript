import { type VgiBatch, type VgiSchema } from "../arrow/index.js";
/**
 * Coerce values for Int64 schema fields from Number to BigInt.
 * Handles both single values and arrays. Returns a new record with coerced values.
 */
export declare function coerceInt64(schema: VgiSchema, values: Record<string, any>): Record<string, any>;
/**
 * Build a 1-row result batch with optional metadata.
 * For unary methods, `values` maps field names to single values.
 */
export declare function buildResultBatch(schema: VgiSchema, values: Record<string, any>, serverId: string, requestId: string | null): VgiBatch;
/**
 * Build a 0-row error batch with EXCEPTION metadata matching Python's Message.from_exception().
 */
export declare function buildErrorBatch(schema: VgiSchema, error: Error, serverId: string, requestId: string | null): VgiBatch;
/**
 * Build a 0-row log batch.
 */
export declare function buildLogBatch(schema: VgiSchema, level: string, message: string, extra?: Record<string, any>, serverId?: string, requestId?: string | null): VgiBatch;
/**
 * Build a 0-row batch from a schema with metadata.
 * Used for error/log batches.
 */
export declare function buildEmptyBatch(schema: VgiSchema, metadata?: Map<string, string>): VgiBatch;
//# sourceMappingURL=response.d.ts.map