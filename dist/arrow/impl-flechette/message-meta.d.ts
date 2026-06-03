/**
 * Walk an Arrow IPC stream and return the first RecordBatch message's
 * (numRows, metadata) — the bits flechette drops on the floor.
 *
 * Returns null if the stream contains no RecordBatch.
 */
export declare function readFirstRecordBatchMeta(stream: Uint8Array): {
    numRows: number;
    metadata: Map<string, string>;
} | null;
//# sourceMappingURL=message-meta.d.ts.map