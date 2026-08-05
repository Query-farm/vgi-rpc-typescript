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
/** Byte span of one on-wire IPC message (framing + metadata + body). */
export interface IpcMessageSpan {
    /** Header type — 1 Schema, 2 DictionaryBatch, 3 RecordBatch. */
    headerType: number;
    /** Offset of the message's continuation marker in the stream. */
    frameStart: number;
    /** Offset just past the message's (padded) body. */
    frameEnd: number;
}
/**
 * Split an Arrow IPC *stream* into its constituent message frames, stopping at
 * the end-of-stream marker. Each span covers the whole on-wire message
 * (continuation marker + metadata length + padded metadata + padded body).
 *
 * Used by the flechette incremental encoder to carve a one-shot
 * `tableToIPC(..., { format: "stream" })` output — a complete
 * `[schema][dict…][recordbatch][EOS]` stream — into the schema preamble and the
 * per-batch `[dict…][recordbatch]` body that the lockstep stdio protocol emits
 * separately.
 */
export declare function splitIpcMessages(stream: Uint8Array): IpcMessageSpan[];
//# sourceMappingURL=message-meta.d.ts.map