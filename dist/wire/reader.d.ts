import type { VgiBatch, VgiSchema } from "../arrow/index.js";
export interface StreamMessage {
    schema: VgiSchema;
    batches: VgiBatch[];
}
/**
 * Reads sequential IPC streams from a byte source (e.g., process.stdin).
 * Uses autoDestroy: false + reset/open pattern to read multiple streams
 * from the same underlying byte source.
 */
export declare class IpcStreamReader {
    private reader;
    private initialized;
    /** True once readNextBatch() returns null (EOS reached for current stream). */
    private streamEnded;
    private constructor();
    static create(input: ReadableStream<Uint8Array> | NodeJS.ReadableStream): Promise<IpcStreamReader>;
    /**
     * Read one complete IPC stream (schema + all batches).
     * Returns null on EOF (no more streams).
     */
    readStream(): Promise<StreamMessage | null>;
    /**
     * Open the next IPC stream and return its schema.
     * Use readNextBatch() to read batches one at a time.
     * Returns null on EOF.
     */
    openNextStream(): Promise<VgiSchema | null>;
    /**
     * Read the next batch from the currently open IPC stream.
     * Returns null when the stream ends (EOS).
     *
     * Once EOS is reached, subsequent calls return null immediately without
     * reading from the underlying byte source. This prevents the Arrow-JS
     * reader from consuming bytes that belong to the next IPC stream.
     */
    readNextBatch(): Promise<VgiBatch | null>;
    cancel(): Promise<void>;
}
//# sourceMappingURL=reader.d.ts.map