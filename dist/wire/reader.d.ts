import type { VgiBatch, VgiSchema } from "../arrow/index.js";
export interface StreamMessage {
    schema: VgiSchema;
    batches: VgiBatch[];
}
/**
 * Largest byte count asked of a single Node `Readable.read(n)`.
 *
 * arrow-js's Node-stream adapter asks for a whole IPC message body in one
 * call. Both Node and Bun throw `ERR_OUT_OF_RANGE` from `read(n)` once `n`
 * exceeds their shared 1 GiB `MAX_HWM`, so a record batch with a body over
 * 1 GiB dies before a single byte is decoded — the read-side twin of the
 * write clamp in ./writer.ts. The adapter already loops until it has the
 * bytes it asked for, so answering with less is absorbed; it only ever
 * mis-handles being answered with *more*.
 */
export declare const MAX_READ_CHUNK: number;
/**
 * Wrap a Node `Readable` so `read(n)` never asks for more than
 * {@link MAX_READ_CHUNK}. Everything else passes through untouched, including
 * the `read`/`pipe`/`readable` trio arrow-js sniffs to pick this adapter.
 */
export declare function clampReads<T extends object>(stream: T): T;
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