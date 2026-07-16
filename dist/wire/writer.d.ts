import type { Socket } from "node:net";
import type { VgiBatch, VgiSchema } from "../arrow/index.js";
/**
 * A browser/worker-friendly byte sink: `write` receives each fully-serialized
 * chunk of response bytes (a schema msg, a batch, or the EOS marker) and must
 * deliver them in order. Used by the in-browser SAB (`worker:`) transport, which
 * has no Node fd/Socket — the sink writes into the worker→client ring.
 */
export type ByteSink = {
    /** Deliver one fully-serialized chunk of response bytes, in order. */
    write: (bytes: Uint8Array) => void | Promise<void>;
};
type WriterTarget = {
    kind: "fd";
    fd: number;
} | {
    kind: "socket";
    socket: Socket;
} | {
    kind: "sink";
    sink: ByteSink;
};
/**
 * Writes sequential IPC streams to either an fd (stdio subprocess transport)
 * or a Node Socket (AF_UNIX transport). Each call to writeStream() writes a
 * complete IPC stream: schema + batches + EOS.
 *
 * All public methods are async. The fd path resolves immediately after a
 * synchronous writeSync; the socket path awaits real `'drain'` events on
 * backpressure so the event loop stays responsive to other connections.
 */
export declare class IpcStreamWriter {
    private readonly target;
    /**
     * Construct from a file descriptor (stdio transport) or a Node net.Socket
     * (AF_UNIX transport). The default targets stdout for legacy stdio servers
     * that didn't pass an fd.
     */
    constructor(fdOrSocketOrSink?: number | Socket | ByteSink);
    /**
     * Write a complete IPC stream with the given schema and batches.
     * Creates schema message, writes all batches (with their metadata), writes EOS.
     */
    writeStream(schema: VgiSchema, batches: VgiBatch[]): Promise<void>;
    /**
     * Open an incremental IPC stream for writing batches one at a time.
     */
    openStream(schema: VgiSchema): IncrementalStream;
}
/**
 * An open IPC stream that supports incremental batch writes.
 *
 * Drives a backend {@link IncrementalEncoder} and flushes its bytes through
 * the same target (fd or socket) as the parent IpcStreamWriter. The write()
 * and close() methods are async so the socket path can yield on backpressure
 * — critical under AF_UNIX where the kernel send buffer (~8 KB on macOS)
 * fills quickly and any synchronous busy-wait would starve every other
 * connection sharing this event loop.
 *
 * The encoder is obtained from the Arrow facade, so this file no longer
 * imports arrow-js directly — keeping arrow-js out of the flechette
 * (workerd/browser) bundle. The flechette encoder throws on construction;
 * the stdio exchange protocol is lockstep (the client reads each response
 * batch before sending the next input) which needs an incremental writer
 * flechette doesn't provide. workerd/browser deployments use HTTP (no
 * stdio), so the flechette path is never reached there; `flechette-pipe`
 * conformance is xfailed for streams.
 */
export declare class IncrementalStream {
    private readonly encoder;
    private readonly target;
    private closed;
    private writeChain;
    constructor(target: WriterTarget, schema: VgiSchema);
    /** Write a single batch. Resolves once the bytes are queued/flushed. */
    write(batch: VgiBatch): Promise<void>;
    /** Close the stream (writes EOS marker). */
    close(): Promise<void>;
    private enqueue;
}
export {};
//# sourceMappingURL=writer.d.ts.map