import { Schema } from "@query-farm/apache-arrow";
import { type ExternalLocationConfig } from "../external.js";
import { IpcStreamReader } from "../wire/reader.js";
import type { RpcClient } from "./connect.js";
import type { ExchangeInput, LogMessage, PipeConnectOptions, StreamSession, SubprocessConnectOptions } from "./types.js";
interface PipeWritable {
    write(data: Uint8Array): void;
    flush?(): void;
    end(): void;
}
type WriteFn = (bytes: Uint8Array) => void;
/**
 * {@link StreamSession} implementation for the pipe/subprocess transport.
 * Drives lockstep streaming over a single bidirectional pipe: each
 * {@link PipeStreamSession.exchange} or iteration step writes one input batch
 * and reads one output batch. Holds the connection's single-threaded busy lock
 * until closed.
 */
export declare class PipeStreamSession implements StreamSession {
    private _reader;
    private _writeFn;
    private _onLog?;
    private _header;
    private _inputWriter;
    private _inputSchema;
    private _outputStreamOpened;
    private _closed;
    private _outputSchema;
    private _releaseBusy;
    private _setDrainPromise;
    private _externalConfig?;
    constructor(opts: {
        reader: IpcStreamReader;
        writeFn: WriteFn;
        onLog?: (msg: LogMessage) => void;
        header: Record<string, any> | null;
        outputSchema: Schema;
        releaseBusy: () => void;
        setDrainPromise: (p: Promise<void>) => void;
        externalConfig?: ExternalLocationConfig;
    });
    /** The stream's one-time header row, or `null` if the method declares no header. */
    get header(): Record<string, any> | null;
    /**
     * Read output batches from the server until a data batch is found.
     * Dispatches log/error batches along the way.
     * Returns null when server closes output stream (EOS).
     */
    private _readOutputBatch;
    /**
     * Ensure the server's output stream is opened for reading.
     * Must be called AFTER sending the first input batch, because
     * the server's output schema may not be flushed until it processes
     * the first input and writes the first output batch.
     */
    private _ensureOutputStream;
    /**
     * Send an exchange request and return the data rows.
     */
    exchange(input: ExchangeInput): Promise<Record<string, any>[]>;
    /**
     * Clean up after an error: close input, drain output, release busy.
     */
    private _cleanup;
    /**
     * Iterate over producer stream batches (lockstep).
     */
    [Symbol.asyncIterator](): AsyncIterableIterator<Record<string, any>[]>;
    /**
     * End the stream: close the input side (or send an empty stream if nothing
     * was sent yet) and drain the server's remaining output in the background,
     * releasing the connection's busy lock once the drain completes.
     */
    close(): void;
}
/**
 * Connect to a vgi-rpc server over a raw bidirectional pipe (a readable stream
 * of server output plus a writable for client input). The connection is
 * single-threaded: only one call or stream may be in flight at a time. The
 * `__describe__` handshake is sent before the reader is opened to avoid deadlock.
 */
export declare function pipeConnect(readable: ReadableStream<Uint8Array>, writable: PipeWritable, options?: PipeConnectOptions): RpcClient;
/**
 * Spawn a server process (via `Bun.spawn`) and connect to it over its
 * stdin/stdout using {@link pipeConnect}. The returned client's
 * {@link RpcClient.close} also kills the subprocess.
 */
export declare function subprocessConnect(cmd: string[], options?: SubprocessConnectOptions): RpcClient;
export {};
//# sourceMappingURL=pipe.d.ts.map