import { RecordBatch, Schema } from "@query-farm/apache-arrow";
import { type ExternalLocationConfig } from "../external.js";
import type { LogMessage, StreamSession } from "./types.js";
type CompressFn = (data: Uint8Array, level: number) => Promise<Uint8Array>;
type DecompressFn = (data: Uint8Array) => Promise<Uint8Array>;
/**
 * Posts an Arrow IPC request body to *url*, transparently handling
 * client-vended request externalization. Provided by the parent connection
 * so a single capability cache can drive both unary and stream call paths.
 */
export type PostFn = (url: string, body: Uint8Array) => Promise<Response>;
/**
 * {@link StreamSession} implementation for the HTTP transport. Stream state is
 * carried statelessly across requests via an HMAC state token: each
 * {@link HttpStreamSession.exchange} or producer-continuation POST sends the
 * current token and receives the next one in the response metadata.
 */
export declare class HttpStreamSession implements StreamSession {
    private _baseUrl;
    private _prefix;
    private _method;
    private _stateToken;
    private _outputSchema;
    private _inputSchema?;
    private _onLog?;
    private _pendingBatches;
    private _finished;
    private _header;
    private _compressionLevel?;
    private _compressFn?;
    private _decompressFn?;
    private _authorization?;
    private _externalConfig?;
    private _postFn?;
    constructor(opts: {
        baseUrl: string;
        prefix: string;
        method: string;
        stateToken: string | null;
        outputSchema: Schema;
        inputSchema?: Schema;
        onLog?: (msg: LogMessage) => void;
        pendingBatches: RecordBatch[];
        finished: boolean;
        header: Record<string, any> | null;
        compressionLevel?: number;
        compressFn?: CompressFn;
        decompressFn?: DecompressFn;
        authorization?: string;
        externalConfig?: ExternalLocationConfig;
        postFn?: PostFn;
    });
    private _post;
    /** The stream's one-time header row, or `null` if the method declares no header. */
    get header(): Record<string, any> | null;
    private _buildHeaders;
    private _prepareBody;
    private _readResponse;
    /**
     * Send an exchange request and return the data rows.
     */
    exchange(input: Record<string, any>[]): Promise<Record<string, any>[]>;
    private _doExchange;
    private _buildEmptyBatch;
    /**
     * Iterate over producer stream batches.
     */
    [Symbol.asyncIterator](): AsyncIterableIterator<Record<string, any>[]>;
    private _sendContinuation;
    /** No-op: the HTTP transport is stateless, so there is nothing to tear down. */
    close(): void;
}
export {};
//# sourceMappingURL=stream.d.ts.map