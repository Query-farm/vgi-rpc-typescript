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
 * One producer batch's rows paired with the continuation token that resumes
 * the stream AFTER that batch. Returned by {@link HttpStreamSession.nextWithToken}.
 */
export interface RowsWithToken {
    /** The data batch's rows. */
    rows: Record<string, any>[];
    /**
     * The resume token continuing the stream after this batch — the worker's
     * own serialized producer state — or `null` when the producer emitted this
     * batch as its final turn (no further continuation).
     */
    token: string | null;
}
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
    /**
     * Read one producer batch and surface the worker's continuation token.
     *
     * Reads exactly one data batch and returns it paired with the resume token
     * that continues the stream AFTER that batch — the worker's own serialized
     * producer state. A fresh session positioned at that token (see
     * {@link HttpStreamSession.seekToToken} / the client's `resumeStream`)
     * resumes on any node, which is the basis for stateless, load-balanced
     * relays that must not pin a scan to one process.
     *
     * Returns `null` at end-of-stream. Requires per-batch continuation tokens
     * (the default server behaviour — i.e. the worker is not configured with
     * `max_response_bytes`); throws if a single response carries more than one
     * data batch (coarser-than-batch resume is not representable here).
     *
     * Drives the same wire protocol as async iteration but yields one
     * `{ rows, token }` per call instead of auto-following the token. Do not
     * interleave with iteration/`exchange` on the same session.
     *
     * Mirrors Python's `HttpStreamSession.next_with_token`.
     */
    nextWithToken(): Promise<RowsWithToken | null>;
    /**
     * Reposition a freshly-initialised session to resume from `token`.
     *
     * Discards any init-preloaded batches and points the session at the given
     * continuation token (as returned by {@link HttpStreamSession.nextWithToken}),
     * so the next `nextWithToken()` continues from exactly there. Used to resume
     * a scan on a new process/node. Mirrors Python's `seek_to_token`.
     */
    seekToToken(token: string): void;
    private _sendContinuation;
    /** No-op: the HTTP transport is stateless, so there is nothing to tear down. */
    close(): void;
}
export {};
//# sourceMappingURL=stream.d.ts.map