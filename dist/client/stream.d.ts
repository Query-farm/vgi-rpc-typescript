import type { RecordBatch, Schema } from "@query-farm/apache-arrow";
import { type ExternalLocationConfig } from "../external.js";
import type { RawBatch, RawBatchWithToken, RawStreamSession } from "./raw.js";
import type { ExchangeInput, LogMessage, StreamSession } from "./types.js";
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
     *
     * Since a stream's state may travel as two tokens (call + cursor), this is
     * the *pair*, packed into one opaque string by {@link packResumeToken}.
     * Treat it as unstructured text; only {@link HttpStreamSession.seekToToken}
     * and the client's `resumeStream` need to know its shape.
     */
    token: string | null;
}
/**
 * Pack a stream's cursor and call tokens into one opaque resume blob.
 *
 * Both halves have to travel: a node resuming from this token may have no
 * cached knowledge of the stream, and the server never re-issues the call
 * token.
 *
 * Layout is `<cursorLen>:<cursor><call>`, and a stream with no call token
 * packs to the bare cursor. Both tokens are base64, whose alphabet contains
 * no `:`, so a bare cursor can never be mistaken for a packed pair — which is
 * what keeps tokens minted before the split readable.
 */
export declare function packResumeToken(cursor: string, callToken: string | null): string;
/**
 * Unpack a blob produced by {@link packResumeToken}.
 *
 * A blob with no length prefix is a bare cursor — either from a server that
 * does not split its stream state, or from a client predating the split.
 */
export declare function unpackResumeToken(token: string): {
    cursor: string;
    callToken: string | null;
};
/**
 * {@link StreamSession} implementation for the HTTP transport. Stream state is
 * carried statelessly across requests via an HMAC state token: each
 * {@link HttpStreamSession.exchange} or producer-continuation POST sends the
 * current token and receives the next one in the response metadata.
 */
export declare class HttpStreamSession implements StreamSession, RawStreamSession {
    private _baseUrl;
    private _prefix;
    private _method;
    private _stateToken;
    /**
     * The stream's call token: handed over once by `/init` and echoed on every
     * subsequent request. The server never re-issues it, so this is the only
     * copy once the init response is parsed.
     */
    private _callStateToken;
    private _outputSchema;
    private _inputSchema?;
    private _onLog?;
    private _pendingBatches;
    private _finished;
    private _header;
    private _rawHeader;
    private _compressionLevel?;
    private _compressFn?;
    private _decompressFn?;
    private _authorization?;
    private _externalConfig?;
    private _postFn?;
    private _acceptedMaxResponseBytes;
    private _responseBudgetSupport;
    constructor(opts: {
        baseUrl: string;
        /** The already-namespaced `{prefix}/{protocol}` an `/exchange` path hangs
         *  off. Folded by the caller, which learns the protocol from
         *  the server's description. */
        prefix: string;
        method: string;
        stateToken: string | null;
        callStateToken?: string | null;
        outputSchema: Schema;
        inputSchema?: Schema;
        onLog?: (msg: LogMessage) => void;
        pendingBatches: RecordBatch[];
        finished: boolean;
        header: Record<string, any> | null;
        rawHeader?: RawBatch | null;
        compressionLevel?: number;
        compressFn?: CompressFn;
        decompressFn?: DecompressFn;
        authorization?: string;
        externalConfig?: ExternalLocationConfig;
        postFn?: PostFn;
        acceptedMaxResponseBytes?: number;
    });
    private _post;
    /** The stream's one-time header row, or `null` if the method declares no header. */
    get header(): Record<string, any> | null;
    /** The stream's header batch and its custom metadata, undecoded. */
    get rawHeader(): RawBatch | null;
    /**
     * Build request metadata carrying the cursor token and the call token.
     *
     * The call token is echoed on every request because the server does not
     * re-issue it; a request that omitted it would still succeed while the
     * server's call-state cache is warm and fail once it is not — exactly the
     * kind of load-dependent bug worth designing out.
     */
    private _tokenMetadata;
    /** Encode this session's current position as one opaque resume blob. */
    private _resumeToken;
    private _buildHeaders;
    private _prepareBody;
    private _readResponse;
    /**
     * Send an exchange request and return the data rows.
     */
    exchange(input: ExchangeInput): Promise<Record<string, any>[]>;
    /** Send one producer continuation tick with application custom metadata. */
    tick(metadata?: ReadonlyMap<string, string>): Promise<Record<string, any>[]>;
    /**
     * Send one producer tick and return the batch undecoded.
     *
     * A tick is one batch forward, whatever it took to get there: `/init` may
     * already have buffered the first one, in which case this consumes that
     * rather than issuing a continuation for a batch the client is holding.
     * Refusing outright — which this did — made `tick()` unusable on the HTTP
     * transport, because the *first* tick of every producer is the buffered
     * one. Only explicit `metadata` is still refused while a batch is
     * buffered, and for a reason that survives: that metadata belongs on a
     * request this turn does not make.
     */
    tickRaw(metadata?: ReadonlyMap<string, string>): Promise<RawBatch | null>;
    /**
     * Send one encoded batch with its custom metadata and read the reply.
     *
     * The declared-batch branch of {@link HttpStreamSession.exchange} without
     * the row decoding: the caller's schema, buffers and metadata cross
     * verbatim (plus this turn's stream tokens), and the reply comes back as
     * the server encoded it.
     */
    exchangeRaw(input: RawBatch): Promise<RawBatch | null>;
    /**
     * Ask the server to discard this stream's state and stop producing.
     *
     * Sends `POST {prefix}/{method}/exchange` carrying `vgi_rpc.cancel`
     * alongside the current tokens, so the server runs the state's cancel hook
     * and releases it. Idempotent and best-effort: a transport failure here is
     * swallowed, because the session is finished either way. Mirrors Python's
     * `HttpStreamSession.cancel`.
     */
    cancel(): Promise<void>;
    private _doExchange;
    /** Decode an exchange reply for the row-oriented surface. */
    private _rowsOfExchange;
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
     * Returns `null` at end-of-stream. Throws a ProtocolError if a peer violates
     * the lock-step contract by returning more than one data batch in a turn.
     *
     * Drives the same wire protocol as async iteration but yields one
     * `{ rows, token }` per call instead of auto-following the token. Do not
     * interleave with iteration/`exchange` on the same session.
     *
     * Mirrors Python's `HttpStreamSession.next_with_token`.
     */
    nextWithToken(): Promise<RowsWithToken | null>;
    /**
     * Read one producer batch, undecoded, together with its resume token.
     *
     * The batch-level twin of {@link HttpStreamSession.nextWithToken}; see
     * there for the resume-token contract.
     */
    nextWithTokenRaw(metadata?: ReadonlyMap<string, string>): Promise<RawBatchWithToken | null>;
    /**
     * Reposition a freshly-initialised session to resume from `token`.
     *
     * Discards any init-preloaded batches and points the session at the given
     * resume token (as returned by {@link HttpStreamSession.nextWithToken}), so
     * the next `nextWithToken()` continues from exactly there. Used to resume a
     * scan on a new process/node — which is why the call token travels inside
     * the blob too: that node may never have seen this stream's `/init`.
     * Mirrors Python's `seek_to_token`.
     */
    seekToToken(token: string): void;
    private _sendContinuation;
    /** No-op: the HTTP transport is stateless, so there is nothing to tear down. */
    close(): void;
}
export {};
//# sourceMappingURL=stream.d.ts.map