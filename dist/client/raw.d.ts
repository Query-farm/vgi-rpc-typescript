import type { RecordBatch } from "@query-farm/apache-arrow";
/**
 * One Arrow record batch together with the Arrow custom metadata that rode
 * with it on the wire.
 *
 * The row-oriented client surface ({@link RpcClient.call},
 * {@link StreamSession.exchange}) decodes values for the caller and drops the
 * metadata map. The raw surface does neither: it is the entry point for a
 * caller that already holds encoded Arrow — a relay, a proxy, or a
 * conformance driver exercising this client against a foreign server — and
 * needs the batch and its metadata to cross unchanged in both directions.
 */
export interface RawBatch {
    /** The batch itself, exactly as encoded on the wire. */
    batch: RecordBatch;
    /** The batch's Arrow custom metadata, empty when the peer attached none. */
    metadata: Map<string, string>;
}
/** One producer batch paired with the opaque token that resumes after it. */
export interface RawBatchWithToken {
    /** The batch and its custom metadata. */
    item: RawBatch;
    /**
     * The continuation token resuming the stream after this batch, or `null`
     * on a transport that carries no resumable stream state (every
     * byte-stream transport) and at the producer's final turn.
     */
    token: string | null;
}
/**
 * A streaming call driven one encoded batch at a time.
 *
 * Mirrors {@link StreamSession} turn for turn, but every payload is a
 * {@link RawBatch} rather than decoded rows. Obtained from
 * {@link RpcClient.streamRaw}.
 */
export interface RawStreamSession {
    /** The stream's header batch, or `null` when the method declares none. */
    readonly rawHeader: RawBatch | null;
    /**
     * Pull the next batch from a producer stream, sending `metadata` as the
     * tick's application custom metadata. Resolves to `null` at end of stream.
     */
    tickRaw(metadata?: ReadonlyMap<string, string>): Promise<RawBatch | null>;
    /**
     * Send one batch (with its custom metadata) and return the reply.
     * Resolves to `null` when the server ended the stream instead of replying.
     */
    exchangeRaw(input: RawBatch): Promise<RawBatch | null>;
    /**
     * Pull the next producer batch together with the token that resumes the
     * stream after it. Resolves to `null` at end of stream.
     */
    nextWithTokenRaw(): Promise<RawBatchWithToken | null>;
    /**
     * Ask the server to stop producing and release the stream's state.
     *
     * Best-effort and idempotent: transport failures are swallowed, and the
     * session is finished either way.
     */
    cancel(): Promise<void>;
    /** Release the stream without cancelling it. */
    close(): void;
}
//# sourceMappingURL=raw.d.ts.map