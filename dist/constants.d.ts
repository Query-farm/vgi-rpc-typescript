/** Batch-metadata key carrying the invoked RPC method name. */
export declare const RPC_METHOD_KEY = "vgi_rpc.method";
/** Names the protocol a request addresses -- the routing key.
 *
 *  Dispatch resolves the pair `(protocol, method)`: a server hosts one or more
 *  protocols and method names may collide across them, which is what lets
 *  protocols be authored independently. Required on every request, including
 *  against a server hosting exactly one protocol -- an exemption would let an
 *  intermediary that rebuilds a request and drops the field land silently on
 *  whichever protocol happened to be first, rather than being told.
 *
 *  The major version is part of the protocol name (`vgi_rpc.Reflection.v1`), so
 *  an incompatible major is a routing failure rather than a parse failure, and
 *  v1 and v2 can be served side by side while clients migrate. */
export declare const PROTOCOL_KEY = "vgi_rpc.protocol";
/** Batch-metadata key carrying a log batch's severity level. */
export declare const LOG_LEVEL_KEY = "vgi_rpc.log_level";
/** Batch-metadata key carrying a log batch's message text. */
export declare const LOG_MESSAGE_KEY = "vgi_rpc.log_message";
/** Batch-metadata key carrying a log batch's structured extra fields. */
export declare const LOG_EXTRA_KEY = "vgi_rpc.log_extra";
/** Batch-metadata key carrying the wire request-framing version. */
export declare const REQUEST_VERSION_KEY = "vgi_rpc.request_version";
/** Current wire request-framing version. Distinct from the application-level
 *  {@link PROTOCOL_VERSION_KEY protocol version}. */
export declare const REQUEST_VERSION = "1";
/** Batch-metadata key identifying the server instance that produced a batch. */
export declare const SERVER_ID_KEY = "vgi_rpc.server_id";
/** Batch-metadata key carrying the client-supplied request id. */
export declare const REQUEST_ID_KEY = "vgi_rpc.request_id";
/** Application protocol surface version. Carried on every request batch from
 *  a client bound to a Protocol that declares `protocolVersion`, and reported
 *  by `vgi_rpc.Reflection.v1`. Format: canonical semver
 *  MAJOR.MINOR.PATCH. Enforced at the dispatch boundary on the server: exact
 *  major+minor match required, patch ignored. Distinct from `REQUEST_VERSION`
 *  (wire framing). Mirrors Python's `PROTOCOL_VERSION_KEY`. */
export declare const PROTOCOL_VERSION_KEY = "vgi_rpc.protocol_version";
/** Batch-metadata key carrying the base64-encoded stream continuation/state token. */
export declare const STATE_KEY = "vgi_rpc.stream_state#b64";
/**
 * The stream's *call state* — the half of a stream's state fixed for the life
 * of the call (the init request, the resolved schemas). A server that splits
 * its stream state mints this once on `/init` and never re-issues it; only
 * {@link STATE_KEY}, the cursor, comes back per turn. A client must echo it on
 * every subsequent request: the server may resolve it from a cache while one
 * is warm, but a continuation landing on a process that never saw the `/init`
 * has only the client's copy to work from.
 */
export declare const CALL_STATE_KEY = "vgi_rpc.call_state#b64";
export declare const CANCEL_KEY = "vgi_rpc.cancel";
export declare const LOCATION_KEY = "vgi_rpc.location";
export declare const LOCATION_SHA256_KEY = "vgi_rpc.location.sha256";
/** How long resolving an external pointer took, in milliseconds, stamped on
 *  the *resolved* batch — never on the pointer. WIRE_PROTOCOL.md §12. */
export declare const LOCATION_FETCH_MS_KEY = "vgi_rpc.location.fetch_ms";
/** Where a resolved batch came from: the *original* pointer URL, not the last
 *  redirect target, and unredacted — §12 classes it as application metadata
 *  rather than a diagnostic string. Stamped on the resolved batch, which by
 *  then is the only record of its own origin: the pointer that named it has
 *  been discarded. */
export declare const LOCATION_SOURCE_KEY = "vgi_rpc.location.source";
/** HTTP response header set when an RPC error is returned over the HTTP transport. */
export declare const RPC_ERROR_HEADER = "X-VGI-RPC-Error";
/** Per-request correlation header. Read from the request when the caller
 *  supplies one, minted otherwise, echoed on the response, and written to the
 *  access log as `request_id` — the same value in all three places, which is
 *  the only property that makes the field worth anything. */
export declare const REQUEST_ID_HEADER = "X-Request-ID";
/** Top-level metadata key on an EXCEPTION batch identifying the error category.
 *  Hoisted by `buildErrorBatch` when the thrown error has a static or instance
 *  `errorKind` property. Mirrors Python's `vgi_rpc.metadata.ERROR_KIND_KEY`. */
export declare const ERROR_KIND_KEY = "vgi_rpc.error_kind";
//# sourceMappingURL=constants.d.ts.map