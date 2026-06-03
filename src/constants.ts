// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Well-known metadata keys matching Python's metadata.py.

/** Batch-metadata key carrying the invoked RPC method name. */
export const RPC_METHOD_KEY = "vgi_rpc.method";
/** Batch-metadata key carrying a log batch's severity level. */
export const LOG_LEVEL_KEY = "vgi_rpc.log_level";
/** Batch-metadata key carrying a log batch's message text. */
export const LOG_MESSAGE_KEY = "vgi_rpc.log_message";
/** Batch-metadata key carrying a log batch's structured extra fields. */
export const LOG_EXTRA_KEY = "vgi_rpc.log_extra";
/** Batch-metadata key carrying the wire request-framing version. */
export const REQUEST_VERSION_KEY = "vgi_rpc.request_version";
/** Current wire request-framing version. Distinct from the application-level
 *  {@link PROTOCOL_VERSION_KEY protocol version}. */
export const REQUEST_VERSION = "1";

/** Batch-metadata key identifying the server instance that produced a batch. */
export const SERVER_ID_KEY = "vgi_rpc.server_id";
/** Batch-metadata key carrying the client-supplied request id. */
export const REQUEST_ID_KEY = "vgi_rpc.request_id";

/** Batch-metadata key carrying the service / protocol name. */
export const PROTOCOL_NAME_KEY = "vgi_rpc.protocol_name";
/** Batch-metadata key carrying the `__describe__` response schema version. */
export const DESCRIBE_VERSION_KEY = "vgi_rpc.describe_version";
export const PROTOCOL_HASH_KEY = "vgi_rpc.protocol_hash";
/** Current `__describe__` response schema version (the slim 8-column schema). */
export const DESCRIBE_VERSION = "4";

/** Application protocol surface version. Carried on every request batch from
 *  a client bound to a Protocol that declares `protocolVersion`; also emitted
 *  in the __describe__ response metadata. Format: canonical semver
 *  MAJOR.MINOR.PATCH. Enforced at the dispatch boundary on the server: exact
 *  major+minor match required, patch ignored. Distinct from `REQUEST_VERSION`
 *  (wire framing). Mirrors Python's `PROTOCOL_VERSION_KEY`. */
export const PROTOCOL_VERSION_KEY = "vgi_rpc.protocol_version";

/** Reserved method name for the introspection (`__describe__`) call. */
export const DESCRIBE_METHOD_NAME = "__describe__";

/** Batch-metadata key carrying the base64-encoded stream continuation/state token. */
export const STATE_KEY = "vgi_rpc.stream_state#b64";
export const CANCEL_KEY = "vgi_rpc.cancel";

export const LOCATION_KEY = "vgi_rpc.location";
export const LOCATION_SHA256_KEY = "vgi_rpc.location.sha256";

/** HTTP response header set when an RPC error is returned over the HTTP transport. */
export const RPC_ERROR_HEADER = "X-VGI-RPC-Error";

/** Top-level metadata key on an EXCEPTION batch identifying the error category.
 *  Hoisted by `buildErrorBatch` when the thrown error has a static or instance
 *  `errorKind` property. Mirrors Python's `vgi_rpc.metadata.ERROR_KIND_KEY`. */
export const ERROR_KIND_KEY = "vgi_rpc.error_kind";
