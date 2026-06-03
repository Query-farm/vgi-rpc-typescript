import { type VgiBatch, type VgiSchema } from "../arrow/index.js";
import type { CookieSpec } from "../types.js";
/** MIME type for Arrow IPC stream request and response bodies. */
export declare const ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream";
export declare const SESSION_HEADER = "VGI-Session";
export declare const SESSION_ACCEPT_HEADER = "VGI-Session-Accept";
export declare const SESSION_CLOSE_HEADER = "VGI-Session-Close";
export declare const STICKY_ENABLED_HEADER = "VGI-Sticky-Enabled";
export declare const STICKY_DEFAULT_TTL_HEADER = "VGI-Sticky-Default-TTL";
export declare const STICKY_ECHO_HEADERS_HEADER = "VGI-Sticky-Echo-Headers";
/** Prefix the server uses to tell the client "echo this header on subsequent
 *  requests in this session". Clients capture and replay
 *  `VGI-Echo-<name>: <value>` as plain `<name>: <value>` for the session
 *  lifetime — used for client-driven routing (e.g. `fly-force-instance-id`). */
export declare const ECHO_HEADER_PREFIX = "VGI-Echo-";
/** Framework-managed sticky session teardown endpoint path component.
 *  `DELETE {prefix}/__session__` idempotently closes the session referenced
 *  by the request's `VGI-Session` header. */
export declare const SESSION_ENDPOINT = "__session__";
/** Serialize a CookieSpec into a Set-Cookie header value. */
export declare function formatSetCookieHeader(c: CookieSpec): string;
/** Append Set-Cookie headers for each queued CookieSpec onto an existing Headers object. */
export declare function appendCookieHeaders(headers: Headers, cookies: readonly CookieSpec[]): void;
export declare class HttpRpcError extends Error {
    readonly statusCode: number;
    constructor(message: string, statusCode: number);
}
/**
 * Serialize a schema + batches into a complete IPC stream as Uint8Array.
 *
 * A single IPC stream is `[schema_msg, batch_msg, batch_msg, ..., EOS]`.
 * Each backend implements `serializeBatches` to write that atomically —
 * arrow-js via `RecordBatchStreamWriter`, flechette via `tablesToIPC`
 * (added in our flechette fork). Naive concatenation of per-batch streams
 * produces multiple EOS markers and breaks readers.
 */
export declare function serializeIpcStream(schema: VgiSchema, batches: VgiBatch[]): Uint8Array;
/**
 * Create a Response with Arrow IPC content type.
 *
 * Server errors (status 500) are translated to HTTP 200 with an
 * ``X-VGI-RPC-Error: true`` header so that clients which discard
 * response bodies on 5xx still receive the Arrow IPC error metadata.
 * Client errors (400, 401, 404, 415) are passed through unchanged.
 */
export declare function arrowResponse(body: Uint8Array, status?: number, extraHeaders?: Headers): Response;
/** Read schema + first batch from an IPC stream body via the facade. */
export declare function readRequestFromBody(body: Uint8Array): Promise<{
    schema: VgiSchema;
    batch: VgiBatch;
}>;
//# sourceMappingURL=common.d.ts.map