/** Error thrown when the server encounters an RPC protocol error. */
export declare class RpcError extends Error {
    /** Remote error class name (e.g. `"ValueError"`). */
    readonly errorType: string;
    /** Human-readable message from the remote error. */
    readonly errorMessage: string;
    /** Remote stack-trace text, or an empty string when unavailable. */
    readonly remoteTraceback: string;
    constructor(
    /** Remote error class name (e.g. `"ValueError"`). */
    errorType: string, 
    /** Human-readable message from the remote error. */
    errorMessage: string, 
    /** Remote stack-trace text, or an empty string when unavailable. */
    remoteTraceback: string);
}
/** Error thrown when the client sends an unsupported request version. */
export declare class VersionError extends Error {
    constructor(message: string);
}
/** `vgi_rpc.error_kind` batch-metadata value for {@link MethodNotImplementedError}.
 *  Mirrors Python's `vgi_rpc.metadata.ERROR_KIND_*` constants. */
export declare const ERROR_KIND_METHOD_NOT_IMPLEMENTED = "method_not_implemented";
/** `vgi_rpc.error_kind` batch-metadata value for {@link SessionLostError}. */
export declare const ERROR_KIND_SESSION_LOST = "session_lost";
/** `vgi_rpc.error_kind` batch-metadata value for {@link ServerDrainingError}. */
export declare const ERROR_KIND_SERVER_DRAINING = "server_draining";
export declare const ERROR_KIND_PROTOCOL_VERSION_MISMATCH = "protocol_version_mismatch";
/** Raised when the client's declared `vgi_rpc.protocol_version` is
 *  incompatible with the server's. Subclass of `VersionError` so existing
 *  catch sites continue to write a typed error stream and keep serving.
 *  Carries a directional message that tells the reader which side to
 *  upgrade. Mirrors Python's `vgi_rpc.rpc.ProtocolVersionError`. */
export declare class ProtocolVersionError extends VersionError {
    static readonly errorKind = "protocol_version_mismatch";
    readonly errorKind = "protocol_version_mismatch";
    constructor(message: string);
}
/** Parse a canonical semver string into `[major, minor, patch]`. Throws on
 *  any input that isn't `MAJOR.MINOR.PATCH` with non-negative integers and
 *  no leading zeros (except literal `0`). No prereleases, no build metadata.
 *  Mirrors Python's `vgi_rpc.metadata.parse_version`. */
export declare function parseProtocolVersion(value: string): [number, number, number];
/** Raised when a client invokes a method the server does not implement.
 *
 *  Mirrors Python's `vgi_rpc.rpc.MethodNotImplementedError`. The static
 *  `errorKind` is hoisted onto the error batch metadata as
 *  `vgi_rpc.error_kind` so clients can branch on the typed marker without
 *  string-matching the message.
 */
export declare class MethodNotImplementedError extends Error {
    /** Typed `vgi_rpc.error_kind` marker for this error class. */
    static readonly errorKind = "method_not_implemented";
    /** Typed `vgi_rpc.error_kind` marker hoisted onto the error batch metadata. */
    readonly errorKind = "method_not_implemented";
    constructor(message: string);
}
/** Raised when a sticky session token is malformed, expired, evicted, or
 *  bound to a different worker / principal. HTTP-only. */
export declare class SessionLostError extends Error {
    /** Typed `vgi_rpc.error_kind` marker for this error class. */
    static readonly errorKind = "session_lost";
    /** Typed `vgi_rpc.error_kind` marker hoisted onto the error batch metadata. */
    readonly errorKind = "session_lost";
    constructor(message: string);
}
/** Raised when `ctx.openSession` is called while the server is draining. */
export declare class ServerDrainingError extends Error {
    /** Typed `vgi_rpc.error_kind` marker for this error class. */
    static readonly errorKind = "server_draining";
    /** Typed `vgi_rpc.error_kind` marker hoisted onto the error batch metadata. */
    readonly errorKind = "server_draining";
    constructor(message: string);
}
//# sourceMappingURL=errors.d.ts.map