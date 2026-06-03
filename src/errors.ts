// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/** Error thrown when the server encounters an RPC protocol error. */
export class RpcError extends Error {
  constructor(
    /** Remote error class name (e.g. `"ValueError"`). */
    public readonly errorType: string,
    /** Human-readable message from the remote error. */
    public readonly errorMessage: string,
    /** Remote stack-trace text, or an empty string when unavailable. */
    public readonly remoteTraceback: string,
  ) {
    super(`${errorType}: ${errorMessage}`);
    this.name = "RpcError";
  }
}

/** Error thrown when the client sends an unsupported request version. */
export class VersionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "VersionError";
  }
}

/** `vgi_rpc.error_kind` batch-metadata value for {@link MethodNotImplementedError}.
 *  Mirrors Python's `vgi_rpc.metadata.ERROR_KIND_*` constants. */
export const ERROR_KIND_METHOD_NOT_IMPLEMENTED = "method_not_implemented";
/** `vgi_rpc.error_kind` batch-metadata value for {@link SessionLostError}. */
export const ERROR_KIND_SESSION_LOST = "session_lost";
/** `vgi_rpc.error_kind` batch-metadata value for {@link ServerDrainingError}. */
export const ERROR_KIND_SERVER_DRAINING = "server_draining";
export const ERROR_KIND_PROTOCOL_VERSION_MISMATCH = "protocol_version_mismatch";

/** Raised when the client's declared `vgi_rpc.protocol_version` is
 *  incompatible with the server's. Subclass of `VersionError` so existing
 *  catch sites continue to write a typed error stream and keep serving.
 *  Carries a directional message that tells the reader which side to
 *  upgrade. Mirrors Python's `vgi_rpc.rpc.ProtocolVersionError`. */
export class ProtocolVersionError extends VersionError {
  static readonly errorKind = ERROR_KIND_PROTOCOL_VERSION_MISMATCH;
  readonly errorKind = ERROR_KIND_PROTOCOL_VERSION_MISMATCH;
  constructor(message: string) {
    super(message);
    this.name = "ProtocolVersionError";
  }
}

const SEMVER_REGEX = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/;

/** Parse a canonical semver string into `[major, minor, patch]`. Throws on
 *  any input that isn't `MAJOR.MINOR.PATCH` with non-negative integers and
 *  no leading zeros (except literal `0`). No prereleases, no build metadata.
 *  Mirrors Python's `vgi_rpc.metadata.parse_version`. */
export function parseProtocolVersion(value: string): [number, number, number] {
  const m = SEMVER_REGEX.exec(value);
  if (!m) {
    throw new Error(
      `Invalid protocol version '${value}': expected canonical semver ` +
        "MAJOR.MINOR.PATCH with non-negative integers and no leading zeros " +
        "(no prereleases or build metadata).",
    );
  }
  return [Number(m[1]), Number(m[2]), Number(m[3])];
}

/** Raised when a client invokes a method the server does not implement.
 *
 *  Mirrors Python's `vgi_rpc.rpc.MethodNotImplementedError`. The static
 *  `errorKind` is hoisted onto the error batch metadata as
 *  `vgi_rpc.error_kind` so clients can branch on the typed marker without
 *  string-matching the message.
 */
export class MethodNotImplementedError extends Error {
  /** Typed `vgi_rpc.error_kind` marker for this error class. */
  static readonly errorKind = ERROR_KIND_METHOD_NOT_IMPLEMENTED;
  /** Typed `vgi_rpc.error_kind` marker hoisted onto the error batch metadata. */
  readonly errorKind = ERROR_KIND_METHOD_NOT_IMPLEMENTED;
  constructor(message: string) {
    super(message);
    this.name = "MethodNotImplementedError";
  }
}

/** Raised when a sticky session token is malformed, expired, evicted, or
 *  bound to a different worker / principal. HTTP-only. */
export class SessionLostError extends Error {
  /** Typed `vgi_rpc.error_kind` marker for this error class. */
  static readonly errorKind = ERROR_KIND_SESSION_LOST;
  /** Typed `vgi_rpc.error_kind` marker hoisted onto the error batch metadata. */
  readonly errorKind = ERROR_KIND_SESSION_LOST;
  constructor(message: string) {
    super(message);
    this.name = "SessionLostError";
  }
}

/** Raised when `ctx.openSession` is called while the server is draining. */
export class ServerDrainingError extends Error {
  /** Typed `vgi_rpc.error_kind` marker for this error class. */
  static readonly errorKind = ERROR_KIND_SERVER_DRAINING;
  /** Typed `vgi_rpc.error_kind` marker hoisted onto the error batch metadata. */
  readonly errorKind = ERROR_KIND_SERVER_DRAINING;
  constructor(message: string) {
    super(message);
    this.name = "ServerDrainingError";
  }
}
