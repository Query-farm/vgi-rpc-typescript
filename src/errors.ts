// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/** Error thrown when the server encounters an RPC protocol error. */
export class RpcError extends Error {
  constructor(
    public readonly errorType: string,
    public readonly errorMessage: string,
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

/** Well-known values for the `vgi_rpc.error_kind` batch metadata key. Mirrors
 *  Python's `vgi_rpc.metadata.ERROR_KIND_*` constants. */
export const ERROR_KIND_METHOD_NOT_IMPLEMENTED = "method_not_implemented";
export const ERROR_KIND_SESSION_LOST = "session_lost";
export const ERROR_KIND_SERVER_DRAINING = "server_draining";

/** Raised when a client invokes a method the server does not implement.
 *
 *  Mirrors Python's `vgi_rpc.rpc.MethodNotImplementedError`. The static
 *  `errorKind` is hoisted onto the error batch metadata as
 *  `vgi_rpc.error_kind` so clients can branch on the typed marker without
 *  string-matching the message.
 */
export class MethodNotImplementedError extends Error {
  static readonly errorKind = ERROR_KIND_METHOD_NOT_IMPLEMENTED;
  readonly errorKind = ERROR_KIND_METHOD_NOT_IMPLEMENTED;
  constructor(message: string) {
    super(message);
    this.name = "MethodNotImplementedError";
  }
}

/** Raised when a sticky session token is malformed, expired, evicted, or
 *  bound to a different worker / principal. HTTP-only. */
export class SessionLostError extends Error {
  static readonly errorKind = ERROR_KIND_SESSION_LOST;
  readonly errorKind = ERROR_KIND_SESSION_LOST;
  constructor(message: string) {
    super(message);
    this.name = "SessionLostError";
  }
}

/** Raised when `ctx.openSession` is called while the server is draining. */
export class ServerDrainingError extends Error {
  static readonly errorKind = ERROR_KIND_SERVER_DRAINING;
  readonly errorKind = ERROR_KIND_SERVER_DRAINING;
  constructor(message: string) {
    super(message);
    this.name = "ServerDrainingError";
  }
}
