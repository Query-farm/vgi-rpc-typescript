// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import {
  type BadRequest,
  type ErrorCode,
  type ErrorDetail,
  type ErrorDetailJson,
  type ErrorInfo,
  type Help,
  isRetryable,
  type LocalizedMessage,
  type PreconditionFailure,
  parseErrorCode,
  parseErrorDetail,
  preconditionFailure,
  type QuotaFailure,
  type ResourceInfo,
  type RetryInfo,
  retryInfo,
} from "./error-model.js";

/** The error-model fields an {@link RpcError} carries (WIRE_PROTOCOL.md §8). */
export interface RpcErrorModel {
  /** `vgi_rpc.error_code`, or `""` when the server sent none. */
  errorCode?: string;
  /** `vgi_rpc.error_kind`, or `""` when absent. */
  errorKind?: string;
  /** The decoded `vgi_rpc.error_details` objects, unknown types included. */
  errorDetails?: readonly ErrorDetailJson[];
  /** `vgi_rpc.request_id`, when the server echoed one. */
  requestId?: string;
}

/**
 * A remote error, as the client decoded it -- also thrown server-side for
 * framework-level protocol errors.
 *
 * Carries the three layers of the error model:
 *
 * - {@link errorCode} -- the canonical code's name (`"UNAVAILABLE"`), or `""`
 *   when the server sent none (a server older than the model). {@link code}
 *   reads it as an {@link ErrorCode}.
 * - {@link errorKind} -- the reason a client branches on, or `""`.
 * - {@link errorDetails} -- the detail objects as received, unknown types
 *   included. The typed accessors ({@link retryInfo}, ...) return the catalog
 *   entry of that type and ignore the rest.
 *
 * {@link isRetryable} classifies; nothing in this package retries an RPC error
 * automatically, because a method may not be idempotent.
 */
export class RpcError extends Error {
  /** `vgi_rpc.error_code` as received, or `""` when absent. */
  readonly errorCode: string;
  /** `vgi_rpc.error_kind` as received, or `""` when absent. */
  readonly errorKind: string;
  /** `vgi_rpc.error_details` as received: every object, in wire order. */
  readonly errorDetails: readonly ErrorDetailJson[];
  /** `vgi_rpc.request_id`, or `""`. */
  readonly requestId: string;

  constructor(
    /** Remote error class name (e.g. `"ValueError"`). */
    public readonly errorType: string,
    /** Human-readable message from the remote error. */
    public readonly errorMessage: string,
    /** Remote stack-trace text, or an empty string when unavailable -- which is
     *  when the server turned tracebacks off. */
    public readonly remoteTraceback: string,
    model: RpcErrorModel = {},
  ) {
    super(`${errorType}: ${errorMessage}`);
    this.name = "RpcError";
    this.errorCode = model.errorCode ?? "";
    this.errorKind = model.errorKind ?? "";
    this.errorDetails = [...(model.errorDetails ?? [])];
    this.requestId = model.requestId ?? "";
  }

  /** The canonical code; `UNKNOWN` when absent or unrecognised. */
  get code(): ErrorCode {
    return parseErrorCode(this.errorCode);
  }

  /** Whether retrying this call is warranted (WIRE_PROTOCOL.md §8): `UNAVAILABLE`
   *  always, `RESOURCE_EXHAUSTED` only with `RetryInfo`. When {@link retryInfo}
   *  is present a retry waits at least that long. */
  isRetryable(): boolean {
    return isRetryable(this.code, this.errorDetails);
  }

  /** The details this client understands, in wire order; unknown types skipped. */
  details(): ErrorDetail[] {
    const out: ErrorDetail[] = [];
    for (const obj of this.errorDetails) {
      const parsed = parseErrorDetail(obj);
      if (parsed) out.push(parsed);
    }
    return out;
  }

  private detail<T extends ErrorDetail>(type: T["@type"]): T | null {
    return (this.details().find((d) => d["@type"] === type) as T | undefined) ?? null;
  }

  /** The `vgi_rpc.ErrorInfo` detail, if present. */
  errorInfo(): ErrorInfo | null {
    return this.detail<ErrorInfo>("vgi_rpc.ErrorInfo");
  }
  /** The `vgi_rpc.RetryInfo` detail, if present. */
  retryInfo(): RetryInfo | null {
    return this.detail<RetryInfo>("vgi_rpc.RetryInfo");
  }
  /** The `vgi_rpc.BadRequest` detail, if present. */
  badRequest(): BadRequest | null {
    return this.detail<BadRequest>("vgi_rpc.BadRequest");
  }
  /** The `vgi_rpc.PreconditionFailure` detail, if present. */
  preconditionFailure(): PreconditionFailure | null {
    return this.detail<PreconditionFailure>("vgi_rpc.PreconditionFailure");
  }
  /** The `vgi_rpc.QuotaFailure` detail, if present. */
  quotaFailure(): QuotaFailure | null {
    return this.detail<QuotaFailure>("vgi_rpc.QuotaFailure");
  }
  /** The `vgi_rpc.ResourceInfo` detail, if present. */
  resourceInfo(): ResourceInfo | null {
    return this.detail<ResourceInfo>("vgi_rpc.ResourceInfo");
  }
  /** The `vgi_rpc.Help` detail, if present. */
  help(): Help | null {
    return this.detail<Help>("vgi_rpc.Help");
  }
  /** The `vgi_rpc.LocalizedMessage` detail, if present. */
  localizedMessage(): LocalizedMessage | null {
    return this.detail<LocalizedMessage>("vgi_rpc.LocalizedMessage");
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
  /** Typed `vgi_rpc.error_kind` marker hoisted onto the error batch metadata. */
  static readonly errorKind = ERROR_KIND_PROTOCOL_VERSION_MISMATCH;
  /** Typed `vgi_rpc.error_kind` marker hoisted onto the error batch metadata. */
  readonly errorKind = ERROR_KIND_PROTOCOL_VERSION_MISMATCH;
  /** Canonical code hoisted as `vgi_rpc.error_code` (WIRE_PROTOCOL.md §8). */
  static readonly errorCode: ErrorCode = "FAILED_PRECONDITION";
  /** Canonical code hoisted as `vgi_rpc.error_code` (WIRE_PROTOCOL.md §8). */
  readonly errorCode: ErrorCode = "FAILED_PRECONDITION";
  /** The protocol whose version gate refused the call. */
  readonly protocol: string;
  /** What the client declared, or `""` when it declared none. */
  readonly clientVersion: string;
  /** What the server's binding declares. */
  readonly serverVersion: string;

  constructor(message: string, gate: { protocol?: string; clientVersion?: string; serverVersion?: string } = {}) {
    super(message);
    this.name = "ProtocolVersionError";
    this.protocol = gate.protocol ?? "";
    this.clientVersion = gate.clientVersion ?? "";
    this.serverVersion = gate.serverVersion ?? "";
  }

  /** One `protocol_version` violation naming the gated protocol. With several
   *  bindings hosted, "Server: 2.0.0" alone does not say which server. */
  get errorDetails(): PreconditionFailure[] {
    if (!this.protocol) return [];
    return [
      preconditionFailure([
        {
          type: "protocol_version",
          subject: this.protocol,
          description:
            `client declares ${this.clientVersion || "<none>"}, server requires ${this.serverVersion}; ` +
            "major and minor must match",
        },
      ]),
    ];
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
  /** Canonical code hoisted as `vgi_rpc.error_code` (WIRE_PROTOCOL.md §8). */
  static readonly errorCode: ErrorCode = "UNIMPLEMENTED";
  /** Canonical code hoisted as `vgi_rpc.error_code` (WIRE_PROTOCOL.md §8). */
  readonly errorCode: ErrorCode = "UNIMPLEMENTED";
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
  /** Retry the whole session, not the call. */
  static readonly errorCode: ErrorCode = "ABORTED";
  /** Canonical code hoisted as `vgi_rpc.error_code` (WIRE_PROTOCOL.md §8). */
  readonly errorCode: ErrorCode = "ABORTED";
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
  /** Canonical code hoisted as `vgi_rpc.error_code` (WIRE_PROTOCOL.md §8). */
  static readonly errorCode: ErrorCode = "UNAVAILABLE";
  /** Canonical code hoisted as `vgi_rpc.error_code` (WIRE_PROTOCOL.md §8). */
  readonly errorCode: ErrorCode = "UNAVAILABLE";
  /** Seconds before a retry -- which a load balancer will usually route to a
   *  worker that is not draining. */
  readonly retryAfter: number;
  constructor(message: string, retryAfter = 1) {
    super(message);
    this.name = "ServerDrainingError";
    this.retryAfter = retryAfter;
  }

  /** The retry hint. */
  get errorDetails(): RetryInfo[] {
    return [retryInfo(this.retryAfter)];
  }
}

/** A response would exceed the server's response-size cap.
 *
 *  `RESOURCE_EXHAUSTED` *without* `RetryInfo`: not retryable, because the same
 *  call against the same limits fails again. */
export class ResponseTooLargeError extends Error {
  /** Canonical code hoisted as `vgi_rpc.error_code` (WIRE_PROTOCOL.md §8). */
  static readonly errorCode: ErrorCode = "RESOURCE_EXHAUSTED";
  /** Canonical code hoisted as `vgi_rpc.error_code` (WIRE_PROTOCOL.md §8). */
  readonly errorCode: ErrorCode = "RESOURCE_EXHAUSTED";
  constructor(message: string) {
    super(message);
    this.name = "ResponseTooLargeError";
  }
}
