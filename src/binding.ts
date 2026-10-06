// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// One protocol hosted by a server, with everything dispatch needs.
//
// A server hosts one or more protocols and resolves the pair
// `(protocol, method)`. Method names may collide across protocols -- that is
// what makes protocols independently authorable, and a port that merges them
// into one namespace is not conformant.

import { ProtocolVersionError, parseProtocolVersion } from "./errors.js";
import type { Protocol } from "./protocol.js";

/** The name grammar: an identifier, optionally dot-qualified, carrying its
 *  major version as the last component (`vgi_rpc.Reflection.v1`).
 *
 *  Validated on both carriers -- at registration, and again on the routing key
 *  read off the wire. An unvalidated name from a request reaches error
 *  messages, log fields and metric labels, where arbitrary bytes do not
 *  belong. */
const PROTOCOL_NAME_RE = /^[A-Za-z_][A-Za-z0-9_.]*$/;

/** Reserved for protocols the framework itself defines. An application claiming
 *  `vgi_rpc.Reflection.v1` would shadow the one surface a client can trust
 *  before it knows anything else about the server. */
export const RESERVED_PROTOCOL_PREFIX = "vgi_rpc.";

/** Bounds a name that crosses process boundaries both as metadata and as a URL
 *  path segment. */
export const MAX_PROTOCOL_NAME_BYTES = 255;

/** Throw unless `name` can be a protocol's wire identity.
 *
 *  `allowReserved` permits the `vgi_rpc.` prefix, and is set only where the
 *  framework registers its own protocols. */
export function validateProtocolName(name: string, allowReserved = false): void {
  if (!name) throw new Error("A protocol name may not be empty.");
  if (new TextEncoder().encode(name).length > MAX_PROTOCOL_NAME_BYTES) {
    throw new Error(`Protocol name exceeds ${MAX_PROTOCOL_NAME_BYTES} bytes: ${name.slice(0, 64)}...`);
  }
  if (!PROTOCOL_NAME_RE.test(name)) {
    throw new Error(
      `Protocol name '${name}' is not an identifier, optionally dot-qualified. ` +
        `Expected something like 'vgi.Identity.v1'.`,
    );
  }
  if (!allowReserved && name.startsWith(RESERVED_PROTOCOL_PREFIX)) {
    throw new Error(
      `Protocol name '${name}' claims the reserved '${RESERVED_PROTOCOL_PREFIX}' prefix, ` +
        `which is for protocols the framework defines.`,
    );
  }
}

/** One hosted protocol. */
export interface ProtocolBinding {
  /** Wire identity -- the routing key. */
  readonly name: string;
  readonly protocol: Protocol;
  /** Skip the version gate for this binding. Set for reflection, which is what
   *  a version-mismatched client calls to learn *what* mismatched -- gating it
   *  would deny the client the diagnosis it came for. */
  readonly versionExempt: boolean;
}

/** Anything that hosts one or more protocols and can enumerate them.
 *
 *  Structural rather than a class reference so the HTTP handler can accept a
 *  `VgiRpcServer` without importing it -- the handler is bundled for
 *  workerd, where the stdio server has no business being pulled in. */
export interface ProtocolHost {
  /** Every protocol this host serves, keyed by wire name, primary first. */
  bindings(): Map<string, ProtocolBinding>;
  /** Fix the hosted set: called when a transport starts serving the host. */
  seal?(): void;
  /** Whether the host's error batches carry the remote traceback. */
  readonly includeTracebacks?: boolean;
  /** The hosted `vgi_rpc.Identity.v1`, whose sealed grants and `resolveToken`
   *  an HTTP handler accepts as bearer credentials. Structural so this module
   *  imports neither the identity nor the grant code. */
  readonly identity?: {
    readonly grantKeys?: unknown;
    readonly resolveToken?: unknown;
  };
}

/** True when `target` enumerates protocols rather than being a bare one. */
export function isProtocolHost(target: unknown): target is ProtocolHost {
  return typeof (target as ProtocolHost | null)?.bindings === "function";
}

/** A request carrying no routing key.
 *
 *  Distinct from {@link ProtocolNotSupportedError} on purpose: the first says
 *  the caller did not say which protocol it meant, the second that it named one
 *  this server does not host, and a client acts differently on each.
 *
 *  Raised on the **raw** transports, where the metadata is the only carrier of
 *  the protocol and an absent field really is unroutable. HTTP does not raise
 *  it for an absent field -- the path segment has already resolved the binding
 *  there -- only for a protocol path segment containing a percent sign, via
 *  {@link ProtocolNotSpecifiedError.percentEncoded}. */
export class ProtocolNotSpecifiedError extends Error {
  readonly errorKind = "protocol_not_specified";
  readonly errorCode = "INVALID_ARGUMENT";
  constructor(hosted: readonly string[], message?: string) {
    super(
      message ??
        `Request carries no 'vgi_rpc.protocol' routing key. Every request must name ` +
          `the protocol it addresses. This server hosts: [${hosted.join(", ")}].`,
    );
    this.name = "ProtocolNotSpecifiedError";
  }

  /** A protocol path segment containing a percent sign.
   *
   *  Rejected without decoding: the name charset never requires
   *  percent-encoding, so a `%` is a bug or an attempt to have the edge and
   *  the worker read different strings. The segment itself is never echoed --
   *  it has not been validated, and an unvalidated request string does not
   *  belong in an error message, a log field or a metric label. */
  static percentEncoded(): ProtocolNotSpecifiedError {
    return new ProtocolNotSpecifiedError(
      [],
      "The protocol path segment contains a percent sign. The protocol name charset " +
        "never requires percent-encoding, so this is rejected rather than decoded.",
    );
  }
}

/** A protocol this server does not host.
 *
 *  Also the answer for an incompatible major version, since the major is part
 *  of the name: a routing answer every proxy, WAF and load balancer understands
 *  without an Arrow parser. */
export class ProtocolNotSupportedError extends Error {
  readonly errorKind = "protocol_not_supported";
  readonly errorCode = "UNIMPLEMENTED";
  constructor(message: string) {
    super(message);
    this.name = "ProtocolNotSupportedError";
  }

  static notHosted(requested: string, hosted: readonly string[]): ProtocolNotSupportedError {
    return new ProtocolNotSupportedError(
      `This server does not host protocol '${requested}'. Hosted: [${hosted.join(", ")}].`,
    );
  }
}

/**
 * Gate a client's declared `vgi_rpc.protocol_version` against the binding that
 * owns the resolved method. No-op for a binding that declares no version.
 *
 * Exact major+minor match, patch ignored; the message names which side is
 * older. Per binding, never against the primary: a server hosting several
 * protocols has a version per binding and no single "server version", so
 * gating a secondary against the primary rejects correct callers and names the
 * wrong protocol when it does. The thrown error carries a
 * `PreconditionFailure` naming the gated protocol. One implementation for
 * every transport, so the raw launchers cannot drift from stdio and HTTP.
 */
export function gateProtocolVersion(binding: ProtocolBinding, clientVersion: string | undefined): void {
  const serverParts = binding.protocol.protocolVersionParts;
  if (serverParts === null) return;
  const serverVersion = binding.protocol.protocolVersion;
  const gate = { protocol: binding.name, serverVersion };
  if (clientVersion === undefined) {
    throw new ProtocolVersionError(
      `VGI client/worker protocol_version mismatch for protocol '${binding.name}'.\n` +
        "  Client: <not declared>\n" +
        `  Server: ${serverVersion}\n` +
        "  Direction: the client did not send a vgi_rpc.protocol_version " +
        "metadata key. This is either a vgi-rpc framework bug or a " +
        "non-VGI client connecting to a VGI worker.",
      gate,
    );
  }
  let clientParts: readonly [number, number, number];
  try {
    clientParts = parseProtocolVersion(clientVersion);
  } catch {
    throw new ProtocolVersionError(
      `VGI client/worker protocol_version mismatch for protocol '${binding.name}'.\n` +
        `  Client: ${clientVersion}\n` +
        `  Server: ${serverVersion}\n` +
        "  Direction: client sent a malformed protocol_version. " +
        "Expected canonical semver MAJOR.MINOR.PATCH.",
      { ...gate, clientVersion },
    );
  }
  if (clientParts[0] === serverParts[0] && clientParts[1] === serverParts[1]) return;
  const clientOlder =
    clientParts[0] < serverParts[0] || (clientParts[0] === serverParts[0] && clientParts[1] < serverParts[1]);
  const direction = clientOlder
    ? `client is too old; upgrade the VGI extension/client to a version supporting protocol_version ${serverVersion}.`
    : `server is too old; upgrade the VGI worker to a version supporting protocol_version ${clientVersion}.`;
  throw new ProtocolVersionError(
    `VGI client/worker protocol_version mismatch for protocol '${binding.name}'.\n` +
      `  Client: ${clientVersion}\n` +
      `  Server: ${serverVersion}\n` +
      `  Direction: ${direction}`,
    { ...gate, clientVersion },
  );
}
