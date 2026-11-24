// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Protocol version negotiation, v1 (vgi-rpc-python
 * `tools/cross-port/specs/PROTOCOL_VERSIONS_V1.md`; port of
 * `vgi_rpc/protocol_versions.py`).
 *
 * Every server advertises, per protocol it gates, the inclusive window of
 * `major.minor` versions it accepts:
 *
 *     VGI-Protocol-Versions: vgi.v3=3.4-3.4, vgi.secret.v1=1.0-1.0
 *
 * on every HTTP response, and as the `vgi_rpc.protocol_versions` metadata value
 * on reflection results and `protocol_version_unsupported` refusals on every
 * transport. A client keeps an {@link EndpointNegotiation} per endpoint, learns
 * the server's windows from whatever it sees first, and speaks the highest
 * version both sides support.
 *
 * This module is pure: parsing, formatting and the negotiation state.
 *
 * @module
 */

import { RpcError } from "./errors.js";

/** HTTP response header carrying the server's windows. On every response. */
export const PROTOCOL_VERSIONS_HEADER = "VGI-Protocol-Versions";
/** HTTP request header carrying the client's own windows. Informational. */
export const CLIENT_PROTOCOL_VERSIONS_HEADER = "VGI-Client-Protocol-Versions";
/** Metadata key carrying the server's windows (reflection results, refusals). */
export const PROTOCOL_VERSIONS_KEY = "vgi_rpc.protocol_versions";
/** Request metadata key carrying the client's own windows. Informational. */
export const CLIENT_PROTOCOL_VERSIONS_KEY = "vgi_rpc.protocol_versions_client";
/** The `error_kind` of a refusal. */
export const PROTOCOL_VERSION_UNSUPPORTED = "protocol_version_unsupported";
/** The pre-negotiation refusal kind, still recognised from older servers. */
export const PROTOCOL_VERSION_MISMATCH = "protocol_version_mismatch";

const MAJOR_MINOR = /^(0|[1-9]\d*)\.(0|[1-9]\d*)$/;
const SEMVER = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/;
/** The routing-key alphabet a protocol name may use on the left of `=`. */
const NAME = /^[A-Za-z0-9_.:/-]+$/;

/** A `(major, minor)` pair. */
export type MajorMinor = readonly [number, number];

function compare(a: MajorMinor, b: MajorMinor): number {
  return a[0] !== b[0] ? a[0] - b[0] : a[1] - b[1];
}

/** Parse `MAJOR.MINOR`; a full `MAJOR.MINOR.PATCH` is accepted and its patch dropped.
 *  @throws Error for anything else. */
export function parseMajorMinor(text: string): MajorMinor {
  const t = text.trim();
  const m = MAJOR_MINOR.exec(t) ?? SEMVER.exec(t);
  if (!m) throw new Error(`not a MAJOR.MINOR version: ${JSON.stringify(text)}`);
  return [Number(m[1]), Number(m[2])];
}

function mm(v: MajorMinor): string {
  return `${v[0]}.${v[1]}`;
}

/** An inclusive `min..max` window of `(major, minor)` versions. */
export class VersionRange {
  constructor(
    /** The oldest version accepted. */
    readonly min: MajorMinor,
    /** The newest version accepted. */
    readonly max: MajorMinor,
  ) {
    if (compare(min, max) > 0) throw new Error(`protocol version range ${mm(min)}-${mm(max)} has min above max`);
  }

  /** Parse `MIN-MAX` (each `MAJOR.MINOR`), or a single `MAJOR.MINOR` meaning `V-V`.
   *  @throws Error when the text is not a range. */
  static parse(text: string): VersionRange {
    const t = text.trim();
    const dash = t.indexOf("-");
    const low = parseMajorMinor(dash < 0 ? t : t.slice(0, dash));
    const high = dash < 0 ? low : parseMajorMinor(t.slice(dash + 1));
    return new VersionRange(low, high);
  }

  /** The single-version window of a semver's `major.minor`. */
  static exactly(version: string): VersionRange {
    const v = parseMajorMinor(version);
    return new VersionRange(v, v);
  }

  /** Whether `version` (`[major, minor, ...]`) lies inside the window. */
  contains(version: readonly number[]): boolean {
    if (version.length < 2) return false;
    const v: MajorMinor = [version[0]!, version[1]!];
    return compare(this.min, v) <= 0 && compare(v, this.max) <= 0;
  }

  /** Wire form: `MIN-MAX`, always both ends. */
  toString(): string {
    return `${mm(this.min)}-${mm(this.max)}`;
  }
}

/** Render `name=MIN-MAX[, name=MIN-MAX]...` in the given order. */
export function formatVersions(ranges: Iterable<readonly [string, VersionRange]>): string {
  return [...ranges].map(([name, rng]) => `${name}=${rng}`).join(", ");
}

/**
 * Parse a `VGI-Protocol-Versions` value. Tolerant: an entry that does not parse
 * is skipped (it reads as "that protocol is unadvertised"), and a name that
 * repeats keeps its first window.
 */
export function parseVersions(text: string | null | undefined): Map<string, VersionRange> {
  const out = new Map<string, VersionRange>();
  if (text === null || text === undefined) return out;
  for (const entry of text.split(",")) {
    const eq = entry.indexOf("=");
    if (eq < 0) continue;
    const name = entry.slice(0, eq).trim();
    if (!NAME.test(name) || out.has(name)) continue;
    try {
      out.set(name, VersionRange.parse(entry.slice(eq + 1)));
    } catch {
      // skipped: a later syntax extension degrades to "unadvertised"
    }
  }
  return out;
}

/** The highest `(major, minor)` inside both windows, or `null` when they are disjoint. */
export function highestCommon(a: VersionRange, b: VersionRange): MajorMinor | null {
  const top = compare(a.max, b.max) <= 0 ? a.max : b.max;
  if (compare(top, a.min) < 0 || compare(top, b.min) < 0) return null;
  return top;
}

/** What a client bound to one protocol speaks. */
export class ClientVersioning {
  /** The client's patch level, sent with whatever major.minor is negotiated. */
  readonly patch: number;

  constructor(
    /** The protocol's wire name. */
    readonly protocol: string,
    /** The newest version the client speaks (`MAJOR.MINOR.PATCH`). */
    readonly version: string,
    /** The window the client supports; its max is `version`'s major.minor. */
    readonly range: VersionRange,
  ) {
    const m = SEMVER.exec(version);
    if (!m) throw new Error(`protocol version ${JSON.stringify(version)} is not canonical MAJOR.MINOR.PATCH`);
    this.patch = Number(m[3]);
    if (compare(range.max, [Number(m[1]), Number(m[2])]) !== 0) {
      throw new Error(
        `protocol version range ${range} must end at protocol version ${version}'s major.minor ` +
          `(${m[1]}.${m[2]}): the version is the newest spoken, and the range widens it downward only`,
      );
    }
  }

  /** Build from a declared version and optional range (`MIN-MAX`); `null` when no version is declared. */
  static of(protocol: string, version: string, range?: string | null): ClientVersioning | null {
    if (!version) return null;
    return new ClientVersioning(protocol, version, range ? VersionRange.parse(range) : VersionRange.exactly(version));
  }

  /** The `VGI-Client-Protocol-Versions` / `vgi_rpc.protocol_versions_client` value. */
  advertised(): string {
    return formatVersions([[this.protocol, this.range]]);
  }

  /** `MAJOR.MINOR.PATCH` to send for a negotiated major.minor: the client's patch rides along. */
  wireVersion(version: MajorMinor): string {
    return `${version[0]}.${version[1]}.${this.patch}`;
  }
}

/**
 * The client and server windows for a protocol do not overlap: raised
 * client-side, before sending. Carries the `protocol_version_unsupported` kind
 * a server refusal does, so a caller handles both in one place.
 */
export class NoCommonVersionError extends RpcError {
  constructor(
    /** The protocol. */
    readonly protocol: string,
    /** What the client supports. */
    readonly clientRange: VersionRange,
    /** What the server advertised. */
    readonly serverRange: VersionRange,
    endpoint = "",
  ) {
    const where = endpoint ? ` at ${endpoint}` : "";
    const advice =
      compare(serverRange.max, clientRange.min) < 0
        ? `The server is too old (this client is too new for it): upgrade the worker to one supporting ` +
          `${protocol} ${mm(clientRange.min)} or newer, or use a client that supports ${protocol} ${mm(serverRange.max)}.`
        : `The client is too old (the server is too new for it): upgrade the client to one supporting ` +
          `${protocol} ${mm(serverRange.min)} or newer, or use a worker that supports ${protocol} ${mm(clientRange.max)}.`;
    const message =
      `${PROTOCOL_VERSION_UNSUPPORTED}: no common ${protocol} version. This client supports ${protocol} ` +
      `${clientRange}; the server${where} supports ${protocol} ${serverRange}. ${advice}`;
    super("ProtocolVersionError", message, "", {
      errorKind: PROTOCOL_VERSION_UNSUPPORTED,
      errorCode: "FAILED_PRECONDITION",
    });
    this.name = "NoCommonVersionError";
  }
}

/** Version texts a pre-spec server's refusal carries ("Server: 3.4.0", "server requires 3.4.0"). */
const LEGACY_SERVER_VERSION = /(?:Server:\s*|server requires\s+)(\d+\.\d+)(?:\.\d+)?/;

/** Whether `err` is a server's version refusal (this spec's kind, or the pre-spec one). */
export function isVersionRefusal(err: unknown): err is RpcError {
  const kind = (err as { errorKind?: unknown } | null)?.errorKind;
  return kind === PROTOCOL_VERSION_UNSUPPORTED || kind === PROTOCOL_VERSION_MISMATCH;
}

/** The server windows a refusal carries in its `vgi_rpc.ErrorInfo` detail. */
export function rangesFromError(err: RpcError): Map<string, VersionRange> {
  for (const d of err.errorDetails ?? []) {
    const detail = d as { "@type"?: string; metadata?: Record<string, unknown> };
    if (detail["@type"] === "vgi_rpc.ErrorInfo") {
      const value = detail.metadata?.[PROTOCOL_VERSIONS_KEY];
      if (typeof value === "string") return parseVersions(value);
    }
  }
  return new Map();
}

/** The single server version a pre-spec refusal names, when one is present. */
export function legacyServerVersion(err: RpcError): MajorMinor | null {
  if (!isVersionRefusal(err)) return null;
  const texts = [err.errorMessage ?? err.message];
  for (const d of err.errorDetails ?? []) {
    const violations = (d as { violations?: { description?: unknown }[] }).violations;
    for (const v of violations ?? []) if (typeof v.description === "string") texts.push(v.description);
  }
  for (const text of texts) {
    const m = LEGACY_SERVER_VERSION.exec(text);
    if (m) return parseMajorMinor(m[1]!);
  }
  return null;
}

/**
 * Negotiation state for one endpoint (one worker location / connection).
 *
 * The decision, per protocol (spec §6): the server advertised a window → the
 * highest common version, or {@link NoCommonVersionError}; a pre-spec refusal
 * named one server version → that one; otherwise the client's own newest.
 */
export class EndpointNegotiation {
  private advertisedRanges = new Map<string, VersionRange>();
  private legacy = new Map<string, MajorMinor>();
  private seen = false;

  constructor(
    /** Where the server is; used only in error messages. */
    readonly endpoint = "",
  ) {}

  /** A copy of the windows learned so far. */
  get advertised(): Map<string, VersionRange> {
    return new Map(this.advertisedRanges);
  }

  /** Whether any response so far carried the advertisement. */
  get hasAdvertisement(): boolean {
    return this.seen;
  }

  /** Record an advertisement read from a response; `null` (absent) is ignored.
   *  A present value replaces what was learned (a redeploy behind one URL). */
  observe(value: string | null | undefined): void {
    if (value === null || value === undefined) return;
    this.advertisedRanges = parseVersions(value);
    this.seen = true;
    this.legacy.clear();
  }

  /** Learn from a `protocol_version_unsupported` (or pre-spec mismatch) refusal. */
  observeRefusal(protocol: string, err: RpcError): void {
    const ranges = rangesFromError(err);
    if (ranges.size > 0) {
      this.advertisedRanges = ranges;
      this.seen = true;
      this.legacy.clear();
      return;
    }
    const legacy = legacyServerVersion(err);
    if (legacy !== null && !this.seen) this.legacy.set(protocol, legacy);
  }

  /** The `(major, minor)` to speak for `client`'s protocol.
   *  @throws NoCommonVersionError when the windows do not overlap. */
  choose(client: ClientVersioning): MajorMinor {
    const server = this.advertisedRanges.get(client.protocol);
    if (server !== undefined) {
      const common = highestCommon(client.range, server);
      if (common === null) throw new NoCommonVersionError(client.protocol, client.range, server, this.endpoint);
      return common;
    }
    const legacy = this.legacy.get(client.protocol);
    if (legacy !== undefined) {
      if (!client.range.contains(legacy)) {
        throw new NoCommonVersionError(client.protocol, client.range, new VersionRange(legacy, legacy), this.endpoint);
      }
      return legacy;
    }
    return client.range.max;
  }

  /** The `vgi_rpc.protocol_version` value to send for `client`'s protocol. */
  wireVersion(client: ClientVersioning): string {
    return client.wireVersion(this.choose(client));
  }
}

/** The reflection protocol: describe / list_protocols, idempotent by definition. */
const REFLECTION = "vgi_rpc.Reflection.v1";

/** Whether a refused call may be re-sent once at a renegotiated version (§6):
 *  reflection, and methods declaring `no_side_effects` or `idempotent`. */
export function isRetrySafe(protocolName: string, idempotency = ""): boolean {
  return protocolName === REFLECTION || idempotency === "no_side_effects" || idempotency === "idempotent";
}
