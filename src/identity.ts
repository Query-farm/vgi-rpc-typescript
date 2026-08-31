// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { AuthContext } from "./auth.js";
import { sha256Hex } from "./util/web-crypto.js";

/** Outcome reported by a peer identity provider. */
export const PeerIdentityStatus = {
  /** The provider is not configured. */
  OFF: "off",
  /** The provider does not apply to this transport or request. */
  NOT_APPLICABLE: "not_applicable",
  /** The provider produced peer identity evidence. */
  AVAILABLE: "available",
  /** The provider could not be reached or queried. */
  UNAVAILABLE: "unavailable",
  /** The provider lacks permission to inspect peer identity. */
  PERMISSION_DENIED: "permission_denied",
  /** The provider found no matching peer identity. */
  NO_MATCH: "no_match",
  /** The supplied peer evidence is invalid. */
  INVALID: "invalid",
  /** Peer evidence came through an untrusted proxy. */
  UNTRUSTED_PROXY: "untrusted_proxy",
} as const;
/** A peer identity provider outcome. */
export type PeerIdentityStatus = (typeof PeerIdentityStatus)[keyof typeof PeerIdentityStatus];
const PEER_IDENTITY_STATUSES = new Set<string>(Object.values(PeerIdentityStatus));

/** Mechanism that established a peer identity. */
export const IdentityAssurance = {
  /** Identity is bound directly to a cryptographic peer. */
  CRYPTOGRAPHIC_PEER: "cryptographic_peer",
  /** Identity was reported by a trusted local daemon. */
  LOCAL_DAEMON: "local_daemon",
  /** Identity was asserted by a configured trusted proxy. */
  CONFIGURED_PROXY: "configured_proxy",
} as const;
/** Assurance mechanism for peer identity evidence. */
export type IdentityAssurance = (typeof IdentityAssurance)[keyof typeof IdentityAssurance];
const IDENTITY_ASSURANCES = new Set<string>(Object.values(IdentityAssurance));

/** Category of subject identified by peer evidence. */
export const PeerSubjectKind = {
  /** A human user. */
  USER: "user",
  /** A tagged infrastructure node. */
  TAGGED_NODE: "tagged_node",
  /** A service workload. */
  WORKLOAD: "workload",
  /** A network endpoint. */
  ENDPOINT: "endpoint",
  /** A subject whose category is unknown. */
  UNKNOWN: "unknown",
} as const;
/** Category of a peer identity subject. */
export type PeerSubjectKind = (typeof PeerSubjectKind)[keyof typeof PeerSubjectKind];
const PEER_SUBJECT_KINDS = new Set<string>(Object.values(PeerSubjectKind));

/** Lifetime over which a peer subject key remains stable. */
export const SubjectStability = {
  /** Stable across logins and sessions. */
  STABLE: "stable",
  /** Stable only for the current login. */
  LOGIN: "login",
  /** No stable subject key is available. */
  NONE: "none",
} as const;
/** Stability class of a peer subject key. */
export type SubjectStability = (typeof SubjectStability)[keyof typeof SubjectStability];
const SUBJECT_STABILITIES = new Set<string>(Object.values(SubjectStability));

/** Immutable value representable in JSON. */
export type JsonValue = null | boolean | number | string | readonly JsonValue[] | { readonly [key: string]: JsonValue };
const MAX_JSON_BYTES = 65_536;
const MAX_JSON_DEPTH = 16;
const MAX_JSON_VALUES = 4_096;
const MAX_HEADER_COUNT = 128;
const MAX_HEADER_VALUES = 16;
const MAX_HEADER_BYTES = 65_536;
const HTTP_FIELD_NAME = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;

function assertWellFormedUtf16(value: string, path: string): void {
  for (let index = 0; index < value.length; index++) {
    const unit = value.charCodeAt(index);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) throw new TypeError(`${path} contains an unpaired surrogate`);
      index++;
    } else if (unit >= 0xdc00 && unit <= 0xdfff) {
      throw new TypeError(`${path} contains an unpaired surrogate`);
    }
  }
}

function snapshotJson(value: unknown, path = "evidence", depth = 0, limits = { values: 0, sourceBytes: 0 }): JsonValue {
  if (depth > MAX_JSON_DEPTH) throw new TypeError(`${path} exceeds maximum JSON depth`);
  limits.values++;
  if (limits.values > MAX_JSON_VALUES) throw new TypeError(`${path} exceeds maximum JSON value count`);
  if (value === null || typeof value === "boolean") return value;
  if (typeof value === "string") {
    assertWellFormedUtf16(value, path);
    if (value.length > MAX_JSON_BYTES) throw new TypeError(`${path} exceeds maximum JSON byte size`);
    limits.sourceBytes += new TextEncoder().encode(value).length;
    if (limits.sourceBytes > MAX_JSON_BYTES) throw new TypeError(`${path} exceeds maximum JSON byte size`);
    return value;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError(`${path} numbers must be finite`);
    return value;
  }
  if (Array.isArray(value)) {
    return Object.freeze(value.map((item, index) => snapshotJson(item, `${path}[${index}]`, depth + 1, limits)));
  }
  if (typeof value === "object" && Object.getPrototypeOf(value) === Object.prototype) {
    const out: Record<string, JsonValue> = {};
    for (const [key, item] of Object.entries(value as Record<string, unknown>)) {
      assertWellFormedUtf16(key, `${path} key`);
      limits.sourceBytes += new TextEncoder().encode(key).length;
      if (limits.sourceBytes > MAX_JSON_BYTES) throw new TypeError(`${path} exceeds maximum JSON byte size`);
      if (item === undefined) throw new TypeError(`${path}.${key} is not JSON-compatible`);
      out[key] = snapshotJson(item, `${path}.${key}`, depth + 1, limits);
    }
    return Object.freeze(out);
  }
  throw new TypeError(`${path} is not JSON-compatible`);
}

function snapshotObject(
  value: Readonly<Record<string, unknown>> | undefined,
  path: string,
): Readonly<Record<string, JsonValue>> {
  const snapshot = snapshotJson(value ?? {}, path) as Readonly<Record<string, JsonValue>>;
  if (new TextEncoder().encode(canonicalJson(snapshot)).length > MAX_JSON_BYTES) {
    throw new TypeError(`${path} exceeds maximum JSON byte size`);
  }
  return snapshot;
}

function canonicalJson(value: JsonValue): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  const object = value as Readonly<Record<string, JsonValue>>;
  return `{${Object.keys(object)
    .sort(compareUnicode)
    .map((key) => `${JSON.stringify(key)}:${canonicalJson(object[key])}`)
    .join(",")}}`;
}

function containsControl(value: string): boolean {
  for (const character of value) {
    const code = character.codePointAt(0) as number;
    if (code <= 0x1f || code === 0x7f) return true;
  }
  return false;
}

/** Transport details supplied to peer identity providers. */
export interface PeerResolutionOptions {
  /** Directly connected peer address or identifier. */
  immediatePeer?: string;
  /** Transport-specific source endpoint. */
  sourceEndpoint?: string;
  /** Peer identity asserted by the transport. */
  assertedPeer?: string;
  /** Local destination address that accepted the connection. */
  destinationAddress?: string;
  /** Request authority or host. */
  authority?: string;
  /** Logical destination service name. */
  serviceName?: string;
  /** Request headers with their original multiplicity preserved. */
  headers?: ReadonlyMap<string, readonly string[]> | Readonly<Record<string, readonly string[]>>;
  /** Additional JSON-compatible transport metadata. */
  metadata?: Readonly<Record<string, unknown>>;
  /** Absolute Unix epoch milliseconds. */
  deadline?: number;
  /** Monotonic total provider budget in milliseconds. */
  budgetMs?: number;
}

/** Immutable provider-neutral transport request snapshot. */
export class PeerResolutionContext {
  /** Transport protocol or adapter name. */
  readonly transport: string;
  /** Directly connected peer address or identifier. */
  readonly immediatePeer?: string;
  /** Transport-specific source endpoint. */
  readonly sourceEndpoint?: string;
  /** Peer identity asserted by the transport. */
  readonly assertedPeer?: string;
  /** Local destination address that accepted the connection. */
  readonly destinationAddress?: string;
  /** Request authority or host. */
  readonly authority?: string;
  /** Logical destination service name. */
  readonly serviceName?: string;
  /** Immutable JSON-compatible transport metadata. */
  readonly metadata: Readonly<Record<string, JsonValue>>;
  /** Absolute Unix epoch deadline in milliseconds. */
  readonly deadline?: number;
  /** Total monotonic provider budget in milliseconds. */
  readonly budgetMs?: number;
  readonly #startedAt: number;
  readonly #headers: ReadonlyMap<string, readonly string[]>;

  constructor(transport: string, options: PeerResolutionOptions = {}) {
    if (!transport) throw new TypeError("peer transport must not be empty");
    assertWellFormedUtf16(transport, "peer transport");
    this.transport = transport;
    for (const [name, value] of Object.entries({
      immediatePeer: options.immediatePeer,
      sourceEndpoint: options.sourceEndpoint,
      assertedPeer: options.assertedPeer,
      destinationAddress: options.destinationAddress,
      authority: options.authority,
      serviceName: options.serviceName,
    })) {
      if (value !== undefined) assertWellFormedUtf16(value, name);
    }
    this.immediatePeer = options.immediatePeer;
    this.sourceEndpoint = options.sourceEndpoint;
    this.assertedPeer = options.assertedPeer;
    this.destinationAddress = options.destinationAddress;
    this.authority = options.authority;
    this.serviceName = options.serviceName;
    if (options.deadline !== undefined && (!Number.isFinite(options.deadline) || options.deadline <= 0)) {
      throw new TypeError("peer deadline must be a positive epoch millisecond value");
    }
    this.deadline = options.deadline;
    if (options.budgetMs !== undefined && (!Number.isFinite(options.budgetMs) || options.budgetMs <= 0)) {
      throw new TypeError("peer budgetMs must be positive");
    }
    this.budgetMs = options.budgetMs;
    this.#startedAt = performance.now();
    this.metadata = snapshotObject(options.metadata, "peer metadata");
    const headers = new Map<string, readonly string[]>();
    const entries = options.headers instanceof Map ? options.headers.entries() : Object.entries(options.headers ?? {});
    let headerBytes = 0;
    for (const [name, rawValues] of entries) {
      if (headers.size >= MAX_HEADER_COUNT) throw new PeerIdentityRejectedError("too many peer identity headers");
      assertWellFormedUtf16(name, "peer-resolution header name");
      if (!HTTP_FIELD_NAME.test(name)) throw new TypeError("invalid peer-resolution header name");
      const key = name.toLowerCase();
      if (headers.has(key)) throw new PeerIdentityRejectedError("case-varied duplicate peer identity header");
      if (!Array.isArray(rawValues)) {
        throw new PeerIdentityRejectedError(
          `peer identity header ${JSON.stringify(name)} did not preserve multiplicity`,
        );
      }
      const values = Object.freeze([...rawValues]);
      if (values.length > MAX_HEADER_VALUES) {
        throw new PeerIdentityRejectedError(`too many values for peer identity header: ${name}`);
      }
      values.forEach((value) => {
        assertWellFormedUtf16(value, `peer-resolution header value: ${name}`);
      });
      if (values.some((value) => typeof value !== "string" || containsControl(value))) {
        throw new TypeError(`invalid peer-resolution header value: ${name}`);
      }
      headerBytes += new TextEncoder().encode(name).length;
      for (const value of values) headerBytes += new TextEncoder().encode(value).length;
      if (headerBytes > MAX_HEADER_BYTES) throw new PeerIdentityRejectedError("peer identity headers are too large");
      headers.set(key, values);
    }
    this.#headers = headers;
    Object.freeze(this);
  }

  /** Returns the single value for a case-insensitive header name. */
  header(name: string): string | undefined {
    assertWellFormedUtf16(name, "peer-resolution header lookup");
    const values = this.#headers.get(name.toLowerCase()) ?? [];
    if (values.length > 1) throw new PeerIdentityRejectedError(`duplicate peer identity header: ${name}`);
    return values[0];
  }

  /** Remaining provider budget measured with the runtime's monotonic clock. */
  remainingBudgetMs(): number | undefined {
    return this.budgetMs === undefined ? undefined : Math.max(0, this.budgetMs - (performance.now() - this.#startedAt));
  }
}

/** Fields used to construct immutable peer identity evidence. */
export interface PeerIdentityOptions {
  /** Provider that produced the evidence. */
  provider: string;
  /** Concrete source from which the evidence was obtained. */
  evidenceSource: string;
  /** Mechanism that established the identity. */
  assurance: IdentityAssurance;
  /** Namespace that issued the subject key. */
  issuer: string;
  /** Transport on which the peer was observed. */
  transport: string;
  /** Category of the identified subject. */
  subjectKind?: PeerSubjectKind;
  /** Provider-specific subject identifier. */
  subjectKey?: string;
  /** Lifetime over which the subject key remains stable. */
  subjectStability?: SubjectStability;
  /** Whether the provider verified the subject key. */
  subjectVerified?: boolean;
  /** Additional JSON-compatible facts about the peer. */
  attributes?: Readonly<Record<string, unknown>>;
  /** JSON-compatible permissions or features reported for the peer. */
  capabilities?: Readonly<Record<string, unknown>>;
  /** Whether the provider verified the reported capabilities. */
  capabilitiesVerified?: boolean;
  /** Original source address observed for the peer. */
  sourceAddress?: string;
  /** Address of the trusted proxy that supplied the evidence. */
  proxyAddress?: string;
}

/** Immutable verified or observed transport-peer evidence. */
export class PeerIdentity {
  /** Provider that produced the evidence. */
  readonly provider: string;
  /** Concrete source from which the evidence was obtained. */
  readonly evidenceSource: string;
  /** Mechanism that established the identity. */
  readonly assurance: IdentityAssurance;
  /** Namespace that issued the subject key. */
  readonly issuer: string;
  /** Transport on which the peer was observed. */
  readonly transport: string;
  /** Category of the identified subject. */
  readonly subjectKind: PeerSubjectKind;
  /** Provider-specific subject identifier, when available. */
  readonly subjectKey?: string;
  /** Lifetime over which the subject key remains stable. */
  readonly subjectStability: SubjectStability;
  /** Whether the provider verified the subject key. */
  readonly subjectVerified: boolean;
  /** Immutable additional facts about the peer. */
  readonly attributes: Readonly<Record<string, JsonValue>>;
  /** Immutable permissions or features reported for the peer. */
  readonly capabilities: Readonly<Record<string, JsonValue>>;
  /** Whether the provider verified the reported capabilities. */
  readonly capabilitiesVerified: boolean;
  /** Original source address observed for the peer. */
  readonly sourceAddress?: string;
  /** Address of the trusted proxy that supplied the evidence. */
  readonly proxyAddress?: string;

  constructor(options: PeerIdentityOptions) {
    if (!options.provider || !options.evidenceSource || !options.issuer || !options.transport) {
      throw new TypeError("provider, evidenceSource, issuer, and transport are required");
    }
    for (const [name, value] of Object.entries({
      provider: options.provider,
      evidenceSource: options.evidenceSource,
      issuer: options.issuer,
      transport: options.transport,
      subjectKey: options.subjectKey,
      sourceAddress: options.sourceAddress,
      proxyAddress: options.proxyAddress,
    })) {
      if (value !== undefined) assertWellFormedUtf16(value, name);
    }
    const stability = options.subjectStability ?? SubjectStability.NONE;
    const subjectKind = options.subjectKind ?? PeerSubjectKind.UNKNOWN;
    if (!IDENTITY_ASSURANCES.has(options.assurance)) throw new TypeError("invalid peer identity assurance");
    if (!PEER_SUBJECT_KINDS.has(subjectKind)) throw new TypeError("invalid peer subject kind");
    if (!SUBJECT_STABILITIES.has(stability)) throw new TypeError("invalid peer subject stability");
    if (options.subjectVerified && !options.subjectKey)
      throw new TypeError("verified peer identity requires subjectKey");
    if (!options.subjectKey && stability !== SubjectStability.NONE) {
      throw new TypeError("subjectless peer identity must use none stability");
    }
    this.provider = options.provider;
    this.evidenceSource = options.evidenceSource;
    this.assurance = options.assurance;
    this.issuer = options.issuer;
    this.transport = options.transport;
    this.subjectKind = subjectKind;
    this.subjectKey = options.subjectKey;
    this.subjectStability = stability;
    this.subjectVerified = options.subjectVerified ?? false;
    this.attributes = snapshotObject(options.attributes, "peer attributes");
    this.capabilities = snapshotObject(options.capabilities, "peer capabilities");
    this.capabilitiesVerified = options.capabilitiesVerified ?? false;
    this.sourceAddress = options.sourceAddress;
    this.proxyAddress = options.proxyAddress;
    Object.freeze(this);
  }

  /** Canonical VGI principal derived from the provider, issuer, and subject. */
  get canonicalPrincipal(): string {
    if (!this.subjectKey) throw new TypeError("subjectless peer evidence has no canonical principal");
    return `peer/${percentIdentity(this.provider)}/${percentIdentity(this.issuer)}/${percentIdentity(this.subjectKey)}`;
  }
}

function percentIdentity(value: string): string {
  let out = "";
  for (const byte of new TextEncoder().encode(value)) {
    const character = String.fromCharCode(byte);
    out += /[A-Za-z0-9._~-]/.test(character) ? character : `%${byte.toString(16).toUpperCase().padStart(2, "0")}`;
  }
  return out;
}

/** Immutable outcome from one peer identity provider. */
export class PeerIdentityResult {
  /** Provider that produced this result. */
  readonly provider: string;
  /** Outcome of resolving peer identity. */
  readonly status: PeerIdentityStatus;
  /** Identities produced when the status is available. */
  readonly identities: readonly PeerIdentity[];

  constructor(provider: string, status: PeerIdentityStatus, identities: readonly PeerIdentity[] = []) {
    if (!provider) throw new TypeError("peer identity provider is required");
    assertWellFormedUtf16(provider, "peer identity provider");
    if (!PEER_IDENTITY_STATUSES.has(status)) throw new TypeError("invalid peer identity status");
    if ((status === PeerIdentityStatus.AVAILABLE) !== identities.length > 0) {
      throw new TypeError("only an available result may carry identities");
    }
    if (identities.some((identity) => identity.provider !== provider))
      throw new TypeError("peer result provider mismatch");
    this.provider = provider;
    this.status = status;
    this.identities = Object.freeze([...identities]);
    Object.freeze(this);
  }

  /** Creates an available result containing one identity. */
  static available(identity: PeerIdentity): PeerIdentityResult {
    return new PeerIdentityResult(identity.provider, PeerIdentityStatus.AVAILABLE, [identity]);
  }
}

/** Immutable aggregate of one result per configured provider. */
export class PeerEvidenceSet {
  /** Shared empty evidence set. */
  static readonly EMPTY = new PeerEvidenceSet();
  /** All identities produced by configured providers. */
  readonly identities: readonly PeerIdentity[];
  readonly #statuses: ReadonlyMap<string, PeerIdentityStatus>;

  constructor(results: readonly PeerIdentityResult[] = []) {
    const statuses = new Map<string, PeerIdentityStatus>();
    const identities: PeerIdentity[] = [];
    for (const result of results) {
      if (!PEER_IDENTITY_STATUSES.has(result.status)) {
        throw new TypeError(`invalid peer identity status: ${String(result.status)}`);
      }
      if (statuses.has(result.provider)) throw new TypeError(`duplicate peer identity provider: ${result.provider}`);
      statuses.set(result.provider, result.status);
      identities.push(...result.identities);
    }
    this.#statuses = statuses;
    this.identities = Object.freeze(identities);
    Object.freeze(this);
  }

  /** Returns a provider's status, or `off` when it has no result. */
  status(provider: string): PeerIdentityStatus {
    return this.#statuses.get(provider) ?? PeerIdentityStatus.OFF;
  }

  /** Returns all identities produced by a provider. */
  forProvider(provider: string): readonly PeerIdentity[] {
    return Object.freeze(this.identities.filter((identity) => identity.provider === provider));
  }

  /** Returns verified, stable subjects eligible for authentication. */
  eligibleSubjects(provider: string): readonly PeerIdentity[] {
    return Object.freeze(
      this.forProvider(provider).filter(
        (identity) =>
          identity.subjectVerified && !!identity.subjectKey && identity.subjectStability === SubjectStability.STABLE,
      ),
    );
  }

  /** Returns the provider's sole eligible subject or rejects ambiguity. */
  uniqueVerifiedSubject(provider: string): PeerIdentity {
    const matches = this.eligibleSubjects(provider);
    if (matches.length !== 1) {
      throw new PeerIdentityRejectedError(
        `provider ${JSON.stringify(provider)} did not produce one verified stable subject`,
      );
    }
    return matches[0];
  }

  /** Requires one verified stable subject from an operational provider. */
  requireUsableProvider(provider: string): PeerIdentity {
    const status = this.status(provider);
    if (status === PeerIdentityStatus.UNAVAILABLE || status === PeerIdentityStatus.PERMISSION_DENIED) {
      throw new PeerIdentityUnavailableError(`peer identity provider ${JSON.stringify(provider)} is unavailable`);
    }
    if (status === PeerIdentityStatus.INVALID || status === PeerIdentityStatus.UNTRUSTED_PROXY) {
      throw new PeerIdentityRejectedError(
        `peer identity provider ${JSON.stringify(provider)} rejected evidence`,
        status === PeerIdentityStatus.UNTRUSTED_PROXY ? "proxy_required" : "invalid_credential",
      );
    }
    return this.uniqueVerifiedSubject(provider);
  }

  /** Requires an operational provider with at least one identity. */
  requireAvailableProvider(provider: string): readonly PeerIdentity[] {
    const status = this.status(provider);
    if (status === PeerIdentityStatus.UNAVAILABLE || status === PeerIdentityStatus.PERMISSION_DENIED) {
      throw new PeerIdentityUnavailableError(`peer identity provider ${JSON.stringify(provider)} is unavailable`);
    }
    if (status === PeerIdentityStatus.INVALID || status === PeerIdentityStatus.UNTRUSTED_PROXY) {
      throw new PeerIdentityRejectedError(
        `peer identity provider ${JSON.stringify(provider)} rejected evidence`,
        status === PeerIdentityStatus.UNTRUSTED_PROXY ? "proxy_required" : "invalid_credential",
      );
    }
    const identities = this.forProvider(provider);
    if (status !== PeerIdentityStatus.AVAILABLE || identities.length === 0) {
      throw new PeerIdentityRejectedError(
        `peer identity provider ${JSON.stringify(provider)} did not produce evidence`,
      );
    }
    return identities;
  }

  /** Computes a deterministic digest binding selected evidence to authentication. */
  async bindingDigest(providers: readonly string[], applicationAuth?: AuthContext): Promise<string> {
    const fields: string[] = [];
    for (const provider of [...new Set(providers)].sort()) {
      fields.push(provider, this.status(provider));
      const identities = this.forProvider(provider)
        .map((identity) => [
          identity.provider,
          identity.issuer,
          identity.subjectKey ?? "",
          identity.assurance,
          identity.evidenceSource,
          identity.transport,
          identity.subjectKind,
          identity.subjectStability,
          String(identity.subjectVerified),
          String(identity.capabilitiesVerified),
          // Routing topology is audit evidence, not authorization evidence.
          // Empty placeholders preserve the shared null-address digest vector.
          "",
          "",
          canonicalJson(identity.attributes),
          canonicalJson(identity.capabilities),
        ])
        .sort((a, b) => compareFields(a, b));
      for (const identity of identities) fields.push(...identity);
    }
    if (applicationAuth) fields.push("application_auth", applicationAuth.domain ?? "", applicationAuth.principal ?? "");
    let size = 0;
    const encoded = fields.map((field) => {
      const bytes = new TextEncoder().encode(field);
      size += 8 + bytes.length;
      return bytes;
    });
    const input = new Uint8Array(size);
    const view = new DataView(input.buffer);
    let offset = 0;
    for (const bytes of encoded) {
      view.setBigUint64(offset, BigInt(bytes.length));
      offset += 8;
      input.set(bytes, offset);
      offset += bytes.length;
    }
    return sha256Hex(input);
  }
}

function compareFields(a: readonly string[], b: readonly string[]): number {
  for (let index = 0; index < a.length; index++) {
    const comparison = compareUnicode(a[index], b[index]);
    if (comparison !== 0) return comparison;
  }
  return 0;
}

/** Locale-independent Unicode scalar ordering, matching Python and UTF-8. */
function compareUnicode(a: string, b: string): number {
  const left = Array.from(a, (character) => character.codePointAt(0) as number);
  const right = Array.from(b, (character) => character.codePointAt(0) as number);
  for (let index = 0; index < Math.min(left.length, right.length); index++) {
    if (left[index] < right[index]) return -1;
    if (left[index] > right[index]) return 1;
  }
  return left.length - right.length;
}

/** Indicates that required peer identity evidence is temporarily unavailable. */
export class PeerIdentityUnavailableError extends Error {
  /** Suggested retry delay in seconds. */
  readonly retryAfter: number;

  constructor(message = "peer identity provider unavailable", retryAfter = 5) {
    super(message);
    this.name = "PeerIdentityUnavailableError";
    this.retryAfter = retryAfter;
  }
}

/** Indicates that peer identity evidence cannot satisfy authentication. */
export class PeerIdentityRejectedError extends Error {
  /** Stable VGI authentication failure reason. */
  readonly vgiAuthReason: "invalid_credential" | "proxy_required";

  constructor(message: string, reason: "invalid_credential" | "proxy_required" = "invalid_credential") {
    super(message);
    this.name = "PeerIdentityRejectedError";
    this.vgiAuthReason = reason;
  }
}

/** Resolves transport context into peer identity evidence. */
export interface PeerIdentityProvider {
  /** Stable provider name used to group results and policy configuration. */
  readonly provider: string;
  /** Resolves peer evidence for a transport request. */
  resolve(context: PeerResolutionContext, signal?: AbortSignal): PeerIdentityResult | Promise<PeerIdentityResult>;
}

/** Policy that combines peer evidence with application authentication. */
export type PeerAuthenticationPolicy = (
  evidence: PeerEvidenceSet,
  existingAuth: AuthContext,
) => AuthContext | Promise<AuthContext>;
/** Verifies that identities from multiple domains belong to the same principal. */
export type PeerIdentityLinker = (
  applicationAuth: AuthContext,
  identities: ReadonlyMap<string, PeerIdentity>,
) => void | Promise<void>;

/** Preserves application authentication while making peer evidence observable. */
export function observePeerIdentity(_evidence: PeerEvidenceSet, auth: AuthContext): AuthContext {
  return auth;
}

/** Requires a provider to produce evidence and binds it to existing authentication. */
export function requirePeerIdentity(provider: string): PeerAuthenticationPolicy {
  return async (evidence, auth) => {
    evidence.requireAvailableProvider(provider);
    return withEvidenceBinding(auth, await evidence.bindingDigest([provider]));
  };
}

/** Uses one provider's verified stable subject as the primary principal. */
export function peerIdentityPrimary(provider: string): PeerAuthenticationPolicy {
  return async (evidence) => {
    const identity = evidence.requireUsableProvider(provider);
    return new AuthContext(provider, true, identity.canonicalPrincipal, {
      issuer: identity.issuer,
      subject_kind: identity.subjectKind,
      assurance: identity.assurance,
      evidence_source: identity.evidenceSource,
      subject: identity.subjectKey,
      peer_evidence_binding: await evidence.bindingDigest([provider]),
    });
  };
}

/** Authenticates with the first usable provider when application authentication is absent. */
export function anyOfPeerIdentities(...providers: string[]): PeerAuthenticationPolicy {
  if (providers.length === 0) throw new TypeError("at least one peer provider is required");
  return async (evidence, auth) => {
    for (const provider of providers) {
      const status = evidence.status(provider);
      if (status === PeerIdentityStatus.INVALID || status === PeerIdentityStatus.UNTRUSTED_PROXY) {
        throw new PeerIdentityRejectedError(`peer identity provider ${JSON.stringify(provider)} rejected evidence`);
      }
      if (evidence.eligibleSubjects(provider).length > 1) {
        throw new PeerIdentityRejectedError(
          `peer identity provider ${JSON.stringify(provider)} produced ambiguous subjects`,
        );
      }
    }
    if (auth.authenticated) return auth;
    for (const provider of providers) {
      if (
        evidence.status(provider) === PeerIdentityStatus.AVAILABLE &&
        evidence.eligibleSubjects(provider).length === 1
      ) {
        return peerIdentityPrimary(provider)(evidence, auth);
      }
    }
    if (
      providers.some(
        (provider) =>
          evidence.status(provider) === PeerIdentityStatus.UNAVAILABLE ||
          evidence.status(provider) === PeerIdentityStatus.PERMISSION_DENIED,
      )
    ) {
      throw new PeerIdentityUnavailableError("no usable authentication factor; a peer provider is unavailable");
    }
    throw new PeerIdentityRejectedError("no configured provider produced a verified subject");
  };
}

/** Requires all providers, links their identities, and selects a primary principal. */
export function allOfPeerIdentities(
  providers: readonly string[],
  identityLinker: PeerIdentityLinker,
  principalProvider = providers[0],
): PeerAuthenticationPolicy {
  if (providers.length === 0 || !identityLinker)
    throw new TypeError("all-of requires providers and an identity linker");
  if (!providers.includes(principalProvider)) throw new TypeError("principalProvider must be one of providers");
  return async (evidence, auth) => {
    if (!auth.authenticated) throw new PeerIdentityRejectedError("all-of requires application authentication");
    const identities = new Map<string, PeerIdentity>();
    for (const provider of providers) identities.set(provider, evidence.requireUsableProvider(provider));
    await identityLinker(auth, identities);
    const primary = await peerIdentityPrimary(principalProvider)(evidence, auth);
    return new AuthContext(primary.domain, true, primary.principal, {
      ...primary.claims,
      application_domain: auth.domain,
      application_principal: auth.principal,
      peer_evidence_binding: await evidence.bindingDigest(providers, auth),
    });
  };
}

function withEvidenceBinding(auth: AuthContext, binding: string): AuthContext {
  return new AuthContext(auth.domain, auth.authenticated, auth.principal, {
    ...auth.claims,
    peer_evidence_binding: binding,
  });
}
