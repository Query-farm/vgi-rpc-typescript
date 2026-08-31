import { AuthContext } from "./auth.js";
/** Outcome reported by a peer identity provider. */
export declare const PeerIdentityStatus: {
    /** The provider is not configured. */
    readonly OFF: "off";
    /** The provider does not apply to this transport or request. */
    readonly NOT_APPLICABLE: "not_applicable";
    /** The provider produced peer identity evidence. */
    readonly AVAILABLE: "available";
    /** The provider could not be reached or queried. */
    readonly UNAVAILABLE: "unavailable";
    /** The provider lacks permission to inspect peer identity. */
    readonly PERMISSION_DENIED: "permission_denied";
    /** The provider found no matching peer identity. */
    readonly NO_MATCH: "no_match";
    /** The supplied peer evidence is invalid. */
    readonly INVALID: "invalid";
    /** Peer evidence came through an untrusted proxy. */
    readonly UNTRUSTED_PROXY: "untrusted_proxy";
};
/** A peer identity provider outcome. */
export type PeerIdentityStatus = (typeof PeerIdentityStatus)[keyof typeof PeerIdentityStatus];
/** Mechanism that established a peer identity. */
export declare const IdentityAssurance: {
    /** Identity is bound directly to a cryptographic peer. */
    readonly CRYPTOGRAPHIC_PEER: "cryptographic_peer";
    /** Identity was reported by a trusted local daemon. */
    readonly LOCAL_DAEMON: "local_daemon";
    /** Identity was asserted by a configured trusted proxy. */
    readonly CONFIGURED_PROXY: "configured_proxy";
};
/** Assurance mechanism for peer identity evidence. */
export type IdentityAssurance = (typeof IdentityAssurance)[keyof typeof IdentityAssurance];
/** Category of subject identified by peer evidence. */
export declare const PeerSubjectKind: {
    /** A human user. */
    readonly USER: "user";
    /** A tagged infrastructure node. */
    readonly TAGGED_NODE: "tagged_node";
    /** A service workload. */
    readonly WORKLOAD: "workload";
    /** A network endpoint. */
    readonly ENDPOINT: "endpoint";
    /** A subject whose category is unknown. */
    readonly UNKNOWN: "unknown";
};
/** Category of a peer identity subject. */
export type PeerSubjectKind = (typeof PeerSubjectKind)[keyof typeof PeerSubjectKind];
/** Lifetime over which a peer subject key remains stable. */
export declare const SubjectStability: {
    /** Stable across logins and sessions. */
    readonly STABLE: "stable";
    /** Stable only for the current login. */
    readonly LOGIN: "login";
    /** No stable subject key is available. */
    readonly NONE: "none";
};
/** Stability class of a peer subject key. */
export type SubjectStability = (typeof SubjectStability)[keyof typeof SubjectStability];
/** Immutable value representable in JSON. */
export type JsonValue = null | boolean | number | string | readonly JsonValue[] | {
    readonly [key: string]: JsonValue;
};
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
export declare class PeerResolutionContext {
    #private;
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
    constructor(transport: string, options?: PeerResolutionOptions);
    /** Returns the single value for a case-insensitive header name. */
    header(name: string): string | undefined;
    /** Remaining provider budget measured with the runtime's monotonic clock. */
    remainingBudgetMs(): number | undefined;
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
export declare class PeerIdentity {
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
    constructor(options: PeerIdentityOptions);
    /** Canonical VGI principal derived from the provider, issuer, and subject. */
    get canonicalPrincipal(): string;
}
/** Immutable outcome from one peer identity provider. */
export declare class PeerIdentityResult {
    /** Provider that produced this result. */
    readonly provider: string;
    /** Outcome of resolving peer identity. */
    readonly status: PeerIdentityStatus;
    /** Identities produced when the status is available. */
    readonly identities: readonly PeerIdentity[];
    constructor(provider: string, status: PeerIdentityStatus, identities?: readonly PeerIdentity[]);
    /** Creates an available result containing one identity. */
    static available(identity: PeerIdentity): PeerIdentityResult;
}
/** Immutable aggregate of one result per configured provider. */
export declare class PeerEvidenceSet {
    #private;
    /** Shared empty evidence set. */
    static readonly EMPTY: PeerEvidenceSet;
    /** All identities produced by configured providers. */
    readonly identities: readonly PeerIdentity[];
    constructor(results?: readonly PeerIdentityResult[]);
    /** Returns a provider's status, or `off` when it has no result. */
    status(provider: string): PeerIdentityStatus;
    /** Returns all identities produced by a provider. */
    forProvider(provider: string): readonly PeerIdentity[];
    /** Returns verified, stable subjects eligible for authentication. */
    eligibleSubjects(provider: string): readonly PeerIdentity[];
    /** Returns the provider's sole eligible subject or rejects ambiguity. */
    uniqueVerifiedSubject(provider: string): PeerIdentity;
    /** Requires one verified stable subject from an operational provider. */
    requireUsableProvider(provider: string): PeerIdentity;
    /** Requires an operational provider with at least one identity. */
    requireAvailableProvider(provider: string): readonly PeerIdentity[];
    /** Computes a deterministic digest binding selected evidence to authentication. */
    bindingDigest(providers: readonly string[], applicationAuth?: AuthContext): Promise<string>;
}
/** Indicates that required peer identity evidence is temporarily unavailable. */
export declare class PeerIdentityUnavailableError extends Error {
    /** Suggested retry delay in seconds. */
    readonly retryAfter: number;
    constructor(message?: string, retryAfter?: number);
}
/** Indicates that peer identity evidence cannot satisfy authentication. */
export declare class PeerIdentityRejectedError extends Error {
    /** Stable VGI authentication failure reason. */
    readonly vgiAuthReason: "invalid_credential" | "proxy_required";
    constructor(message: string, reason?: "invalid_credential" | "proxy_required");
}
/** Resolves transport context into peer identity evidence. */
export interface PeerIdentityProvider {
    /** Stable provider name used to group results and policy configuration. */
    readonly provider: string;
    /** Resolves peer evidence for a transport request. */
    resolve(context: PeerResolutionContext, signal?: AbortSignal): PeerIdentityResult | Promise<PeerIdentityResult>;
}
/** Policy that combines peer evidence with application authentication. */
export type PeerAuthenticationPolicy = (evidence: PeerEvidenceSet, existingAuth: AuthContext) => AuthContext | Promise<AuthContext>;
/** Verifies that identities from multiple domains belong to the same principal. */
export type PeerIdentityLinker = (applicationAuth: AuthContext, identities: ReadonlyMap<string, PeerIdentity>) => void | Promise<void>;
/** Preserves application authentication while making peer evidence observable. */
export declare function observePeerIdentity(_evidence: PeerEvidenceSet, auth: AuthContext): AuthContext;
/** Requires a provider to produce evidence and binds it to existing authentication. */
export declare function requirePeerIdentity(provider: string): PeerAuthenticationPolicy;
/** Uses one provider's verified stable subject as the primary principal. */
export declare function peerIdentityPrimary(provider: string): PeerAuthenticationPolicy;
/** Authenticates with the first usable provider when application authentication is absent. */
export declare function anyOfPeerIdentities(...providers: string[]): PeerAuthenticationPolicy;
/** Requires all providers, links their identities, and selects a primary principal. */
export declare function allOfPeerIdentities(providers: readonly string[], identityLinker: PeerIdentityLinker, principalProvider?: string): PeerAuthenticationPolicy;
//# sourceMappingURL=identity.d.ts.map