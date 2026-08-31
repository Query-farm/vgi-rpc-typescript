import { AuthContext } from "./auth.js";
export declare const PeerIdentityStatus: {
    readonly OFF: "off";
    readonly NOT_APPLICABLE: "not_applicable";
    readonly AVAILABLE: "available";
    readonly UNAVAILABLE: "unavailable";
    readonly PERMISSION_DENIED: "permission_denied";
    readonly NO_MATCH: "no_match";
    readonly INVALID: "invalid";
    readonly UNTRUSTED_PROXY: "untrusted_proxy";
};
export type PeerIdentityStatus = (typeof PeerIdentityStatus)[keyof typeof PeerIdentityStatus];
export declare const IdentityAssurance: {
    readonly CRYPTOGRAPHIC_PEER: "cryptographic_peer";
    readonly LOCAL_DAEMON: "local_daemon";
    readonly CONFIGURED_PROXY: "configured_proxy";
};
export type IdentityAssurance = (typeof IdentityAssurance)[keyof typeof IdentityAssurance];
export declare const PeerSubjectKind: {
    readonly USER: "user";
    readonly TAGGED_NODE: "tagged_node";
    readonly WORKLOAD: "workload";
    readonly ENDPOINT: "endpoint";
    readonly UNKNOWN: "unknown";
};
export type PeerSubjectKind = (typeof PeerSubjectKind)[keyof typeof PeerSubjectKind];
export declare const SubjectStability: {
    readonly STABLE: "stable";
    readonly LOGIN: "login";
    readonly NONE: "none";
};
export type SubjectStability = (typeof SubjectStability)[keyof typeof SubjectStability];
export type JsonValue = null | boolean | number | string | readonly JsonValue[] | {
    readonly [key: string]: JsonValue;
};
export interface PeerResolutionOptions {
    immediatePeer?: string;
    sourceEndpoint?: string;
    assertedPeer?: string;
    destinationAddress?: string;
    authority?: string;
    serviceName?: string;
    headers?: ReadonlyMap<string, readonly string[]> | Readonly<Record<string, readonly string[]>>;
    metadata?: Readonly<Record<string, unknown>>;
    /** Absolute Unix epoch milliseconds. */
    deadline?: number;
    /** Monotonic total provider budget in milliseconds. */
    budgetMs?: number;
}
/** Immutable provider-neutral transport request snapshot. */
export declare class PeerResolutionContext {
    #private;
    readonly transport: string;
    readonly immediatePeer?: string;
    readonly sourceEndpoint?: string;
    readonly assertedPeer?: string;
    readonly destinationAddress?: string;
    readonly authority?: string;
    readonly serviceName?: string;
    readonly metadata: Readonly<Record<string, JsonValue>>;
    readonly deadline?: number;
    readonly budgetMs?: number;
    constructor(transport: string, options?: PeerResolutionOptions);
    header(name: string): string | undefined;
    /** Remaining provider budget measured with the runtime's monotonic clock. */
    remainingBudgetMs(): number | undefined;
}
export interface PeerIdentityOptions {
    provider: string;
    evidenceSource: string;
    assurance: IdentityAssurance;
    issuer: string;
    transport: string;
    subjectKind?: PeerSubjectKind;
    subjectKey?: string;
    subjectStability?: SubjectStability;
    subjectVerified?: boolean;
    attributes?: Readonly<Record<string, unknown>>;
    capabilities?: Readonly<Record<string, unknown>>;
    capabilitiesVerified?: boolean;
    sourceAddress?: string;
    proxyAddress?: string;
}
/** Immutable verified or observed transport-peer evidence. */
export declare class PeerIdentity {
    readonly provider: string;
    readonly evidenceSource: string;
    readonly assurance: IdentityAssurance;
    readonly issuer: string;
    readonly transport: string;
    readonly subjectKind: PeerSubjectKind;
    readonly subjectKey?: string;
    readonly subjectStability: SubjectStability;
    readonly subjectVerified: boolean;
    readonly attributes: Readonly<Record<string, JsonValue>>;
    readonly capabilities: Readonly<Record<string, JsonValue>>;
    readonly capabilitiesVerified: boolean;
    readonly sourceAddress?: string;
    readonly proxyAddress?: string;
    constructor(options: PeerIdentityOptions);
    get canonicalPrincipal(): string;
}
export declare class PeerIdentityResult {
    readonly provider: string;
    readonly status: PeerIdentityStatus;
    readonly identities: readonly PeerIdentity[];
    constructor(provider: string, status: PeerIdentityStatus, identities?: readonly PeerIdentity[]);
    static available(identity: PeerIdentity): PeerIdentityResult;
}
/** Immutable aggregate of one result per configured provider. */
export declare class PeerEvidenceSet {
    #private;
    static readonly EMPTY: PeerEvidenceSet;
    readonly identities: readonly PeerIdentity[];
    constructor(results?: readonly PeerIdentityResult[]);
    status(provider: string): PeerIdentityStatus;
    forProvider(provider: string): readonly PeerIdentity[];
    eligibleSubjects(provider: string): readonly PeerIdentity[];
    uniqueVerifiedSubject(provider: string): PeerIdentity;
    requireUsableProvider(provider: string): PeerIdentity;
    requireAvailableProvider(provider: string): readonly PeerIdentity[];
    bindingDigest(providers: readonly string[], applicationAuth?: AuthContext): Promise<string>;
}
export declare class PeerIdentityUnavailableError extends Error {
    readonly retryAfter: number;
    constructor(message?: string, retryAfter?: number);
}
export declare class PeerIdentityRejectedError extends Error {
    readonly vgiAuthReason: "invalid_credential" | "proxy_required";
    constructor(message: string, reason?: "invalid_credential" | "proxy_required");
}
export interface PeerIdentityProvider {
    readonly provider: string;
    resolve(context: PeerResolutionContext, signal?: AbortSignal): PeerIdentityResult | Promise<PeerIdentityResult>;
}
export type PeerAuthenticationPolicy = (evidence: PeerEvidenceSet, existingAuth: AuthContext) => AuthContext | Promise<AuthContext>;
export type PeerIdentityLinker = (applicationAuth: AuthContext, identities: ReadonlyMap<string, PeerIdentity>) => void | Promise<void>;
export declare function observePeerIdentity(_evidence: PeerEvidenceSet, auth: AuthContext): AuthContext;
export declare function requirePeerIdentity(provider: string): PeerAuthenticationPolicy;
export declare function peerIdentityPrimary(provider: string): PeerAuthenticationPolicy;
export declare function anyOfPeerIdentities(...providers: string[]): PeerAuthenticationPolicy;
export declare function allOfPeerIdentities(providers: readonly string[], identityLinker: PeerIdentityLinker, principalProvider?: string): PeerAuthenticationPolicy;
//# sourceMappingURL=identity.d.ts.map