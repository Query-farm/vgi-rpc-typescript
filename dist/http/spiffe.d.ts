/** Strict SPIFFE evidence delivered by explicitly trusted HTTP proxies. */
import { type PeerIdentityProvider } from "../identity.js";
/** Preserve Node/Bun `IncomingMessage.rawHeaders` multiplicity for identity resolution. */
export declare function headersFromNodeRawHeaders(rawHeaders: readonly string[]): ReadonlyMap<string, readonly string[]>;
/** Validate one canonical workload SPIFFE ID and return its trust domain. */
export declare function validateSpiffeId(value: string, trustDomains: ReadonlySet<string>): string;
export interface SpiffeX509HeaderProviderOptions {
    readonly trustDomains: Iterable<string>;
    readonly trustedProxyAddresses: Iterable<string>;
    readonly header?: string;
    readonly chainVerifiedHeader: string;
    readonly chainVerifiedValue?: string;
    readonly maxHeaderBytes?: number;
}
export declare function spiffeX509HeaderProvider(options: SpiffeX509HeaderProviderOptions): PeerIdentityProvider;
export interface CertificateProxySpiffeOptions {
    readonly trustDomains: Iterable<string>;
    readonly trustedProxyAddresses: Iterable<string>;
    readonly certificateHeader?: string;
    readonly verificationHeader?: string;
    readonly maxHeaderBytes?: number;
}
export declare function nginxSpiffeProvider(options: CertificateProxySpiffeOptions): PeerIdentityProvider;
export declare function azureApplicationGatewaySpiffeProvider(options: CertificateProxySpiffeOptions): PeerIdentityProvider;
export interface AwsAlbSpiffeOptions {
    readonly trustDomains: Iterable<string>;
    readonly trustedProxyAddresses: Iterable<string>;
    readonly leafHeader?: string;
    readonly maxHeaderBytes?: number;
}
export declare function awsAlbSpiffeProvider(options: AwsAlbSpiffeOptions): PeerIdentityProvider;
export interface GcpLoadBalancerSpiffeOptions {
    readonly trustDomains: Iterable<string>;
    readonly trustedProxyAddresses: Iterable<string>;
    readonly spiffeIdHeader?: string;
    readonly presentHeader?: string;
    readonly chainVerifiedHeader?: string;
    readonly errorHeader?: string;
}
export declare function gcpLoadBalancerSpiffeProvider(options: GcpLoadBalancerSpiffeOptions): PeerIdentityProvider;
export interface EnvoyXfccSpiffeOptions {
    readonly trustDomains: Iterable<string>;
    readonly trustedProxyAddresses: Iterable<string>;
    readonly header?: string;
    readonly maxHeaderBytes?: number;
}
export declare function envoyXfccSpiffeProvider(options: EnvoyXfccSpiffeOptions): PeerIdentityProvider;
//# sourceMappingURL=spiffe.d.ts.map