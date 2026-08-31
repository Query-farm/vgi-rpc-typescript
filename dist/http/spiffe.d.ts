/** Strict SPIFFE evidence delivered by explicitly trusted HTTP proxies. */
import { type PeerIdentityProvider } from "../identity.js";
/** Preserve Node/Bun `IncomingMessage.rawHeaders` multiplicity for identity resolution. */
export declare function headersFromNodeRawHeaders(rawHeaders: readonly string[]): ReadonlyMap<string, readonly string[]>;
/** Validate one canonical workload SPIFFE ID and return its trust domain. */
export declare function validateSpiffeId(value: string, trustDomains: ReadonlySet<string>): string;
/** Configuration for a generic trusted-proxy X.509-SVID header provider. */
export interface SpiffeX509HeaderProviderOptions {
    /** SPIFFE trust domains accepted from validated workload IDs. */
    readonly trustDomains: Iterable<string>;
    /** Exact normalized proxy IP literals permitted to assert evidence. */
    readonly trustedProxyAddresses: Iterable<string>;
    /** Header carrying the percent-encoded PEM leaf certificate. */
    readonly header?: string;
    /** Header proving that the proxy successfully verified the certificate chain. */
    readonly chainVerifiedHeader: string;
    /** Required value of `chainVerifiedHeader`; defaults to `true`. */
    readonly chainVerifiedValue?: string;
    /** Maximum encoded certificate-header size. */
    readonly maxHeaderBytes?: number;
}
/** Build a strict provider for a proxy-verified X.509-SVID certificate header. */
export declare function spiffeX509HeaderProvider(options: SpiffeX509HeaderProviderOptions): PeerIdentityProvider;
/** Shared configuration for nginx and Azure certificate-header adapters. */
export interface CertificateProxySpiffeOptions {
    /** SPIFFE trust domains accepted from validated workload IDs. */
    readonly trustDomains: Iterable<string>;
    /** Exact normalized proxy IP literals permitted to assert evidence. */
    readonly trustedProxyAddresses: Iterable<string>;
    /** Override for the proxy header containing the encoded leaf certificate. */
    readonly certificateHeader?: string;
    /** Override for the proxy header reporting certificate verification. */
    readonly verificationHeader?: string;
    /** Maximum encoded certificate-header size. */
    readonly maxHeaderBytes?: number;
}
/** Build a provider for nginx's verified client-certificate headers. */
export declare function nginxSpiffeProvider(options: CertificateProxySpiffeOptions): PeerIdentityProvider;
/** Build a provider for Azure Application Gateway's verified mTLS headers. */
export declare function azureApplicationGatewaySpiffeProvider(options: CertificateProxySpiffeOptions): PeerIdentityProvider;
/** Configuration for AWS ALB mTLS verify-mode leaf certificates. */
export interface AwsAlbSpiffeOptions {
    /** SPIFFE trust domains accepted from validated workload IDs. */
    readonly trustDomains: Iterable<string>;
    /** Exact normalized ALB IP literals permitted to assert evidence. */
    readonly trustedProxyAddresses: Iterable<string>;
    /** Header containing ALB's URL-encoded PEM leaf certificate. */
    readonly leafHeader?: string;
    /** Maximum encoded certificate-header size. */
    readonly maxHeaderBytes?: number;
}
/** Build a provider for AWS ALB mTLS verify-mode leaf evidence. */
export declare function awsAlbSpiffeProvider(options: AwsAlbSpiffeOptions): PeerIdentityProvider;
/** Configuration for Google Cloud Load Balancing mTLS identity headers. */
export interface GcpLoadBalancerSpiffeOptions {
    /** SPIFFE trust domains accepted from validated workload IDs. */
    readonly trustDomains: Iterable<string>;
    /** Exact normalized load-balancer IP literals permitted to assert evidence. */
    readonly trustedProxyAddresses: Iterable<string>;
    /** Header containing the verified client SPIFFE ID. */
    readonly spiffeIdHeader?: string;
    /** Header reporting whether a client certificate was presented. */
    readonly presentHeader?: string;
    /** Header reporting whether the client certificate chain was verified. */
    readonly chainVerifiedHeader?: string;
    /** Header carrying any certificate verification error. */
    readonly errorHeader?: string;
}
/** Build a provider for Google Cloud Load Balancing mTLS headers. */
export declare function gcpLoadBalancerSpiffeProvider(options: GcpLoadBalancerSpiffeOptions): PeerIdentityProvider;
/** Configuration for Envoy's sanitized X-Forwarded-Client-Cert evidence. */
export interface EnvoyXfccSpiffeOptions {
    /** SPIFFE trust domains accepted from validated workload IDs. */
    readonly trustDomains: Iterable<string>;
    /** Exact normalized Envoy IP literals permitted to assert evidence. */
    readonly trustedProxyAddresses: Iterable<string>;
    /** XFCC header name; defaults to `X-Forwarded-Client-Cert`. */
    readonly header?: string;
    /** Maximum accepted XFCC header size. */
    readonly maxHeaderBytes?: number;
}
/** Build a provider for one sanitized Envoy XFCC element. */
export declare function envoyXfccSpiffeProvider(options: EnvoyXfccSpiffeOptions): PeerIdentityProvider;
//# sourceMappingURL=spiffe.d.ts.map