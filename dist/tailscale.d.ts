import { type PeerIdentityProvider } from "./identity.js";
/** Trust boundary and resource limits for Tailscale Serve identity headers. */
export interface TailscaleServeOptions {
    /** Operator-defined namespace that distinguishes identities from different tailnets. */
    readonly issuer: string;
    /** Exact normalized IP literals allowed to supply Tailscale Serve headers. */
    readonly trustedProxyAddresses: Iterable<string>;
    /** Maximum encoded bytes accepted in any identity or capability header. */
    readonly maxHeaderBytes?: number;
}
/** Consume identity and application-capability headers from an exact trusted Serve peer. */
export declare function tailscaleServeIdentityProvider(options: TailscaleServeOptions): PeerIdentityProvider;
/** Connection, namespace, and response limits for tailscaled LocalAPI WhoIs. */
export interface TailscaleLocalApiOptions {
    /** Operator-defined namespace that distinguishes identities from different tailnets. */
    readonly issuer: string;
    /** Unix-domain socket. Linux defaults to `/var/run/tailscale/tailscaled.sock`. */
    readonly unixSocket?: string;
    /** Explicit plain-HTTP local origin (for example a same-user-proof endpoint). */
    readonly endpoint?: string;
    /** Basic-auth password, valid only with `endpoint`. */
    readonly password?: string;
    /** Total LocalAPI lookup timeout in milliseconds. */
    readonly timeoutMs?: number;
    /** Maximum decoded LocalAPI response body size. */
    readonly maxResponseBytes?: number;
    /** Maximum LocalAPI response-header size. */
    readonly maxResponseHeaderBytes?: number;
}
/** Resolve a fresh LocalAPI `/localapi/v0/whois` snapshot for every call. */
export declare function tailscaleLocalApiIdentityProvider(options: TailscaleLocalApiOptions): PeerIdentityProvider;
//# sourceMappingURL=tailscale.d.ts.map