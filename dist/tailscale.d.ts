import { type PeerIdentityProvider } from "./identity.js";
export interface TailscaleServeOptions {
    readonly issuer: string;
    readonly trustedProxyAddresses: Iterable<string>;
    readonly maxHeaderBytes?: number;
}
/** Consume identity and application-capability headers from an exact trusted Serve peer. */
export declare function tailscaleServeIdentityProvider(options: TailscaleServeOptions): PeerIdentityProvider;
export interface TailscaleLocalApiOptions {
    readonly issuer: string;
    /** Unix-domain socket. Linux defaults to `/var/run/tailscale/tailscaled.sock`. */
    readonly unixSocket?: string;
    /** Explicit plain-HTTP local origin (for example a same-user-proof endpoint). */
    readonly endpoint?: string;
    /** Basic-auth password, valid only with `endpoint`. */
    readonly password?: string;
    readonly timeoutMs?: number;
    readonly maxResponseBytes?: number;
    readonly maxResponseHeaderBytes?: number;
}
/** Resolve a fresh LocalAPI `/localapi/v0/whois` snapshot for every call. */
export declare function tailscaleLocalApiIdentityProvider(options: TailscaleLocalApiOptions): PeerIdentityProvider;
//# sourceMappingURL=tailscale.d.ts.map