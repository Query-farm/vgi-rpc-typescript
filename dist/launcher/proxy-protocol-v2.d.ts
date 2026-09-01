import type { Socket } from "node:net";
/** Default bound for a complete PROXY protocol v2 preamble, including TLVs. */
export declare const DEFAULT_MAX_PROXY_V2_BYTES = 536;
/** One TCP endpoint asserted by a trusted PROXY protocol sender. */
export interface ProxyProtocolV2Endpoint {
    /** Normalized IPv4 or IPv6 address asserted by the trusted proxy. */
    readonly address: string;
    /** TCP port asserted by the trusted proxy. */
    readonly port: number;
}
/** Asserted TCP endpoints from one strictly validated PROXY protocol v2 preamble. */
export interface ProxyProtocolV2Address {
    /** Original client endpoint asserted by the trusted proxy. */
    readonly source: ProxyProtocolV2Endpoint;
    /** Worker destination endpoint asserted by the trusted proxy. */
    readonly destination: ProxyProtocolV2Endpoint;
}
/** A malformed, truncated, oversized, or timed-out PROXY protocol preamble. */
export declare class ProxyProtocolV2Error extends Error {
    constructor(message: string);
}
/** Validate and canonicalize one exact IPv4/IPv6 address (never a CIDR or hostname). */
export declare function normalizeProxyIpAddress(value: string): string;
/** Internal comparison key used to match exact trusted proxy addresses. */
export declare function proxyIpAddressKey(value: string): string;
/** Format an endpoint for LocalAPI and peer-resolution contexts. */
export declare function formatProxyEndpoint(value: ProxyProtocolV2Endpoint): string;
/**
 * Parse one exact, bounded PROXY protocol v2 preamble.
 *
 * Only the PROXY command with TCP over IPv4 or IPv6 is accepted. LOCAL,
 * UNSPEC, UDP, Unix sockets, malformed address blocks, and malformed TLVs fail
 * closed. Unknown TLVs are structurally validated and otherwise ignored.
 */
export declare function parseProxyProtocolV2(input: Uint8Array, maximumBytes?: number): ProxyProtocolV2Address;
/**
 * Consume exactly one preamble under a single independent deadline.
 * Bytes received after its declared length are pushed back for Arrow IPC.
 */
export declare function readProxyProtocolV2(socket: Socket, timeoutMs: number, maximumBytes?: number): Promise<ProxyProtocolV2Address>;
//# sourceMappingURL=proxy-protocol-v2.d.ts.map