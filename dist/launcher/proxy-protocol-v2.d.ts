import type { Socket } from "node:net";
/** Default bound for a complete PROXY protocol v2 preamble, including TLVs. */
export declare const DEFAULT_MAX_PROXY_V2_BYTES = 536;
/** Fixed VGI identity TLV used only by an explicitly trusted Iroh bridge. */
export declare const VGI_IROH_ENDPOINT_TLV = 224;
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
/** Non-IP peer identity carried by a trusted Iroh bridge. */
export interface ProxyProtocolV2IrohIdentity {
    /** Canonical lowercase hexadecimal encoding of the 32-byte EndpointId. */
    readonly endpointId: string;
}
/** Internal result for a listener accepting ordinary IP or opt-in Iroh forwarding. */
export interface ProxyProtocolV2ForwardedPeer {
    readonly address?: ProxyProtocolV2Address;
    readonly irohIdentity?: ProxyProtocolV2IrohIdentity;
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
/** Parse the dedicated PROXY/UNSPEC form emitted by a trusted Iroh bridge. */
export declare function parseIrohProxyProtocolV2(input: Uint8Array, maximumBytes?: number): ProxyProtocolV2IrohIdentity;
/**
 * Consume exactly one preamble under a single independent deadline.
 * Bytes received after its declared length are pushed back for Arrow IPC.
 */
export declare function readProxyProtocolV2(socket: Socket, timeoutMs: number, maximumBytes?: number): Promise<ProxyProtocolV2Address>;
/** Consume and parse the dedicated trusted Iroh PROXY/UNSPEC preamble. */
export declare function readIrohProxyProtocolV2(socket: Socket, timeoutMs: number, maximumBytes?: number): Promise<ProxyProtocolV2IrohIdentity>;
/** Consume either strict TCP/IP PROXY v2 or the opt-in Iroh PROXY/UNSPEC form. */
export declare function readProxyProtocolV2AllowingIrohIdentity(socket: Socket, timeoutMs: number, maximumBytes?: number): Promise<ProxyProtocolV2ForwardedPeer>;
//# sourceMappingURL=proxy-protocol-v2.d.ts.map