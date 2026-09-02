import type { RpcClient } from "./connect.js";
import type { PipeConnectOptions } from "./types.js";
export declare const IROH_ARROW_MUX_ALPN = "vgi-rpc/arrow-mux/1";
export declare const IROH_HTTP_ALPN = "iroh-http/2";
export type IrohErrorStage = "parse" | "bind" | "resolve" | "connect" | "alpn" | "open_stream" | "write" | "read" | "cancel" | "close" | "internal";
export type IrohErrorCategory = "invalid_input" | "unsupported" | "unavailable" | "timeout" | "protocol" | "connection_reset" | "cancelled" | "authentication" | "resource_exhausted" | "internal";
export type IrohDispatchCertainty = "not_sent" | "unknown" | "sent";
/** Portable failure dimensions shared by every VGI Iroh transport. */
export declare class IrohTransportError extends Error {
    readonly stage: IrohErrorStage;
    readonly category: IrohErrorCategory;
    readonly dispatchCertainty: IrohDispatchCertainty;
    constructor(message: string, stage: IrohErrorStage, category: IrohErrorCategory, dispatchCertainty: IrohDispatchCertainty, options?: ErrorOptions);
}
export declare class IrohUriError extends IrohTransportError {
    constructor(message: string);
}
export interface IrohEndpoint {
    readonly scheme: "iroh" | "httpi";
    readonly endpointId: string;
    readonly endpointIdBytes: Uint8Array;
    readonly basePath: string;
    readonly alpn: typeof IROH_ARROW_MUX_ALPN | typeof IROH_HTTP_ALPN;
}
/** Parse the canonical VGI Iroh URI without URL-parser hostname normalization. */
export declare function parseIrohEndpoint(raw: string): IrohEndpoint;
interface NativeRecvStream {
    read(limit: number): Promise<number[]>;
    stop(errorCode: bigint): Promise<void>;
}
interface NativeSendStream {
    writeAll(bytes: number[]): Promise<void>;
    finish(): Promise<void>;
    reset(errorCode: bigint): Promise<void>;
}
interface NativeConnection {
    openBi(): Promise<{
        recv: NativeRecvStream;
        send: NativeSendStream;
    }>;
    close(errorCode: bigint, reason: number[]): void;
}
interface NativeEndpoint {
    connect(address: unknown, alpn: number[]): Promise<NativeConnection>;
    close(): Promise<void>;
}
interface NativeEndpointBuilder {
    applyN0(): void;
    applyN0DisableRelay(): void;
    secretKey(bytes: number[]): void;
    relayMode(mode: unknown): void;
    bind(): Promise<NativeEndpoint>;
}
/** Minimal surface implemented by the official `@number0/iroh` Node binding. */
export interface IrohNativeBinding {
    Endpoint: {
        builder(): NativeEndpointBuilder;
    };
    EndpointId: {
        fromBytes(bytes: number[]): unknown;
    };
    EndpointAddr: new (id: unknown, relayUrl?: string | null, addresses?: string[] | null) => unknown;
    RelayMode: {
        customFromUrls(urls: string[]): unknown;
    };
}
export interface IrohConnectOptions extends PipeConnectOptions {
    /** Optional 32-byte Ed25519 secret. Omit for a process-local ephemeral identity. */
    secretKey?: Uint8Array;
    /** Custom relay URLs. Mutually exclusive with `noRelay`. */
    relayUrls?: readonly string[];
    /** Disable relay use. Direct discovery/addressing must then succeed. */
    noRelay?: boolean;
    /** Total endpoint bind, connection, and stream-open deadline. Default: 30000 ms. */
    connectTimeoutMs?: number;
    /** Deadline for each active native read or write. Default: 300000 ms. */
    ioTimeoutMs?: number;
    /** Optional relay hint for the remote endpoint. */
    remoteRelayUrl?: string;
    /** Optional direct socket-address hints for the remote endpoint. */
    directAddresses?: readonly string[];
    /** Cancels connection setup and later closes the active native connection. */
    signal?: AbortSignal;
    /** Dependency-injection seam for testing or an application-pinned binding. */
    binding?: IrohNativeBinding;
}
/**
 * Connect the ordinary VGI raw client over a native Iroh bidirectional stream.
 * HTTP-over-Iroh is deliberately not routed through this function: it needs an
 * iroh-http/2 codec, not raw Arrow-mux framing. The httpi scheme is therefore
 * parsed for the shared endpoint contract but explicitly unsupported here.
 */
export declare function irohConnect(rawEndpoint: string, options?: IrohConnectOptions): Promise<RpcClient>;
export {};
//# sourceMappingURL=iroh.d.ts.map