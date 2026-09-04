import type { RpcClient } from "./connect.js";
import type { PipeConnectOptions } from "./types.js";
/** ALPN negotiated by the stateful raw Arrow-mux transport. */
export declare const IROH_ARROW_MUX_ALPN = "vgi-rpc/arrow-mux/1";
/** ALPN negotiated by HTTP-over-Iroh connections. */
export declare const IROH_HTTP_ALPN = "iroh-http/2";
/** Transport operation in progress when an Iroh failure occurred. */
export type IrohErrorStage = "parse" | "bind" | "resolve" | "connect" | "alpn" | "open_stream" | "write" | "read" | "cancel" | "close" | "internal";
/** Portable category assigned to an Iroh transport failure. */
export type IrohErrorCategory = "invalid_input" | "unsupported" | "unavailable" | "timeout" | "protocol" | "connection_reset" | "cancelled" | "authentication" | "resource_exhausted" | "internal";
/** Whether a failed operation may have reached the remote worker. */
export type IrohDispatchCertainty = "not_sent" | "unknown" | "sent";
/** Portable failure dimensions shared by every VGI Iroh transport. */
export declare class IrohTransportError extends Error {
    /** Transport operation in progress when the failure occurred. */
    readonly stage: IrohErrorStage;
    /** Portable failure category suitable for caller policy. */
    readonly category: IrohErrorCategory;
    /** Whether the failed operation may have reached the remote worker. */
    readonly dispatchCertainty: IrohDispatchCertainty;
    constructor(message: string, stage: IrohErrorStage, category: IrohErrorCategory, dispatchCertainty: IrohDispatchCertainty, options?: ErrorOptions);
}
export declare class IrohUriError extends IrohTransportError {
    constructor(message: string);
}
/** Parsed canonical address and negotiated protocol for a VGI Iroh endpoint. */
export interface IrohEndpoint {
    /** URI transport scheme. */
    readonly scheme: "iroh" | "httpi";
    /** Remote endpoint's canonical 64-character lowercase hexadecimal ID. */
    readonly endpointId: string;
    /** Remote endpoint ID decoded into its 32-byte binary representation. */
    readonly endpointIdBytes: Uint8Array;
    /** Canonical HTTP base path, or an empty string for raw Iroh. */
    readonly basePath: string;
    /** ALPN required by the selected transport scheme. */
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
    /** Native endpoint factory exported by the binding. */
    Endpoint: {
        /** Creates a configurable native endpoint builder. */
        builder(): NativeEndpointBuilder;
    };
    /** Native endpoint-ID factory exported by the binding. */
    EndpointId: {
        /** Decodes a 32-byte endpoint identifier. */
        fromBytes(bytes: number[]): unknown;
    };
    /** Native remote-address constructor exported by the binding. */
    EndpointAddr: {
        /** Combines an endpoint ID with optional relay and direct-address hints. */
        new (id: unknown, relayUrl?: string | null, addresses?: string[] | null): unknown;
    };
    /** Native relay-mode factory exported by the binding. */
    RelayMode: {
        /** Creates a custom relay mode from the supplied relay URLs. */
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
 * HTTP-over-Iroh is available separately through `httpiConnect`; it uses the
 * iroh-http/2 codec rather than raw Arrow-mux framing.
 */
export declare function irohConnect(rawEndpoint: string, options?: IrohConnectOptions): Promise<RpcClient>;
export {};
//# sourceMappingURL=iroh.d.ts.map