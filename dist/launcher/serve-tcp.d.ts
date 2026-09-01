import type { ExternalLocationConfig } from "../external.js";
import { type PeerAuthenticationPolicy, type PeerIdentityProvider } from "../identity.js";
import type { Protocol } from "../protocol.js";
import { type DispatchHook, type ServeStartHook } from "../types.js";
/** Configuration for {@link serveTcp}. */
export interface ServeTcpOptions {
    /** Interface to bind.  Defaults to `127.0.0.1` (loopback only).  Binding a
     *  routable address exposes the unauthenticated framing on the network. */
    host?: string;
    /** TCP port to bind.  Defaults to `0`, which lets the OS pick a free port
     *  (reported via the `TCP:<host>:<port>` line and {@link onBound}). */
    port?: number;
    /** Self-terminate after this many seconds with zero connected clients.
     *  Default: 300.  `0` disables the timer (server runs until killed). */
    idleTimeout?: number;
    /** Grace period after `listen()` succeeds before the idle timer starts
     *  ticking.  Default: 5 — gives the first launcher caller a chance to
     *  connect after the `TCP:<host>:<port>` announcement. */
    startupGraceSeconds?: number;
    /** Optional logical-service / protocol-contract version label. */
    protocolVersion?: string;
    /** Custom server identifier. */
    serverId?: string;
    /** Enable __describe__ method. Default: true. */
    enableDescribe?: boolean;
    /** Optional dispatch hook for observability. */
    dispatchHook?: DispatchHook;
    /** Optional external-storage config for large-batch externalisation. */
    externalLocation?: ExternalLocationConfig;
    /** Lifecycle hook fired once before the first dispatched request. */
    onServeStart?: ServeStartHook;
    /** Maximum listen backlog.  Default: 128. */
    backlog?: number;
    /** Called *after* `listen()` returns successfully but *before*
     *  `TCP:<host>:<port>` is printed.  Invoked with the bound host and the
     *  *actual* bound port (resolved when `port=0`). */
    onBound?: (host: string, port: number) => void;
    /** Override the stream used for the `TCP:<host>:<port>` line.  Defaults to
     *  `process.stdout`. */
    announcementSink?: NodeJS.WritableStream;
    /** Resolve peer identity once per accepted connection. Evidence is snapshotted
     *  for the connection lifetime and never read from VGI request bytes. */
    peerIdentityProviders?: readonly PeerIdentityProvider[];
    /** Combine connection evidence with anonymous application authentication. */
    peerAuthenticationPolicy?: PeerAuthenticationPolicy;
    /** Logical service destination supplied to destination-aware providers. */
    peerServiceName?: string;
    /** Total provider-resolution budget per accepted connection. Default: 1000 ms. */
    identityResolutionTimeoutMs?: number;
    /** Maximum provider calls that may remain active after connection deadlines. Default: 64. */
    peerProviderConcurrency?: number;
}
/** Handle returned by {@link serveTcp} for callers that want to stop the server. */
export interface ServeTcpHandle {
    /** Host the server is listening on. */
    readonly host: string;
    /** Actual bound TCP port (resolved from `0` when the OS auto-selects). */
    readonly port: number;
    /** Shut down the listener. */
    stop(): Promise<void>;
    /** Promise that resolves when the server has stopped (idle timeout, stop(),
     *  or a fatal error).  Mirrors Python's blocking `serve()` return. */
    readonly done: Promise<void>;
}
/**
 * Bind a TCP socket and serve `protocol` over per-connection IPC streams.
 *
 * The network analog of {@link serveUnix}: same raw Arrow-IPC framing, only
 * the listening socket differs.  Nagle's algorithm is disabled
 * (`setNoDelay(true)`) on each connection so the lockstep request/response
 * framing is not delayed waiting to coalesce writes.
 *
 * SECURITY: no authentication or TLS — trusted networks only; the default
 * host is loopback (`127.0.0.1`).  Use the HTTP transport for untrusted
 * networks.
 */
export declare function serveTcp(protocol: Protocol, options?: ServeTcpOptions): Promise<ServeTcpHandle>;
//# sourceMappingURL=serve-tcp.d.ts.map