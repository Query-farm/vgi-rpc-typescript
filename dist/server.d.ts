import { type ProtocolBinding } from "./binding.js";
import type { ExternalLocationConfig } from "./external.js";
import type { Protocol } from "./protocol.js";
import { type IdentityImpl } from "./token-identity.js";
import { type DispatchHook, type MethodDefinition, type ServeStartHook, TransportKind } from "./types.js";
import { type ByteSink } from "./wire/writer.js";
/**
 * RPC server that reads Arrow IPC requests from stdin and writes responses to stdout.
 * Supports unary and streaming (producer/exchange) methods.
 */
export declare class VgiRpcServer {
    private protocol;
    private serverId;
    private protocolVersion;
    private dispatchHook;
    private externalConfig;
    private onServeStart;
    /** True once the on_serve_start hook has fired successfully. The bind
     *  state is committed only after the hook returns, so a transient
     *  failure on first request leaves it `false` and the next request
     *  re-fires rather than silently skipping. Mirrors Python 7b3999c. */
    private serveStartFired;
    /** Protocols hosted beyond the primary, keyed by wire name.
     *
     *  The primary stays in `protocol` so every existing path is untouched; it is
     *  projected into a binding on demand by {@link bindings}. */
    private extraBindings;
    constructor(protocol: Protocol, options?: {
        /** Host `vgi_rpc.Reflection.v1`. Default `true`.
         *
         *  Named for the `__describe__` method it used to switch on, and kept
         *  under that name across the fleet: what it gates is introspection,
         *  and introspection is now a co-hosted protocol rather than a reserved
         *  method answered before dispatch. */
        enableDescribe?: boolean;
        /** Opaque per-process server identifier surfaced to clients and the landing page. */
        serverId?: string;
        /** Hook invoked around each dispatched request (tracing/metrics/auth enrichment). */
        dispatchHook?: DispatchHook;
        /** Configuration for externalizing oversized record batches to blob storage. */
        externalLocation?: ExternalLocationConfig;
        /** Protocol version string reported in the service description. */
        protocolVersion?: string;
        /** Lifecycle hook fired once before the first dispatched request. */
        onServeStart?: ServeStartHook;
    });
    /** Every protocol this server hosts, primary first.
     *
     *  The primary is projected from the server's own protocol rather than
     *  stored, so the existing registration paths keep working untouched. */
    bindings(): Map<string, ProtocolBinding>;
    /** Host `vgi_rpc.Reflection.v1` on this server.
     *
     *  Registered after the application protocol so it appears in its own output
     *  without being special-cased, and so the primary stays the application
     *  protocol -- which is what the single-protocol accessors report.
     *
     *  The binding is version-exempt: this is the protocol a version-mismatched
     *  client calls to learn *what* mismatched, and gating it would deny the
     *  client the diagnosis it came for.
     *
     *  Idempotent, because the constructor already calls it unless the
     *  deployment opted out: an explicit second call should not turn a working
     *  server into a duplicate-name error. */
    registerReflection(): void;
    /** Host `vgi_rpc.Identity.v1` on this server, when the deployment configured
     *  it.
     *
     *  Call this *after* {@link registerReflection} so identity appears in
     *  reflection's output. (`listBindings` is read at request time here, so the
     *  order is a convention rather than a mechanism -- but it is the convention
     *  every port follows, and a port that later caches the listing would break
     *  silently without it.)
     *
     *  Only the methods whose hooks the deployment supplied are hosted, so the
     *  binding's `protocol_hash` narrows with them: a method this worker cannot
     *  answer is better *absent* than routed-and-refusing, because then what the
     *  server hosts describes what it actually does and a client learns it from
     *  reflection rather than by calling and reading an error. With neither hook
     *  configured nothing is registered at all -- which is what keeps a
     *  dependency upgrade from growing a credential-to-identity oracle on every
     *  existing worker.
     *
     *  Not version-exempt: the binding declares no `protocolVersion`, so the gate
     *  never fires, and exempting it would be a claim rather than a fact. */
    registerIdentity(identity: IdentityImpl): void;
    /** Host an additional protocol alongside the primary.
     *
     *  `allowReserved` is for the framework's own protocols only; an application
     *  passing `true` would be able to shadow reflection. */
    addProtocol(binding: ProtocolBinding, allowReserved?: boolean): void;
    /** Resolve one request's (protocol, method) pair.
     *
     *  The routing key is required, including against a server hosting exactly
     *  one protocol: an exemption would let an intermediary that rebuilds a
     *  request and drops the field land silently on whichever protocol happened
     *  to be first, rather than being told.
     *
     *  The three failures are deliberately distinct, and a client depends on the
     *  difference -- particularly the last, which is the documented
     *  capability-probe signal: a client testing for an optional method must be
     *  able to tell "you do not speak this protocol" from "you speak it but lack
     *  this method". */
    resolve(protocol: string, method: string): {
        /** The resolved method definition. */
        method: MethodDefinition;
        /** The binding that owns it -- the source of the access record's identity. */
        binding: ProtocolBinding;
    };
    /** Fire the on_serve_start hook once for this transport. Idempotent
     *  on success — re-throws on failure without committing the bind. */
    private notifyTransport;
    /** Validate a client's declared protocol_version against the Protocol's
     *  declared version. Caller invokes only when
     *  `protocol.protocolVersionParts` is non-null. Mirrors Python's
     *  `RpcServer._check_protocol_version`: exact major+minor match, patch
     *  ignored; directional error message names which side is older. */
    private checkProtocolVersion;
    /** Start the server loop over stdin/stdout. Reads requests until stdin closes. */
    run(): Promise<void>;
    /**
     * Serve requests over an explicit byte-stream pair until the readable ends —
     * the transport-agnostic core that {@link run} (stdin/stdout) is built on.
     *
     * Use this to serve over any duplex channel that the stdio/unix/tcp helpers
     * don't cover: a Web Worker / `MessagePort` bridge, an in-memory pipe, or a
     * pre-connected socket. The loop, on_serve_start firing, and EOF/broken-pipe
     * handling are identical to {@link run}.
     *
     * @param readable incoming request bytes — a web `ReadableStream<Uint8Array>`
     *   or a Node `Readable` (e.g. a `Duplex` bridging a MessagePort).
     * @param writable outgoing response sink — a stdout-like fd number, or a
     *   `net.Socket` / structurally-compatible `Duplex`. Omit for the stdout fd.
     * @param transportKind reported to the `on_serve_start` hook (default `PIPE`).
     */
    serveConnection(readable: ReadableStream<Uint8Array> | NodeJS.ReadableStream, writable?: number | import("node:net").Socket | ByteSink, transportKind?: TransportKind): Promise<void>;
    private serveOne;
}
//# sourceMappingURL=server.d.ts.map