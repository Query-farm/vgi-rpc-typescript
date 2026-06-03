import type { ExternalLocationConfig } from "./external.js";
import type { Protocol } from "./protocol.js";
import { type DispatchHook, type ServeStartHook } from "./types.js";
/**
 * RPC server that reads Arrow IPC requests from stdin and writes responses to stdout.
 * Supports unary and streaming (producer/exchange) methods.
 */
export declare class VgiRpcServer {
    private protocol;
    private enableDescribe;
    private serverId;
    private _describePromise;
    private protocolVersion;
    private dispatchHook;
    private externalConfig;
    private onServeStart;
    /** True once the on_serve_start hook has fired successfully. The bind
     *  state is committed only after the hook returns, so a transient
     *  failure on first request leaves it `false` and the next request
     *  re-fires rather than silently skipping. Mirrors Python 7b3999c. */
    private serveStartFired;
    constructor(protocol: Protocol, options?: {
        enableDescribe?: boolean;
        serverId?: string;
        dispatchHook?: DispatchHook;
        externalLocation?: ExternalLocationConfig;
        protocolVersion?: string;
        /** Lifecycle hook fired once before the first dispatched request. */
        onServeStart?: ServeStartHook;
    });
    /** Fire the on_serve_start hook once for this transport. Idempotent
     *  on success — re-throws on failure without committing the bind. */
    private notifyTransport;
    /** Build (or retrieve cached) describe batch + protocol hash. */
    private describeInfo;
    /** Validate a client's declared protocol_version against the Protocol's
     *  declared version. Caller invokes only when
     *  `protocol.protocolVersionParts` is non-null. Mirrors Python's
     *  `RpcServer._check_protocol_version`: exact major+minor match, patch
     *  ignored; directional error message names which side is older. */
    private checkProtocolVersion;
    /** Start the server loop. Reads requests until stdin closes. */
    run(): Promise<void>;
    private serveOne;
}
//# sourceMappingURL=server.d.ts.map