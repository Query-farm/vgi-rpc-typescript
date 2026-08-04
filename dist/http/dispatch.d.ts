import type { AuthContext } from "../auth.js";
import { type ExternalLocationConfig } from "../external.js";
import type { MethodDefinition } from "../types.js";
import { TransportKind } from "../types.js";
import type { StateSerializer } from "./types.js";
export interface DispatchContext {
    tokenKey: Uint8Array;
    tokenTtl: number;
    serverId: string;
    /** Producer-only soft wire-cap (deprecated alias for the producer-loop
     *  byte budget). Unary/exchange ignore this. */
    maxStreamResponseBytes?: number;
    /** Soft wire-cap for producer streams; hard wire-cap for unary/exchange.
     *  Externalised payloads do not count toward this. */
    maxResponseBytes?: number;
    /** Hard cap on bytes uploaded to external storage during one HTTP response. */
    maxExternalizedResponseBytes?: number;
    /** Entries the resolved-call cache may hold; `0` disables it so every
     *  continuation re-opens the call token the client echoed. Default 4096. */
    callStateCacheEntries?: number;
    stateSerializer: StateSerializer;
    authContext?: AuthContext;
    externalLocation?: ExternalLocationConfig;
    /** Incoming HTTP request cookies.  Empty/absent on non-HTTP paths. */
    cookies?: ReadonlyMap<string, string>;
    /** Transport identifier surfaced to handlers via CallContext.kind.
     *  Defaults to HTTP when unset (the only caller that overrides it is
     *  the AF_UNIX launcher path). */
    kind?: TransportKind;
    /** Per-request sticky-session sink. Installed by the handler when sticky
     *  is enabled and the dispatcher attaches it to the OutputCollector so
     *  `ctx.session` / `ctx.openSession` / `ctx.closeSession` work. */
    stickyContext?: import("../types.js").StickyContext;
    /** Per-request externalisation tally, surfaced on the access log as
     *  `externalized_bytes`. Those bytes never touch the HTTP body — only a
     *  pointer batch does — so nothing measured at the transport can see them. */
    egress?: {
        externalizedBytes: number;
    };
}
/** Dispatch a __describe__ request. */
export declare function httpDispatchDescribe(protocolName: string, methods: Map<string, MethodDefinition>, serverId: string, protocolVersion?: string): Promise<Response>;
/** Dispatch a unary HTTP request. */
export declare function httpDispatchUnary(method: MethodDefinition, body: Uint8Array, ctx: DispatchContext): Promise<Response>;
/** Dispatch a stream init HTTP request (producer or exchange). */
export declare function httpDispatchStreamInit(method: MethodDefinition, body: Uint8Array, ctx: DispatchContext): Promise<Response>;
/** Dispatch a stream exchange HTTP request (producer continuation or exchange round). */
export declare function httpDispatchStreamExchange(method: MethodDefinition, body: Uint8Array, ctx: DispatchContext): Promise<Response>;
//# sourceMappingURL=dispatch.d.ts.map