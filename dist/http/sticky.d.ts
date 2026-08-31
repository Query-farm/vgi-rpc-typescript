/** Seal a sticky-session token. Returns the base64url-encoded value for the
 *  `VGI-Session` header. */
export declare function sealSessionToken(serverId: string, sessionId: Uint8Array, expiresAt: number, tokenKey: Uint8Array, aad: Uint8Array, now?: number): string;
export interface OpenedSessionToken {
    serverId: string;
    sessionId: Uint8Array;
    expiresAt: number;
}
/** Open a sticky-session token. Raises {@link SessionLostError} on any failure
 *  — wrong AAD (cross-principal replay) is indistinguishable from garbage. */
export declare function openSessionToken(token: string, tokenKey: Uint8Array, aad: Uint8Array): OpenedSessionToken;
/** Minimal promise-based mutex. The HTTP handler awaits `acquire()` before
 *  dispatching on a resumed session and calls the returned release in a
 *  `finally` so concurrent calls on the same session run sequentially. */
declare class AsyncMutex {
    private locked;
    private waiters;
    acquire(): Promise<() => void>;
    private release;
}
/** A live session in the per-worker registry. */
export interface SessionEntry {
    state: unknown;
    expiresAt: number;
    principalKey: string;
    lock: AsyncMutex;
}
/**
 * Derive the registry partition key for a request principal.
 *
 * Both the dispatch path and the `DELETE /__session__` teardown path MUST
 * compute this identically — otherwise a session opened on one path can't
 * be looked up on the other. The NUL separator (rather than a space)
 * keeps `{domain:"a", principal:"b "}` from colliding with
 * `{domain:"a ", principal:"b"}`. Anonymous requests collapse to a
 * single sentinel.
 *
 * `domain` / `principal` are the authenticated identity fields, or
 * null/undefined for anonymous.
 */
export declare function sessionPrincipalKey(authenticated: boolean, domain: string | null | undefined, principal: string | null | undefined, evidenceBinding?: string): string;
/** Hex-encode a session_id Uint8Array (24-char lowercase hex). */
export declare function sessionIdHex(sessionId: Uint8Array): string;
/** Per-worker in-process map of live sticky sessions. */
export declare class SessionRegistry {
    readonly defaultTtl: number;
    private entries;
    private _draining;
    constructor(defaultTtl: number);
    get draining(): boolean;
    setDraining(value: boolean): void;
    /** Register a session. Throws {@link ServerDrainingError} when draining. */
    open(state: unknown, ttl: number | undefined, principalKey: string): {
        sessionId: Uint8Array;
        expiresAt: number;
    };
    /** Look up a session. Returns null on miss, expiry, or principal mismatch.
     *  Expired entries are evicted in-line (and `state.close?.()` invoked). */
    get(sessionId: Uint8Array, principalKey: string): SessionEntry | null;
    /** Remove a session and invoke `state.close?.()`. Returns true on hit. */
    close(sessionId: Uint8Array): boolean;
    /** Evict every entry past its TTL. Returns the eviction count. */
    drainExpired(now?: number): number;
    /** Invoke `state.close?.()` on every live session and clear the registry. */
    shutdown(): void;
    get size(): number;
}
/** Start a periodic reaper that evicts expired sessions. Returns a stop fn.
 *  Uses `setInterval().unref()` where available so the reaper does not keep
 *  the process alive. */
export declare function startSessionReaper(registry: SessionRegistry, tickMs?: number): () => void;
/** Per-request handle that the HTTP handler installs on the OutputCollector.
 *  `CallContext.openSession` / `closeSession` / `session` read and mutate
 *  this object; the handler then emits the resulting headers on the
 *  response. */
export interface StickySink {
    /** True iff the request carried `VGI-Session-Accept: true`. */
    acceptOpens: boolean;
    /** Live session state (resumed or just-opened). Null until `openSession`
     *  or a successful resume populates it. */
    state: unknown | null;
    /** Hex session_id for the access log. Populated on resume + open;
     *  preserved across `closeSession` so close records still carry the id. */
    sessionId: string | null;
    /** Set by `openSession` so `process_response` emits `VGI-Session: <token>`. */
    mintToken: string | null;
    /** Set by `closeSession` so `process_response` emits `VGI-Session-Close: true`. */
    closed: boolean;
    /** Sticky-session lifecycle action observed during dispatch — one of
     *  "none" / "resume" / "open" / "close". Surfaced on the access log. */
    action: "none" | "resume" | "open" | "close";
    /** Bound by the handler: registers `state` in the registry, mints the
     *  AEAD-sealed token, and stamps `mintToken` + `sessionId`. */
    _open(state: unknown, ttl: number | undefined): void;
    /** Bound by the handler: removes the registry entry + invokes
     *  `state.close?.()`. Idempotent. */
    _close(): void;
}
/** Build a `StickySink` for a request without sticky support — `_open` /
 *  `_close` throw the same RuntimeError shape as Python's implementation so
 *  call sites get a clear message. */
export declare function unavailableStickySink(): StickySink;
/** Operator-facing handle returned by `createHttpHandler` (when sticky is
 *  enabled) so SIGTERM hooks / worker-exit hooks can drain in-flight
 *  sessions cleanly. Mirrors Python's `DrainHandle`. */
export interface DrainHandle {
    /** Flip the registry's drain flag — subsequent `ctx.openSession` raises
     *  {@link ServerDrainingError}. Existing sessions continue. */
    drain(): void;
    /** Invoke `state.close?.()` on every live session and clear the registry. */
    shutdown(): void;
    /** Return whether `drain()` has been invoked. */
    isDraining(): boolean;
    /** Test-only / advanced: flip the drain flag back. Production deployments
     *  only ever drain in one direction; conformance tests use this to clean
     *  up the fixture between tests. */
    setDraining(value: boolean): void;
}
/** Build a {@link DrainHandle} for *registry*. `stopReaper`, when supplied,
 *  is invoked by `shutdown()` so the periodic reaper interval is cleared and
 *  the handle fully releases its resources. */
export declare function makeDrainHandle(registry: SessionRegistry, stopReaper?: () => void): DrainHandle;
export {};
//# sourceMappingURL=sticky.d.ts.map