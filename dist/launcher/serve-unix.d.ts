import type { ExternalLocationConfig } from "../external.js";
import type { Protocol } from "../protocol.js";
import { type DispatchHook, type ServeStartHook } from "../types.js";
/** Configuration for {@link serveUnix}. */
export interface ServeUnixOptions {
    /** Absolute path to the Unix socket file the worker should bind. */
    unixPath: string;
    /** Self-terminate after this many seconds with zero connected clients.
     *  Default: 300.  `0` disables the timer (server runs until killed). */
    idleTimeout?: number;
    /** Grace period after `listen()` succeeds before the idle timer starts
     *  ticking.  Default: 5 — gives the first launcher caller a chance to
     *  connect after the `UNIX:<path>` announcement. */
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
    /** Maximum sequential listen backlog. Mirrors Python's `serve_unix`
     *  (`backlog=16`).  Default: 16. */
    backlog?: number;
    /** Called *after* `listen()` returns successfully but *before*
     *  `UNIX:<path>` is printed.  The launcher uses this hook to write the
     *  announcement only after we're sure the bind took. */
    onBound?: (sockPath: string) => void;
    /** Override the stream used for the `UNIX:<path>` line.  Defaults to
     *  `process.stdout`. */
    announcementSink?: NodeJS.WritableStream;
}
/** Handle returned by {@link serveUnix} for callers that want to stop the server. */
export interface ServeUnixHandle {
    /** Absolute path of the bound AF_UNIX socket the server is listening on. */
    readonly socketPath: string;
    /** Shut down the listener and unlink the socket file. */
    stop(): Promise<void>;
    /** Promise that resolves when the server has stopped (idle timeout, stop(),
     *  or a fatal error).  Mirrors Python's blocking `serve()` return. */
    readonly done: Promise<void>;
}
/**
 * Bind an AF_UNIX socket and serve `protocol` over per-connection IPC streams.
 *
 * Sequential listen — one client at a time, just like Python's `serve_unix`.
 * Each connection gets its own dispatch loop and shares the protocol.
 */
export declare function serveUnix(protocol: Protocol, options: ServeUnixOptions): Promise<ServeUnixHandle>;
//# sourceMappingURL=serve-unix.d.ts.map