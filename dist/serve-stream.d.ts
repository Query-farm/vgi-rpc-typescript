import type { Socket } from "node:net";
import type { Protocol } from "./protocol.js";
import { VgiRpcServer } from "./server.js";
import type { TransportKind } from "./types.js";
import type { ByteSink } from "./wire/writer.js";
/** Options for {@link serveStream} — a single RPC session over one stream pair. */
export interface ServeStreamOptions {
    /** Incoming request bytes — a web `ReadableStream<Uint8Array>` or a Node
     *  `Readable` (e.g. a `Duplex` bridging a MessagePort). */
    readable: ReadableStream<Uint8Array> | NodeJS.ReadableStream;
    /** Outgoing response sink — a stdout-like fd number, or a `net.Socket` /
     *  structurally-compatible `Duplex`. Omit for the stdout fd. */
    writable?: number | Socket | ByteSink;
    /** Passed through to the `VgiRpcServer` constructor (describe, hooks, …). */
    serverOptions?: ConstructorParameters<typeof VgiRpcServer>[1];
    /** Reported to the `on_serve_start` hook. Defaults to `PIPE`. */
    transportKind?: TransportKind;
}
/**
 * Serve `protocol` over the provided `readable`/`writable` until the readable
 * ends. Thin wrapper over {@link VgiRpcServer.serveConnection}. Resolves on
 * clean EOF; rejects on a real protocol/transport error.
 */
export declare function serveStream(protocol: Protocol, options: ServeStreamOptions): Promise<void>;
//# sourceMappingURL=serve-stream.d.ts.map