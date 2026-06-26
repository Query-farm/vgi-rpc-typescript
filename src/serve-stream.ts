// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

//! Serve a protocol over a caller-provided byte-stream pair — the stream
//! sibling of `serveTcp` / `serveUnix`, with no socket/listener of its own.
//!
//! Useful for transports the launcher helpers don't cover: a Web Worker /
//! `MessagePort` bridge (postMessage), an in-memory pipe, or a pre-connected
//! socket. The host side already has this symmetry via `pipeConnect`.

import type { Socket } from "node:net";

import type { Protocol } from "./protocol.js";
import { VgiRpcServer } from "./server.js";
import { TransportKind } from "./types.js";

export interface ServeStreamOptions {
  /** Incoming request bytes — a web `ReadableStream<Uint8Array>` or a Node
   *  `Readable` (e.g. a `Duplex` bridging a MessagePort). */
  readable: ReadableStream<Uint8Array> | NodeJS.ReadableStream;
  /** Outgoing response sink — a stdout-like fd number, or a `net.Socket` /
   *  structurally-compatible `Duplex`. Omit for the stdout fd. */
  writable?: number | Socket;
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
export async function serveStream(protocol: Protocol, options: ServeStreamOptions): Promise<void> {
  const server = new VgiRpcServer(protocol, options.serverOptions);
  await server.serveConnection(options.readable, options.writable, options.transportKind);
}
