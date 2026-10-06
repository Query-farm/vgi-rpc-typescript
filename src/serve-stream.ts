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
import { VgiRpcServer, type VgiRpcServerOptions } from "./server.js";
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
  /** Passed through to the `VgiRpcServer` constructor (describe, hooks, …)
   *  when `target` is a bare `Protocol`. Refused alongside a built server:
   *  the options belong on that server. */
  serverOptions?: VgiRpcServerOptions;
  /** Reported to the `on_serve_start` hook. Defaults to `PIPE`. */
  transportKind?: TransportKind;
}

/**
 * Serve `target` over the provided `readable`/`writable` until the readable
 * ends. Thin wrapper over {@link VgiRpcServer.serveConnection}. Resolves on
 * clean EOF; rejects on a real protocol/transport error.
 *
 * `target` is a bare {@link Protocol}, or a {@link VgiRpcServer} carrying
 * additional protocols -- the same host object every other transport accepts.
 */
export async function serveStream(target: Protocol | VgiRpcServer, options: ServeStreamOptions): Promise<void> {
  let server: VgiRpcServer;
  if (target instanceof VgiRpcServer) {
    if (options.serverOptions !== undefined) {
      throw new TypeError(
        "serveStream was given a VgiRpcServer together with serverOptions. Pass them to the VgiRpcServer " +
          "constructor instead, so every transport serving it agrees.",
      );
    }
    server = target;
  } else {
    server = new VgiRpcServer(target, options.serverOptions);
  }
  await server.serveConnection(options.readable, options.writable, options.transportKind);
}
