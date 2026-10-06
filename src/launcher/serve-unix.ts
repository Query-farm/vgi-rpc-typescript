// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * AF_UNIX worker runner for vgi-rpc TypeScript.
 *
 * Bind a deterministic Unix-domain socket, accept connections one at a
 * time (sequential listen, matching Python's `serve_unix`), and dispatch
 * each via the existing {@link VgiRpcServer.serveOne} loop.  Implements
 * the cross-language launcher contract:
 *
 * - Accept `--unix PATH` and `--idle-timeout SEC` (parsed by callers).
 * - Emit `UNIX:<absolute-path>\n` to stdout once bind+listen succeed.
 * - Self-terminate after `idleTimeout` seconds with zero connected
 *   clients; the timer starts ticking only after a `startupGrace`
 *   window so a slow first caller doesn't see the server vanish.
 */

import { existsSync, unlinkSync } from "node:fs";
import { createServer, type Server, type Socket } from "node:net";
import * as path from "node:path";
import type { ExternalLocationConfig } from "../external.js";
import type { Protocol } from "../protocol.js";
import { isConnectionClosed, type VgiRpcServer } from "../server.js";
import { type DispatchHook, type ServeStartHook, TransportKind } from "../types.js";
import { IpcStreamReader } from "../wire/reader.js";
import { IpcStreamWriter } from "../wire/writer.js";
import { launcherHost } from "./host.js";

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
  /** Host `vgi_rpc.Reflection.v1`. Default: true.
   *
   *  Named for the `__describe__` method it used to switch on. What it gates
   *  is introspection, which is now a co-hosted protocol rather than a
   *  reserved method answered before dispatch. */
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
 * Bind an AF_UNIX socket and serve `target` over per-connection IPC streams.
 *
 * `target` is a bare {@link Protocol}, or a {@link VgiRpcServer} carrying
 * additional protocols (and identity) -- the same host object every other
 * transport accepts, so all of them serve one hosted set. With a server, the
 * server-level options here (`serverId`, `protocolVersion`, `enableDescribe`,
 * `dispatchHook`, `externalLocation`, `onServeStart`) must be left unset.
 *
 * Sequential listen — one client at a time, just like Python's `serve_unix`.
 * Each connection gets its own dispatch loop and shares the protocol.
 */
export async function serveUnix(target: Protocol | VgiRpcServer, options: ServeUnixOptions): Promise<ServeUnixHandle> {
  const sockPath = path.resolve(options.unixPath);
  const idleTimeoutS = options.idleTimeout ?? 300;
  const startupGraceS = options.startupGraceSeconds ?? 5;
  const backlog = options.backlog ?? 16;
  const announcementSink = options.announcementSink ?? process.stdout;

  // Defensive probe-then-bind: an existing live worker on this path means a
  // peer launcher already won the race.  Refuse to bind so we don't take
  // its connections.  (A stale path with no listener was unlinked by the
  // launcher before spawning us, but a leftover from a co-launcher right
  // now is possible.)
  if (existsSync(sockPath)) {
    try {
      // Best-effort cleanup; bind below will surface any real conflict.
      unlinkSync(sockPath);
    } catch {
      // ignore — let listen() fail with EADDRINUSE.
    }
  }

  // One protocol host, shared with every other transport this server is
  // handed to: routing, the version gate, error encoding and the dispatch hook
  // all run in `VgiRpcServer.serveRequest`, so this launcher owns only its
  // socket lifecycle.
  const host = launcherHost(target, options, "serveUnix");
  host.seal();

  const server: Server = createServer({ allowHalfOpen: false });

  let activeConnections = 0;
  let idleTimer: ReturnType<typeof setTimeout> | null = null;
  let resolveDone: () => void = () => {};
  let rejectDone: (err: unknown) => void = () => {};
  const done = new Promise<void>((resolve, reject) => {
    resolveDone = resolve;
    rejectDone = reject;
  });
  let stopped = false;

  function armIdleTimer(): void {
    if (idleTimeoutS <= 0) return;
    if (idleTimer) clearTimeout(idleTimer);
    idleTimer = setTimeout(() => {
      if (activeConnections === 0 && !stopped) {
        void shutdown();
      }
    }, idleTimeoutS * 1000);
  }

  function disarmIdleTimer(): void {
    if (idleTimer) {
      clearTimeout(idleTimer);
      idleTimer = null;
    }
  }

  async function shutdown(): Promise<void> {
    if (stopped) return;
    stopped = true;
    disarmIdleTimer();
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
    });
    try {
      unlinkSync(sockPath);
    } catch {
      // already gone
    }
    resolveDone();
  }

  server.on("connection", (socket) => {
    activeConnections += 1;
    disarmIdleTimer();
    handleConnection(socket)
      .catch((err) => {
        // Per-connection errors must not take down the server — log to stderr
        // and let the next connection proceed.
        process.stderr.write(`vgi-rpc/unix: connection failed: ${(err as Error)?.message ?? err}\n`);
      })
      .finally(() => {
        activeConnections -= 1;
        socket.destroy();
        if (activeConnections === 0 && !stopped) {
          armIdleTimer();
        }
      });
  });

  server.on("error", (err) => {
    if (stopped) return;
    rejectDone(err);
  });

  async function handleConnection(socket: Socket): Promise<void> {
    // The reader takes any Node Readable; sockets are duplex Readables.
    const reader = await IpcStreamReader.create(socket);
    // Build the writer over the Socket itself, not its raw fd. AF_UNIX
    // sockets in Node are non-blocking; a fd-based writer would do
    // `fs.writeSync` and busy-wait on EAGAIN whenever the ~8 KB kernel send
    // buffer fills (trivial for any Arrow batch of meaningful size). That
    // synchronous wait freezes the shared event loop and starves every
    // *other* connection's handler — observed as 30 s `catalog_attach`
    // timeouts from co-running unittest processes. Going through
    // `socket.write` + `'drain'` lets the JS thread yield while the kernel
    // drains the buffer.
    const writer = new IpcStreamWriter(socket);

    try {
      // Fire on_serve_start lazily — first request retries on hook failure.
      await host.notifyTransport(TransportKind.UNIX);

      while (true) {
        try {
          await host.serveRequest(reader, writer, TransportKind.UNIX);
        } catch (e: unknown) {
          // EOF/closed client → end this connection cleanly.
          if (isConnectionClosed(e)) return;
          throw e;
        }
      }
    } finally {
      try {
        await reader.cancel();
      } catch {
        // already closed
      }
    }
  }

  // bind + listen
  await new Promise<void>((resolve, reject) => {
    server.listen({ path: sockPath, backlog }, () => resolve());
    server.once("error", (err) => reject(err));
  });

  // Set a tight mode on the bound socket so peers from other UIDs can't
  // even initiate a connection.
  try {
    const { chmodSync } = await import("node:fs");
    chmodSync(sockPath, 0o600);
  } catch {
    // best-effort — operator-managed dirs may already be 0700
  }

  options.onBound?.(sockPath);
  // Cross-language launcher contract: announce on stdout.
  announcementSink.write(`UNIX:${sockPath}\n`);

  // Start the idle timer with a startup grace window.
  if (idleTimeoutS > 0) {
    setTimeout(() => {
      if (activeConnections === 0 && !stopped) armIdleTimer();
    }, startupGraceS * 1000).unref?.();
  }

  return {
    socketPath: sockPath,
    stop: shutdown,
    done,
  };
}
