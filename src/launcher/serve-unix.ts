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
import { schema as makeSchema, serializeBatch } from "../arrow/index.js";
import type { ProtocolBinding } from "../binding.js";
import { dispatchStream } from "../dispatch/stream.js";
import { dispatchUnary } from "../dispatch/unary.js";
import { RpcError, VersionError } from "../errors.js";
import type { ExternalLocationConfig } from "../external.js";
import type { Protocol } from "../protocol.js";
import { protocolHashFor } from "../reflection.js";
import { VgiRpcServer } from "../server.js";
import {
  type CallStatistics,
  type DispatchHook,
  type DispatchInfo,
  type MethodDefinition,
  MethodType,
  type ServeStartHook,
  TransportKind,
} from "../types.js";
import { IpcStreamReader } from "../wire/reader.js";
import { applyDefaults, parseRequest, validateRequestSchema } from "../wire/request.js";
import { buildErrorBatch } from "../wire/response.js";
import { IpcStreamWriter } from "../wire/writer.js";

const EMPTY_SCHEMA = makeSchema([]);

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
 * Bind an AF_UNIX socket and serve `protocol` over per-connection IPC streams.
 *
 * Sequential listen — one client at a time, just like Python's `serve_unix`.
 * Each connection gets its own dispatch loop and shares the protocol.
 */
export async function serveUnix(protocol: Protocol, options: ServeUnixOptions): Promise<ServeUnixHandle> {
  const sockPath = path.resolve(options.unixPath);
  const idleTimeoutS = options.idleTimeout ?? 300;
  const startupGraceS = options.startupGraceSeconds ?? 5;
  const protocolVersion = options.protocolVersion ?? "";
  const serverId = options.serverId ?? crypto.randomUUID().replace(/-/g, "").slice(0, 12);
  const dispatchHook = options.dispatchHook ?? null;
  const externalConfig = options.externalLocation;
  const onServeStart = options.onServeStart ?? null;
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

  // One routing host, so this transport resolves `(protocol, method)` the
  // same way the stdio server does -- and hosts `vgi_rpc.Reflection.v1`,
  // which is what a client bootstraps from now that `__describe__` is gone.
  // Only the binding table and the resolver are used; the dispatch loop
  // below stays this transport's own, because its framing is.
  const host = new VgiRpcServer(protocol, {
    serverId,
    protocolVersion,
    enableDescribe: options.enableDescribe ?? true,
  });

  // Lifecycle: only commit `serveStartFired` after the hook returns successfully.
  let serveStartFired = false;
  let serveStartInFlight: Promise<void> | null = null;
  async function notifyTransport(): Promise<void> {
    if (serveStartFired) return;
    if (serveStartInFlight) {
      await serveStartInFlight;
      return;
    }
    if (!onServeStart) {
      serveStartFired = true;
      return;
    }
    const attempt = Promise.resolve().then(() => onServeStart(TransportKind.UNIX));
    serveStartInFlight = attempt;
    try {
      await attempt;
      serveStartFired = true;
    } finally {
      if (serveStartInFlight === attempt) serveStartInFlight = null;
    }
  }

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
      await notifyTransport();

      while (true) {
        try {
          await serveOnce(reader, writer);
        } catch (e: unknown) {
          const err = e as { code?: string; message?: string };
          // EOF/closed client → end this connection cleanly.
          if (
            err?.message?.includes("closed") ||
            err?.message?.includes("Expected Schema Message") ||
            err?.message?.includes("null or length 0") ||
            err?.message?.includes("EOF") ||
            err?.code === "EPIPE" ||
            err?.code === "ERR_STREAM_PREMATURE_CLOSE" ||
            err?.code === "ERR_STREAM_DESTROYED"
          ) {
            return;
          }
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

  async function serveOnce(reader: IpcStreamReader, writer: IpcStreamWriter): Promise<void> {
    const stream = await reader.readStream();
    if (!stream) {
      throw new Error("EOF");
    }
    const { schema, batches } = stream;
    if (batches.length === 0) {
      const err = new RpcError("ProtocolError", "Request stream contains no batches", "");
      const errBatch = buildErrorBatch(EMPTY_SCHEMA, err, serverId, null);
      await writer.writeStream(EMPTY_SCHEMA, [errBatch]);
      return;
    }
    const batch = batches[0];
    let methodName: string;
    let protocolName: string;
    let params: Record<string, unknown>;
    let requestId: string | null;
    try {
      const parsed = parseRequest(schema, batch);
      methodName = parsed.methodName;
      protocolName = parsed.protocol;
      params = parsed.params;
      requestId = parsed.requestId;
    } catch (e: unknown) {
      const errBatch = buildErrorBatch(EMPTY_SCHEMA, e as Error, serverId, null);
      await writer.writeStream(EMPTY_SCHEMA, [errBatch]);
      if (e instanceof VersionError || e instanceof RpcError) return;
      throw e;
    }

    // Resolve (protocol, method). The routing key is part of the lookup rather
    // than a label on it: method names may collide across protocols, and a
    // retired `__describe__` is refused here with a message naming where
    // introspection went.
    let method: MethodDefinition;
    let binding: ProtocolBinding;
    try {
      ({ method, binding } = host.resolve(protocolName, methodName));
    } catch (error) {
      const errBatch = buildErrorBatch(EMPTY_SCHEMA, error as Error, serverId, requestId);
      await writer.writeStream(EMPTY_SCHEMA, [errBatch]);
      return;
    }

    try {
      validateRequestSchema(schema, method.paramsSchema, methodName);
    } catch (error) {
      const errSchema = method.type === MethodType.UNARY ? method.resultSchema : EMPTY_SCHEMA;
      await writer.writeStream(errSchema, [buildErrorBatch(errSchema, error as Error, serverId, requestId)]);
      return;
    }

    const methodType = method.type === MethodType.UNARY ? "unary" : "stream";
    let requestData: Uint8Array | undefined;
    try {
      requestData = serializeBatch(batch);
    } catch {
      // best-effort
    }
    const info: DispatchInfo = {
      method: methodName,
      methodType,
      serverId,
      requestId,
      // The protocol that owns the dispatched method, and *its* canonical
      // digest. Both from the resolved binding, never the server's primary:
      // `protocol_hash` is the registry key for decoding an archived record,
      // so a record naming one protocol while carrying another's is decoded
      // against the wrong description -- and passes the schema while doing it.
      // Read inline rather than through a local so the pairing is visible at
      // the emit site, which is what `test/dispatch-identity.test.ts` checks.
      protocol: binding.name,
      protocolHash: await protocolHashFor(binding),
      protocolVersion,
      kind: TransportKind.UNIX,
      principal: "",
      authDomain: "",
      authenticated: false,
      remoteAddr: "",
      requestData,
    };
    const stats: CallStatistics = {
      inputBatches: 0,
      outputBatches: 0,
      inputRows: 0,
      outputRows: 0,
      inputBytes: 0,
      outputBytes: 0,
    };

    const token = dispatchHook?.onDispatchStart(info);
    let dispatchError: Error | undefined;
    applyDefaults(params, method.defaults);
    try {
      if (method.type === MethodType.UNARY) {
        await dispatchUnary(method, params, writer, serverId, requestId, externalConfig, TransportKind.UNIX);
      } else {
        await dispatchStream(method, params, writer, reader, serverId, requestId, externalConfig, TransportKind.UNIX);
      }
    } catch (e) {
      dispatchError = e instanceof Error ? e : new Error(String(e));
      throw e;
    } finally {
      dispatchHook?.onDispatchEnd(token, info, stats, dispatchError);
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
