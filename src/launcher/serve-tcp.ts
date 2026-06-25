// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * AF_INET (raw TCP) worker runner for vgi-rpc TypeScript.
 *
 * Bind a TCP socket on `(host, port)`, accept connections, and dispatch
 * each via the same per-connection IPC-stream loop as {@link serveUnix}.
 * This is the network analog of the AF_UNIX runner — identical raw
 * Arrow-IPC framing, only the listening socket differs.  Implements the
 * cross-language launcher contract:
 *
 * - Accept `[HOST:]PORT` (host defaults to `127.0.0.1`, port `0` lets the
 *   OS pick a free port) and `--idle-timeout SEC`.
 * - Emit `TCP:<host>:<port>\n` to stdout once bind+listen succeed, where
 *   `<port>` is the *actual* bound port (resolved when `port=0`).
 * - Self-terminate after `idleTimeout` seconds with zero connected
 *   clients; the timer starts ticking only after a `startupGrace` window
 *   so a slow first caller doesn't see the server vanish.
 *
 * SECURITY: raw TCP carries **no authentication or TLS** — it is the bare
 * framing protocol on a socket.  The default host is loopback-only
 * (`127.0.0.1`).  Binding a routable address (e.g. `0.0.0.0`) exposes the
 * unauthenticated protocol on the network and must only be done on a
 * trusted network; use the HTTP transport otherwise.
 */

import { createServer, type Server, type Socket } from "node:net";
import { schema as makeSchema, serializeBatch } from "../arrow/index.js";
import { DESCRIBE_METHOD_NAME } from "../constants.js";
import { buildDescribeBatch } from "../dispatch/describe.js";
import { dispatchStream } from "../dispatch/stream.js";
import { dispatchUnary } from "../dispatch/unary.js";
import { RpcError, VersionError } from "../errors.js";
import type { ExternalLocationConfig } from "../external.js";
import type { Protocol } from "../protocol.js";
import {
  type CallStatistics,
  type DispatchHook,
  type DispatchInfo,
  MethodType,
  type ServeStartHook,
  TransportKind,
} from "../types.js";
import { IpcStreamReader } from "../wire/reader.js";
import { applyDefaults, parseRequest } from "../wire/request.js";
import { buildErrorBatch } from "../wire/response.js";
import { IpcStreamWriter } from "../wire/writer.js";

const EMPTY_SCHEMA = makeSchema([]);

/** Configuration for {@link serveTcp}. */
export interface ServeTcpOptions {
  /** Interface to bind.  Defaults to `127.0.0.1` (loopback only).  Binding a
   *  routable address exposes the unauthenticated framing on the network. */
  host?: string;
  /** TCP port to bind.  Defaults to `0`, which lets the OS pick a free port
   *  (reported via the `TCP:<host>:<port>` line and {@link onBound}). */
  port?: number;
  /** Self-terminate after this many seconds with zero connected clients.
   *  Default: 300.  `0` disables the timer (server runs until killed). */
  idleTimeout?: number;
  /** Grace period after `listen()` succeeds before the idle timer starts
   *  ticking.  Default: 5 — gives the first launcher caller a chance to
   *  connect after the `TCP:<host>:<port>` announcement. */
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
  /** Maximum listen backlog.  Default: 128. */
  backlog?: number;
  /** Called *after* `listen()` returns successfully but *before*
   *  `TCP:<host>:<port>` is printed.  Invoked with the bound host and the
   *  *actual* bound port (resolved when `port=0`). */
  onBound?: (host: string, port: number) => void;
  /** Override the stream used for the `TCP:<host>:<port>` line.  Defaults to
   *  `process.stdout`. */
  announcementSink?: NodeJS.WritableStream;
}

/** Handle returned by {@link serveTcp} for callers that want to stop the server. */
export interface ServeTcpHandle {
  /** Host the server is listening on. */
  readonly host: string;
  /** Actual bound TCP port (resolved from `0` when the OS auto-selects). */
  readonly port: number;
  /** Shut down the listener. */
  stop(): Promise<void>;
  /** Promise that resolves when the server has stopped (idle timeout, stop(),
   *  or a fatal error).  Mirrors Python's blocking `serve()` return. */
  readonly done: Promise<void>;
}

/**
 * Bind a TCP socket and serve `protocol` over per-connection IPC streams.
 *
 * The network analog of {@link serveUnix}: same raw Arrow-IPC framing, only
 * the listening socket differs.  Nagle's algorithm is disabled
 * (`setNoDelay(true)`) on each connection so the lockstep request/response
 * framing is not delayed waiting to coalesce writes.
 *
 * SECURITY: no authentication or TLS — trusted networks only; the default
 * host is loopback (`127.0.0.1`).  Use the HTTP transport for untrusted
 * networks.
 */
export async function serveTcp(protocol: Protocol, options: ServeTcpOptions = {}): Promise<ServeTcpHandle> {
  const host = options.host ?? "127.0.0.1";
  const requestedPort = options.port ?? 0;
  const idleTimeoutS = options.idleTimeout ?? 300;
  const startupGraceS = options.startupGraceSeconds ?? 5;
  const protocolVersion = options.protocolVersion ?? "";
  const serverId = options.serverId ?? crypto.randomUUID().replace(/-/g, "").slice(0, 12);
  const enableDescribe = options.enableDescribe ?? true;
  const dispatchHook = options.dispatchHook ?? null;
  const externalConfig = options.externalLocation;
  const onServeStart = options.onServeStart ?? null;
  const backlog = options.backlog ?? 128;
  const announcementSink = options.announcementSink ?? process.stdout;

  // Cache for __describe__ — Web-Crypto digest is async, so memoise.
  let describePromise: Promise<{ batch: import("../arrow/index.js").VgiBatch; protocolHash: string }> | null = null;
  function describeInfo(): Promise<{ batch: import("../arrow/index.js").VgiBatch; protocolHash: string }> {
    if (!describePromise) {
      describePromise = buildDescribeBatch(protocol.name, protocol.getMethods(), serverId).then(
        ({ batch, metadata }) => ({
          batch,
          protocolHash: metadata.get("vgi_rpc.protocol_hash") ?? "",
        }),
      );
    }
    return describePromise;
  }

  // Lifecycle: only commit `serveStartFired` after the hook returns successfully.
  let serveStartFired = false;
  async function notifyTransport(): Promise<void> {
    if (serveStartFired) return;
    if (onServeStart) {
      await onServeStart(TransportKind.TCP);
    }
    serveStartFired = true;
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
    resolveDone();
  }

  server.on("connection", (socket) => {
    // Disable Nagle so the lockstep framing is not delayed coalescing writes.
    try {
      socket.setNoDelay(true);
    } catch {
      // best-effort
    }
    activeConnections += 1;
    disarmIdleTimer();
    handleConnection(socket)
      .catch((err) => {
        // Per-connection errors must not take down the server — log to stderr
        // and let the next connection proceed.
        process.stderr.write(`vgi-rpc/tcp: connection failed: ${(err as Error)?.message ?? err}\n`);
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
    // Build the writer over the Socket itself, not its raw fd, so we go
    // through `socket.write` + `'drain'` and yield the event loop while the
    // kernel send buffer drains (see serve-unix.ts for the rationale).
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
    let params: Record<string, unknown>;
    let requestId: string | null;
    try {
      const parsed = parseRequest(schema, batch);
      methodName = parsed.methodName;
      params = parsed.params;
      requestId = parsed.requestId;
    } catch (e: unknown) {
      const errBatch = buildErrorBatch(EMPTY_SCHEMA, e as Error, serverId, null);
      await writer.writeStream(EMPTY_SCHEMA, [errBatch]);
      if (e instanceof VersionError || e instanceof RpcError) return;
      throw e;
    }

    if (methodName === DESCRIBE_METHOD_NAME && enableDescribe) {
      const { batch: descBatch } = await describeInfo();
      await writer.writeStream(descBatch.schema, [descBatch]);
      return;
    }

    const methods = protocol.getMethods();
    const method = methods.get(methodName);
    if (!method) {
      const available = [...methods.keys()].sort();
      const err = new Error(`Unknown method: '${methodName}'. Available methods: [${available.join(", ")}]`);
      const errBatch = buildErrorBatch(EMPTY_SCHEMA, err, serverId, requestId);
      await writer.writeStream(EMPTY_SCHEMA, [errBatch]);
      return;
    }

    const methodType = method.type === MethodType.UNARY ? "unary" : "stream";
    let requestData: Uint8Array | undefined;
    try {
      requestData = serializeBatch(batch);
    } catch {
      // best-effort
    }
    const { protocolHash } = await describeInfo();
    const info: DispatchInfo = {
      method: methodName,
      methodType,
      serverId,
      requestId,
      protocol: protocol.name,
      protocolHash,
      protocolVersion,
      kind: TransportKind.TCP,
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
        await dispatchUnary(method, params, writer, serverId, requestId, externalConfig, TransportKind.TCP);
      } else {
        await dispatchStream(method, params, writer, reader, serverId, requestId, externalConfig, TransportKind.TCP);
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
    server.listen({ host, port: requestedPort, backlog }, () => resolve());
    server.once("error", (err) => reject(err));
  });

  const address = server.address();
  const boundPort = typeof address === "object" && address ? address.port : requestedPort;

  options.onBound?.(host, boundPort);
  // Cross-language launcher contract: announce on stdout, then write nothing
  // more to stdout for the rest of the process lifetime.
  announcementSink.write(`TCP:${host}:${boundPort}\n`);

  // Start the idle timer with a startup grace window.
  if (idleTimeoutS > 0) {
    setTimeout(() => {
      if (activeConnections === 0 && !stopped) armIdleTimer();
    }, startupGraceS * 1000).unref?.();
  }

  return {
    host,
    port: boundPort,
    stop: shutdown,
    done,
  };
}
