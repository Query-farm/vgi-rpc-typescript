// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import type { Socket } from "node:net";
import type { IncrementalEncoder, VgiBatch, VgiSchema } from "../arrow/index.js";
import { createIncrementalEncoder, serializeBatches } from "../arrow/index.js";

const STDOUT_FD = 1;

// Resolve node:fs via indirect-string require so esbuild/wrangler don't
// statically pull node:fs into the bundle. Workers (Cloudflare workerd) never
// instantiate IpcStreamWriter (no stdio transport on workers), so the
// runtime-time require("node:fs") is unreachable in those builds.
//
// `globalThis.require` is undefined in both Bun ESM and Node ESM, so we try
// `import.meta.require` (Bun) first, then fall back to globalThis.require
// (Node CJS). Node ESM consumers must polyfill require if they need the
// subprocess transport.
const _NODE_FS_MOD = "node:fs";
let _writeSync: ((fd: number, data: Uint8Array, offset?: number, len?: number) => number) | null = null;
function _loadWriteSync(): (fd: number, data: Uint8Array, offset?: number, len?: number) => number {
  if (_writeSync) return _writeSync;
  const req: any = (import.meta as any).require ?? (globalThis as any).require ?? null;
  if (!req) {
    throw new Error(
      "IpcStreamWriter requires Bun or Node.js CJS for sync node:fs.writeSync. " +
        "Subprocess transport is not available in this runtime.",
    );
  }
  const fs = req(_NODE_FS_MOD);
  _writeSync = fs.writeSync.bind(fs);
  return _writeSync!;
}

/**
 * Write all bytes to a file descriptor, looping on partial writes.
 *
 * Stdio path only. The fd here is a pipe (typically stdout) inherited from
 * the launcher in subprocess transport. Pipes set up by fork/exec are
 * blocking by default, so writeSync blocks the kernel (not the event loop in
 * a bad way) until the parent reader drains — that is the *desired* lockstep
 * with the client. The AF_UNIX path does not come through here; sockets are
 * non-blocking and EAGAIN-spinning them would freeze the event loop and
 * starve every other connection. See `socketWriteAll` below.
 */
function writeAll(fd: number, data: Uint8Array): void {
  const writeSync = _loadWriteSync();
  let offset = 0;
  let spins = 0;
  while (offset < data.length) {
    try {
      const written = writeSync(fd, data, offset, data.length - offset);
      if (written <= 0) throw new Error(`writeSync returned ${written}`);
      offset += written;
      spins = 0;
    } catch (e: any) {
      if (e.code === "EAGAIN") {
        // Non-blocking pipe backpressure. The stdio path has a single peer (the
        // engine), which drains the pipe within microseconds in lockstep, so
        // spin-retry rather than sleep: a 1ms `Atomics.wait` here is ~1000x too
        // coarse and *dominated* large-batch write time — a 4KB-row passthru
        // spent ~13s of a 15s run asleep in this branch, i.e. the whole TS
        // "wire tax" was this sleep. Spinning is safe here because there are no
        // other connections to starve (unlike the AF_UNIX socketWriteAll path,
        // which correctly awaits 'drain'). Fall back to a brief sleep only if
        // the peer looks genuinely stalled, so a hung reader can't pin a core.
        if (++spins < 8192) continue;
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 1);
        spins = 0;
        continue;
      }
      throw e;
    }
  }
}

/**
 * Asynchronously write all bytes to a Node net.Socket, honouring backpressure.
 *
 * `socket.write(buf)` queues `buf` into libuv's per-socket outbound buffer
 * and returns false when that buffer has grown past `highWaterMark`. We then
 * `await` the `'drain'` event so the JS thread yields — other connections
 * can be dispatched while the kernel drains the send buffer in the
 * background. Without this yield, a slow consumer on connection A would
 * freeze every other connection's handler on the shared event loop.
 *
 * Resolves cleanly on socket close/error so the caller's try/finally can
 * unwind without dangling listeners.
 */
async function socketWriteAll(socket: Socket, data: Uint8Array): Promise<void> {
  if (socket.destroyed || socket.writableEnded) {
    throw new Error("socketWriteAll: socket is already closed");
  }
  const ok = socket.write(data);
  if (ok) return;
  await new Promise<void>((resolve, reject) => {
    const cleanup = () => {
      socket.off("drain", onDrain);
      socket.off("error", onError);
      socket.off("close", onClose);
    };
    const onDrain = () => {
      cleanup();
      resolve();
    };
    const onError = (err: Error) => {
      cleanup();
      reject(err);
    };
    const onClose = () => {
      // Peer hung up before drain. Resolve so the caller's try/finally
      // runs; the next read on this connection will surface EOF.
      cleanup();
      resolve();
    };
    socket.once("drain", onDrain);
    socket.once("error", onError);
    socket.once("close", onClose);
  });
}

/**
 * A browser/worker-friendly byte sink: `write` receives each fully-serialized
 * chunk of response bytes (a schema msg, a batch, or the EOS marker) and must
 * deliver them in order. Used by the in-browser SAB (`worker:`) transport, which
 * has no Node fd/Socket — the sink writes into the worker→client ring.
 */
export type ByteSink = {
  /** Deliver one fully-serialized chunk of response bytes, in order. */
  write: (bytes: Uint8Array) => void | Promise<void>;
};

type WriterTarget = { kind: "fd"; fd: number } | { kind: "socket"; socket: Socket } | { kind: "sink"; sink: ByteSink };

/**
 * Writes sequential IPC streams to either an fd (stdio subprocess transport)
 * or a Node Socket (AF_UNIX transport). Each call to writeStream() writes a
 * complete IPC stream: schema + batches + EOS.
 *
 * All public methods are async. The fd path resolves immediately after a
 * synchronous writeSync; the socket path awaits real `'drain'` events on
 * backpressure so the event loop stays responsive to other connections.
 */
export class IpcStreamWriter {
  private readonly target: WriterTarget;

  /**
   * Construct from a file descriptor (stdio transport) or a Node net.Socket
   * (AF_UNIX transport). The default targets stdout for legacy stdio servers
   * that didn't pass an fd.
   */
  constructor(fdOrSocketOrSink: number | Socket | ByteSink = STDOUT_FD) {
    if (typeof fdOrSocketOrSink === "number") {
      this.target = { kind: "fd", fd: fdOrSocketOrSink };
    } else if (typeof (fdOrSocketOrSink as ByteSink).write === "function" && !("writable" in fdOrSocketOrSink)) {
      // A plain byte sink (browser/worker SAB) — has write() but is not a Node Socket
      // (Sockets have a `writable` boolean property).
      this.target = { kind: "sink", sink: fdOrSocketOrSink as ByteSink };
    } else {
      this.target = { kind: "socket", socket: fdOrSocketOrSink as Socket };
    }
  }

  /**
   * Write a complete IPC stream with the given schema and batches.
   * Creates schema message, writes all batches (with their metadata), writes EOS.
   */
  async writeStream(schema: VgiSchema, batches: VgiBatch[]): Promise<void> {
    // Delegate to the Arrow facade so the bytes-on-the-wire match the
    // active backend (arrow-js by default, flechette under the `workerd`/
    // `worker`/`browser` package.json conditions). The incremental stream
    // below still uses arrow-js directly — flechette has no equivalent
    // streaming-writer surface and only the stdio server uses the
    // incremental path.
    const bytes = serializeBatches(schema, batches);
    if (this.target.kind === "fd") {
      writeAll(this.target.fd, bytes);
    } else if (this.target.kind === "sink") {
      await this.target.sink.write(bytes);
    } else {
      await socketWriteAll(this.target.socket, bytes);
    }
  }

  /**
   * Open an incremental IPC stream for writing batches one at a time.
   */
  openStream(schema: VgiSchema): IncrementalStream {
    return new IncrementalStream(this.target, schema);
  }
}

/**
 * An open IPC stream that supports incremental batch writes.
 *
 * Drives a backend {@link IncrementalEncoder} and flushes its bytes through
 * the same target (fd or socket) as the parent IpcStreamWriter. The write()
 * and close() methods are async so the socket path can yield on backpressure
 * — critical under AF_UNIX where the kernel send buffer (~8 KB on macOS)
 * fills quickly and any synchronous busy-wait would starve every other
 * connection sharing this event loop.
 *
 * The encoder is obtained from the Arrow facade, so this file no longer
 * imports arrow-js directly — keeping arrow-js out of the flechette
 * (workerd/browser) bundle. Both backends now provide an incremental encoder
 * (the flechette one carves per-message frames out of its one-shot stream
 * encoder), so the stdio lockstep exchange works on flechette as well as
 * arrow-js.
 */
export class IncrementalStream {
  private readonly encoder: IncrementalEncoder;
  private readonly target: WriterTarget;
  private closed = false;
  // Chains async writes so concurrent callers can't interleave bytes on the
  // wire. Each operation appends to this promise; awaiting it preserves
  // FIFO order even when the caller doesn't await between writes.
  private writeChain: Promise<void> = Promise.resolve();

  constructor(target: WriterTarget, schema: VgiSchema) {
    this.target = target;
    this.encoder = createIncrementalEncoder(schema);
    // Schema bytes — synchronous on the fd path, queued on the socket path.
    // Callers don't await openStream(), so we serialize via writeChain.
    this.enqueue(this.encoder.start());
  }

  /** Write a single batch. Resolves once the bytes are queued/flushed. */
  async write(batch: VgiBatch): Promise<void> {
    if (this.closed) throw new Error("Stream already closed");
    return this.enqueue(this.encoder.writeBatch(batch));
  }

  /** Close the stream (writes EOS marker). */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    return this.enqueue(this.encoder.finish());
  }

  private enqueue(bytes: Uint8Array): Promise<void> {
    const next = this.writeChain.then(() => {
      if (this.target.kind === "fd") {
        writeAll(this.target.fd, bytes);
        return;
      }
      if (this.target.kind === "sink") {
        return this.target.sink.write(bytes);
      }
      return socketWriteAll(this.target.socket, bytes);
    });
    // Swallow rejections on the chain so a single failure doesn't poison
    // every subsequent enqueue with an unhandled rejection. The returned
    // `next` still propagates the error to the caller that triggered it.
    this.writeChain = next.catch(() => undefined);
    return next;
  }
}
