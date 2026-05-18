// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import {
  type RecordBatch as A_RecordBatch,
  type Schema as A_Schema,
  RecordBatchStreamWriter,
} from "@query-farm/apache-arrow";
import { serializeBatches, type VgiBatch, type VgiSchema } from "../arrow/index.js";

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
 * Handles EAGAIN (pipe buffer full) by busy-waiting with Atomics.wait().
 * writeSync() can return fewer bytes than requested when the pipe buffer
 * is full (e.g., 64KB limit), and throws EAGAIN on non-blocking fds.
 */
function writeAll(fd: number, data: Uint8Array): void {
  const writeSync = _loadWriteSync();
  let offset = 0;
  while (offset < data.length) {
    try {
      const written = writeSync(fd, data, offset, data.length - offset);
      if (written <= 0) throw new Error(`writeSync returned ${written}`);
      offset += written;
    } catch (e: any) {
      if (e.code === "EAGAIN") {
        // Pipe buffer full — busy-wait briefly then retry
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 1);
        continue;
      }
      throw e;
    }
  }
}

/**
 * Writes sequential IPC streams to a file descriptor (e.g., stdout).
 * Each call to writeStream() writes a complete IPC stream: schema + batches + EOS.
 *
 * All writes use synchronous I/O (writeSync) to avoid deadlocks when
 * interleaving stdout writes with blocking stdin reads.
 */
export class IpcStreamWriter {
  private readonly fd: number;

  constructor(fd: number = STDOUT_FD) {
    this.fd = fd;
  }

  /**
   * Write a complete IPC stream with the given schema and batches.
   * Creates schema message, writes all batches (with their metadata), writes EOS.
   */
  writeStream(schema: VgiSchema, batches: VgiBatch[]): void {
    // Delegate to the Arrow facade so the bytes-on-the-wire match the
    // active backend (arrow-js by default, flechette under the `workerd`/
    // `worker`/`browser` package.json conditions). The incremental stream
    // below still uses arrow-js directly — flechette has no equivalent
    // streaming-writer surface and only the stdio server uses the
    // incremental path.
    writeAll(this.fd, serializeBatches(schema, batches));
  }

  /**
   * Open an incremental IPC stream for writing batches one at a time.
   */
  openStream(schema: VgiSchema): IncrementalStream {
    return new IncrementalStream(this.fd, schema);
  }
}

/**
 * An open IPC stream that supports incremental batch writes.
 *
 * Uses RecordBatchStreamWriter with internal buffering (no pipe to stdout).
 * After each operation, drains the writer's internal AsyncByteQueue buffer
 * and writes bytes synchronously via writeAll(). This avoids deadlocks
 * caused by Node.js async stream piping when stdin reads block before
 * stdout writes flush through the event loop.
 */
export class IncrementalStream {
  private writer: RecordBatchStreamWriter;
  private readonly fd: number;
  private closed = false;

  constructor(fd: number, schema: VgiSchema) {
    this.fd = fd;
    this.writer = new RecordBatchStreamWriter();
    // Buffer internally (no sink) — we drain manually via writeAll
    this.writer.reset(undefined, schema as unknown as A_Schema);
    this.drain();
  }

  /**
   * Write a single batch to the stream. Bytes are flushed synchronously.
   *
   * Uses _writeRecordBatch() directly to bypass the Arrow writer's schema
   * comparison in write(). The public write() method calls compareSchemas()
   * and auto-closes the writer if the batch's schema differs (e.g., in
   * nullability), silently dropping the batch. Since our output schema is
   * set at stream open time and all batches are structurally compatible,
   * we skip the comparison.
   */
  write(batch: VgiBatch): void {
    if (this.closed) throw new Error("Stream already closed");
    (this.writer as any)._writeRecordBatch(batch as unknown as A_RecordBatch);
    this.drain();
  }

  /**
   * Close the stream (writes EOS marker synchronously).
   */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    // EOS marker: continuation (0xFFFFFFFF) + metadata length (0x00000000)
    const eos = new Uint8Array(new Int32Array([-1, 0]).buffer);
    writeAll(this.fd, eos);
  }

  /**
   * Drain buffered bytes from the Arrow writer's internal queue
   * and write them synchronously to the output fd.
   */
  private drain(): void {
    const values = (this.writer as any)._sink._values as Uint8Array[];
    for (const chunk of values) {
      writeAll(this.fd, chunk);
    }
    values.length = 0;
  }
}
