// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { type RecordBatch, RecordBatchReader } from "@query-farm/apache-arrow";
import type { VgiBatch, VgiSchema } from "../arrow/index.js";

export interface StreamMessage {
  schema: VgiSchema;
  batches: VgiBatch[];
}

/**
 * Largest byte count asked of a single Node `Readable.read(n)`.
 *
 * arrow-js's Node-stream adapter asks for a whole IPC message body in one
 * call. Both Node and Bun throw `ERR_OUT_OF_RANGE` from `read(n)` once `n`
 * exceeds their shared 1 GiB `MAX_HWM`, so a record batch with a body over
 * 1 GiB dies before a single byte is decoded — the read-side twin of the
 * write clamp in ./writer.ts. The adapter already loops until it has the
 * bytes it asked for, so answering with less is absorbed; it only ever
 * mis-handles being answered with *more*.
 */
export const MAX_READ_CHUNK = 1 << 26; // 64 MiB

/** True for a Node `Readable`; a web `ReadableStream` has no `pipe`/`read`. */
function isNodeReadable(input: unknown): boolean {
  const s = input as { read?: unknown; pipe?: unknown } | null;
  return typeof s?.read === "function" && typeof s?.pipe === "function";
}

/**
 * Wrap a Node `Readable` so `read(n)` never asks for more than
 * {@link MAX_READ_CHUNK}. Everything else passes through untouched, including
 * the `read`/`pipe`/`readable` trio arrow-js sniffs to pick this adapter.
 */
export function clampReads<T extends object>(stream: T): T {
  return new Proxy(stream, {
    get(target, prop) {
      if (prop === "read") {
        return (size?: number) =>
          (target as { read(n?: number): unknown }).read(
            typeof size === "number" ? Math.min(size, MAX_READ_CHUNK) : size,
          );
      }
      // `target` as the receiver, not the proxy: a getter on a Node stream
      // runs with `this` bound to the real object, so private fields and
      // internal state resolve as they would without the wrapper.
      const value = Reflect.get(target, prop, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

/**
 * Reads sequential IPC streams from a byte source (e.g., process.stdin).
 * Uses autoDestroy: false + reset/open pattern to read multiple streams
 * from the same underlying byte source.
 */
export class IpcStreamReader {
  private reader: RecordBatchReader;
  private initialized = false;
  /** True once readNextBatch() returns null (EOS reached for current stream). */
  private streamEnded = false;

  private constructor(reader: RecordBatchReader) {
    this.reader = reader;
  }

  static async create(input: ReadableStream<Uint8Array> | NodeJS.ReadableStream): Promise<IpcStreamReader> {
    const source = isNodeReadable(input) ? clampReads(input as object) : input;
    const reader = await RecordBatchReader.from(source as any);
    await reader.open({ autoDestroy: false });
    if (reader.closed) {
      throw new Error("Input stream closed before first IPC message");
    }
    return new IpcStreamReader(reader);
  }

  /**
   * Read one complete IPC stream (schema + all batches).
   * Returns null on EOF (no more streams).
   */
  async readStream(): Promise<StreamMessage | null> {
    if (this.initialized) {
      // Advance to next stream
      await this.reader.reset().open();
      if (this.reader.closed) {
        return null;
      }
    }
    this.initialized = true;

    const schema = this.reader.schema;
    if (!schema) {
      return null;
    }

    const batches: RecordBatch[] = [];
    while (true) {
      const result = await this.reader.next();
      if (result.done) break;
      // Skip Arrow-JS synthetic placeholder for empty streams
      if (result.value.constructor.name === "_InternalEmptyPlaceholderRecordBatch") break;
      batches.push(result.value);
    }

    return { schema, batches };
  }

  /**
   * Open the next IPC stream and return its schema.
   * Use readNextBatch() to read batches one at a time.
   * Returns null on EOF.
   */
  async openNextStream(): Promise<VgiSchema | null> {
    if (this.initialized) {
      await this.reader.reset().open();
      if (this.reader.closed) {
        return null;
      }
    }
    this.initialized = true;
    this.streamEnded = false;
    return this.reader.schema ?? null;
  }

  /**
   * Read the next batch from the currently open IPC stream.
   * Returns null when the stream ends (EOS).
   *
   * Once EOS is reached, subsequent calls return null immediately without
   * reading from the underlying byte source. This prevents the Arrow-JS
   * reader from consuming bytes that belong to the next IPC stream.
   */
  async readNextBatch(): Promise<VgiBatch | null> {
    if (this.streamEnded) return null;
    const result = await this.reader.next();
    if (result.done) {
      this.streamEnded = true;
      return null;
    }
    // Arrow-JS synthesizes a placeholder batch for streams with a schema but
    // zero real batches. Treat it as EOS so callers don't block trying to
    // read more bytes from a stream that has already ended.
    if (result.value.constructor.name === "_InternalEmptyPlaceholderRecordBatch") {
      this.streamEnded = true;
      return null;
    }
    return result.value;
  }

  async cancel(): Promise<void> {
    await this.reader.cancel();
  }
}
