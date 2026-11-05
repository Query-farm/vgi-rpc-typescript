// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { RecordBatchReader } from "@query-farm/apache-arrow";
import * as arrowjs from "../arrow/impl-arrowjs/index.js";
import { backend, deserializeBatch, deserializeSchema, type VgiBatch, type VgiSchema } from "../arrow/index.js";

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
 * write clamp in ./writer.ts.
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
 *
 * An oversized read answers `null`, not a shorter chunk. That is `read(n)`'s
 * normal answer when `n` bytes are not available, and the adapter responds with
 * a bare `read()`. A short non-null answer would lose data: the adapter
 * overwrites it with that `read()` result.
 *
 * That bare `read()` has to be bounded by what the adapter still needs, because
 * this wrapper answers `null` even when the bytes are there. Draining the stream
 * would hand the adapter the start of the next message, and its serve loop
 * (`while (size < bufferLength)`) then waits for another `'readable'` when a
 * request exactly empties its buffer. A peer that has sent everything and is
 * waiting for a reply never causes that event. Unwrapped, the adapter only falls
 * back to `read()` when fewer than `n` bytes are buffered, so it cannot get there.
 */
export function clampReads<T extends object>(stream: T): T {
  const source = stream as { read(n?: number): unknown; readableLength?: number };
  // What the adapter asked for in the read just answered with `null`.
  let owed = 0;
  return new Proxy(stream, {
    get(target, prop) {
      if (prop === "read") {
        return (size?: number) => {
          if (typeof size === "number" && size > MAX_READ_CHUNK) {
            owed = size;
            return null;
          }
          const limit = size === undefined ? owed : 0;
          owed = 0;
          if (limit === 0) return source.read(size);
          const take = Math.min(limit, MAX_READ_CHUNK, source.readableLength ?? 0);
          return take > 0 ? source.read(take) : source.read();
        };
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

  private constructor(
    reader: RecordBatchReader,
    private readonly useActiveBackend: boolean,
  ) {
    this.reader = reader;
  }

  static async create(
    input: ReadableStream<Uint8Array> | NodeJS.ReadableStream,
    options: { useActiveBackend?: boolean } = {},
  ): Promise<IpcStreamReader> {
    const source = isNodeReadable(input) ? clampReads(input as object) : input;
    const reader = await RecordBatchReader.from(source as any);
    await reader.open({ autoDestroy: false });
    if (reader.closed) {
      throw new Error("Input stream closed before first IPC message");
    }
    return new IpcStreamReader(reader, options.useActiveBackend === true && backend.name !== "arrow-js");
  }

  // Server handlers need their selected backend's schemas and columns. Clients
  // retain the public Arrow JS result API. Cross via IPC to preserve raw units,
  // nested buffers, dictionary values, row counts and custom batch metadata.
  private schemaForBackend(schema: VgiSchema): VgiSchema {
    return this.useActiveBackend ? deserializeSchema(arrowjs.serializeSchema(schema)) : schema;
  }

  private batchForBackend(batch: VgiBatch): VgiBatch {
    return this.useActiveBackend ? deserializeBatch(arrowjs.serializeBatch(batch)) : batch;
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

    const batches: VgiBatch[] = [];
    while (true) {
      const result = await this.reader.next();
      if (result.done) break;
      // Skip Arrow-JS synthetic placeholder for empty streams
      if (result.value.constructor.name === "_InternalEmptyPlaceholderRecordBatch") break;
      batches.push(this.batchForBackend(result.value));
    }

    return { schema: this.schemaForBackend(schema), batches };
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
    return this.reader.schema ? this.schemaForBackend(this.reader.schema) : null;
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
    return this.batchForBackend(result.value);
  }

  async cancel(): Promise<void> {
    await this.reader.cancel();
  }
}
