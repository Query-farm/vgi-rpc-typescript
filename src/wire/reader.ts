// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { RecordBatchReader, type RecordBatch, type Schema } from "apache-arrow";

export interface StreamMessage {
  schema: Schema;
  batches: RecordBatch[];
}

/**
 * Reads sequential IPC streams from a byte source (e.g., process.stdin).
 * Uses autoDestroy: false + reset/open pattern to read multiple streams
 * from the same underlying byte source.
 */
export class IpcStreamReader {
  private reader: RecordBatchReader;
  private initialized = false;

  private constructor(reader: RecordBatchReader) {
    this.reader = reader;
  }

  static async create(
    input: ReadableStream<Uint8Array> | NodeJS.ReadableStream,
  ): Promise<IpcStreamReader> {
    const reader = await RecordBatchReader.from(input as any);
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
      batches.push(result.value);
    }

    return { schema, batches };
  }

  /**
   * Open the next IPC stream and return its schema.
   * Use readNextBatch() to read batches one at a time.
   * Returns null on EOF.
   */
  async openNextStream(): Promise<Schema | null> {
    if (this.initialized) {
      await this.reader.reset().open();
      if (this.reader.closed) {
        return null;
      }
    }
    this.initialized = true;
    return this.reader.schema ?? null;
  }

  /**
   * Read the next batch from the currently open IPC stream.
   * Returns null when the stream ends (EOS).
   */
  async readNextBatch(): Promise<RecordBatch | null> {
    const result = await this.reader.next();
    if (result.done) return null;
    return result.value;
  }

  async cancel(): Promise<void> {
    await this.reader.cancel();
  }
}
