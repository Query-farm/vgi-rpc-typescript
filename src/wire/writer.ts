import {
  RecordBatchStreamWriter,
  type RecordBatch,
  type Schema,
} from "apache-arrow";

/**
 * Writes sequential IPC streams to a byte sink (e.g., process.stdout).
 * Each call to writeStream() writes a complete IPC stream: schema + batches + EOS.
 */
export class IpcStreamWriter {
  readonly output: NodeJS.WritableStream;

  constructor(output: NodeJS.WritableStream) {
    this.output = output;
  }

  /**
   * Write a complete IPC stream with the given schema and batches.
   * Creates schema message, writes all batches (with their metadata), writes EOS.
   */
  writeStream(schema: Schema, batches: RecordBatch[]): void {
    const writer = new RecordBatchStreamWriter();
    writer.reset(undefined, schema);
    for (const batch of batches) {
      writer.write(batch);
    }
    writer.close();
    const bytes = writer.toUint8Array(true);
    this.output.write(bytes);
  }

  /**
   * Open an incremental IPC stream for writing batches one at a time.
   * Used for streaming methods where output batches are produced incrementally.
   * The writer flushes bytes to stdout as each batch is written.
   */
  openStream(schema: Schema): IncrementalStream {
    return new IncrementalStream(this.output, schema);
  }
}

/**
 * An open IPC stream that supports incremental batch writes.
 * Uses RecordBatchStreamWriter with the output writable as the direct sink.
 */
export class IncrementalStream {
  private writer: RecordBatchStreamWriter;
  private closed = false;

  constructor(
    output: NodeJS.WritableStream,
    schema: Schema,
  ) {
    this.writer = new RecordBatchStreamWriter();
    // Pass the output directly as the sink - bytes flow immediately
    this.writer.reset(output as any, schema);
  }

  /**
   * Write a single batch to the stream. Bytes are flushed immediately.
   */
  write(batch: RecordBatch): void {
    if (this.closed) throw new Error("Stream already closed");
    this.writer.write(batch);
  }

  /**
   * Close the stream (writes EOS marker and flushes).
   */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.writer.close();
  }
}
