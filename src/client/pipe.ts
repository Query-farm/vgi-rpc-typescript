// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import {
  RecordBatch,
  RecordBatchStreamWriter,
  Schema,
  Field,
  Struct,
  makeData,
  vectorFromArray,
} from "apache-arrow";
import { DESCRIBE_METHOD_NAME } from "../constants.js";
import { serializeIpcStream } from "../http/common.js";
import { IpcStreamReader } from "../wire/reader.js";
import {
  inferArrowType,
  buildRequestIpc,
  dispatchLogOrError,
  extractBatchRows,
} from "./ipc.js";
import {
  parseDescribeResponse,
  type MethodInfo,
  type ServiceDescription,
} from "./introspect.js";
import type {
  LogMessage,
  PipeConnectOptions,
  SubprocessConnectOptions,
  StreamSession,
} from "./types.js";
import type { RpcClient } from "./connect.js";
import { RpcError } from "../errors.js";

// ---------------------------------------------------------------------------
// Writable abstraction
// ---------------------------------------------------------------------------

interface PipeWritable {
  write(data: Uint8Array): void;
  flush?(): void;
  end(): void;
}

type WriteFn = (bytes: Uint8Array) => void;

// ---------------------------------------------------------------------------
// PipeIncrementalWriter — batch-by-batch IPC writing for lockstep streaming
// ---------------------------------------------------------------------------

class PipeIncrementalWriter {
  private writer: RecordBatchStreamWriter;
  private writeFn: WriteFn;
  private closed = false;

  constructor(writeFn: WriteFn, schema: Schema) {
    this.writeFn = writeFn;
    this.writer = new RecordBatchStreamWriter();
    this.writer.reset(undefined, schema);
    this.drain(); // flushes schema message
  }

  write(batch: RecordBatch): void {
    if (this.closed) throw new Error("PipeIncrementalWriter already closed");
    (this.writer as any)._writeRecordBatch(batch);
    this.drain();
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    // EOS marker: continuation (0xFFFFFFFF) + metadata length (0x00000000)
    const eos = new Uint8Array(new Int32Array([-1, 0]).buffer);
    this.writeFn(eos);
  }

  private drain(): void {
    const values = (this.writer as any)._sink._values as Uint8Array[];
    for (const chunk of values) {
      this.writeFn(chunk);
    }
    values.length = 0;
  }
}

// ---------------------------------------------------------------------------
// PipeStreamSession — lockstep streaming over pipes
// ---------------------------------------------------------------------------

export class PipeStreamSession implements StreamSession {
  private _reader: IpcStreamReader;
  private _writeFn: WriteFn;
  private _onLog?: (msg: LogMessage) => void;
  private _header: Record<string, any> | null;
  private _inputWriter: PipeIncrementalWriter | null = null;
  private _inputSchema: Schema | null = null;
  private _outputStreamOpened = false;
  private _closed = false;
  private _outputSchema: Schema;
  private _releaseBusy: () => void;
  private _setDrainPromise: (p: Promise<void>) => void;

  constructor(opts: {
    reader: IpcStreamReader;
    writeFn: WriteFn;
    onLog?: (msg: LogMessage) => void;
    header: Record<string, any> | null;
    outputSchema: Schema;
    releaseBusy: () => void;
    setDrainPromise: (p: Promise<void>) => void;
  }) {
    this._reader = opts.reader;
    this._writeFn = opts.writeFn;
    this._onLog = opts.onLog;
    this._header = opts.header;
    this._outputSchema = opts.outputSchema;
    this._releaseBusy = opts.releaseBusy;
    this._setDrainPromise = opts.setDrainPromise;
  }

  get header(): Record<string, any> | null {
    return this._header;
  }

  /**
   * Read output batches from the server until a data batch is found.
   * Dispatches log/error batches along the way.
   * Returns null when server closes output stream (EOS).
   */
  private async _readOutputBatch(): Promise<RecordBatch | null> {
    while (true) {
      const batch = await this._reader.readNextBatch();
      if (batch === null) return null; // Server closed output stream

      if (batch.numRows === 0) {
        // Check if it's a log/error batch. If so, dispatch and continue.
        // Otherwise it's a zero-row data batch — return it.
        if (dispatchLogOrError(batch, this._onLog)) {
          continue;
        }
      }

      return batch;
    }
  }

  /**
   * Ensure the server's output stream is opened for reading.
   * Must be called AFTER sending the first input batch, because
   * the server's output schema may not be flushed until it processes
   * the first input and writes the first output batch.
   */
  private async _ensureOutputStream(): Promise<void> {
    if (this._outputStreamOpened) return;
    this._outputStreamOpened = true;
    const schema = await this._reader.openNextStream();
    if (!schema) {
      throw new RpcError("ProtocolError", "Expected output stream but got EOF", "");
    }
  }

  /**
   * Send an exchange request and return the data rows.
   */
  async exchange(input: Record<string, any>[]): Promise<Record<string, any>[]> {
    if (this._closed) {
      throw new RpcError("ProtocolError", "Stream session is closed", "");
    }

    // Build input batch
    let inputSchema: Schema;
    let batch: RecordBatch;

    if (input.length === 0) {
      // Zero-row exchange: use cached input schema from a prior exchange,
      // then fall back to the output schema from describe. The cached
      // schema is preferred because input and output schemas may differ
      // (e.g. exchange_accumulate: input {value} → output {running_sum, exchange_count}).
      inputSchema = this._inputSchema ?? this._outputSchema;
      const children = inputSchema.fields.map((f) => {
        return makeData({ type: f.type, length: 0, nullCount: 0 });
      });
      const structType = new Struct(inputSchema.fields);
      const data = makeData({
        type: structType,
        length: 0,
        children,
        nullCount: 0,
      });
      batch = new RecordBatch(inputSchema, data);
    } else {
      // Infer schema from first row.
      // Always use nullable fields — the server validates input schemas
      // strictly and its schema typically uses nullable columns.
      const keys = Object.keys(input[0]);
      const fields = keys.map((key) => {
        let sample: any = undefined;
        for (const row of input) {
          if (row[key] != null) { sample = row[key]; break; }
        }
        const arrowType = inferArrowType(sample);
        return new Field(key, arrowType, /* nullable */ true);
      });
      inputSchema = new Schema(fields);

      // Validate schema consistency: all exchanges on the same pipe session
      // share a single IPC stream, so the schema is locked to the first call.
      if (this._inputSchema) {
        const cached = this._inputSchema;
        if (
          cached.fields.length !== inputSchema.fields.length ||
          cached.fields.some((f, i) => f.name !== inputSchema.fields[i].name)
        ) {
          throw new RpcError(
            "ProtocolError",
            `Exchange input schema changed: expected [${cached.fields.map((f) => f.name).join(", ")}] ` +
            `but got [${inputSchema.fields.map((f) => f.name).join(", ")}]`,
            "",
          );
        }
      } else {
        this._inputSchema = inputSchema;
      }

      const children = inputSchema.fields.map((f) => {
        const values = input.map((row) => row[f.name]);
        return vectorFromArray(values, f.type).data[0];
      });
      const structType = new Struct(inputSchema.fields);
      const data = makeData({
        type: structType,
        length: input.length,
        children,
        nullCount: 0,
      });
      batch = new RecordBatch(inputSchema, data);
    }

    // Lazy-open input writer on first exchange
    if (!this._inputWriter) {
      this._inputWriter = new PipeIncrementalWriter(this._writeFn, inputSchema);
    }

    // Write one input batch FIRST, then open output stream.
    // The server may not flush the output schema until it processes the
    // first input batch and writes the first output batch.
    this._inputWriter.write(batch);
    await this._ensureOutputStream();

    // Read output batch(es) from server
    try {
      const outputBatch = await this._readOutputBatch();
      if (outputBatch === null) {
        return [];
      }
      return extractBatchRows(outputBatch);
    } catch (e) {
      // On error, clean up the pipe so it's ready for the next request
      await this._cleanup();
      throw e;
    }
  }

  /**
   * Clean up after an error: close input, drain output, release busy.
   */
  private async _cleanup(): Promise<void> {
    if (this._closed) return;
    this._closed = true;
    if (this._inputWriter) {
      this._inputWriter.close();
      this._inputWriter = null;
    }
    try {
      if (this._outputStreamOpened) {
        while ((await this._reader.readNextBatch()) !== null) {}
      }
    } catch {
      // Suppress errors during drain
    }
    this._releaseBusy();
  }

  /**
   * Iterate over producer stream batches (lockstep).
   */
  async *[Symbol.asyncIterator](): AsyncIterableIterator<Record<string, any>[]> {
    if (this._closed) return;

    try {
      // Open input writer with empty schema for tick batches
      const tickSchema = new Schema([]);
      this._inputWriter = new PipeIncrementalWriter(this._writeFn, tickSchema);

      // Build a zero-row tick batch
      const structType = new Struct(tickSchema.fields);
      const tickData = makeData({
        type: structType,
        length: 0,
        children: [],
        nullCount: 0,
      });
      const tickBatch = new RecordBatch(tickSchema, tickData);

      while (true) {
        // Send one tick FIRST, then open output stream on first iteration.
        // The server may not flush the output schema until it processes the
        // first tick and writes the first output batch.
        this._inputWriter.write(tickBatch);
        await this._ensureOutputStream();

        // Read output batch(es)
        const outputBatch = await this._readOutputBatch();
        if (outputBatch === null) {
          // Server finished — EOS on output stream
          break;
        }

        yield extractBatchRows(outputBatch);
      }
    } finally {
      // Close input stream if still open
      if (this._inputWriter) {
        this._inputWriter.close();
        this._inputWriter = null;
      }
      // Drain any remaining output batches
      try {
        if (this._outputStreamOpened) {
          while ((await this._reader.readNextBatch()) !== null) {}
        }
      } catch {
        // Suppress errors during drain
      }
      this._closed = true;
      this._releaseBusy();
    }
  }

  close(): void {
    if (this._closed) return;
    this._closed = true;

    if (this._inputWriter) {
      // Close the input stream (EOS)
      this._inputWriter.close();
      this._inputWriter = null;
    } else {
      // Never iterated/exchanged — send empty schema stream so server unblocks.
      // Server is blocked at reader.openNextStream() waiting for client's input.
      const emptySchema = new Schema([]);
      const ipc = serializeIpcStream(emptySchema, []);
      this._writeFn(ipc);
    }

    // Drain remaining output batches asynchronously. Register the drain
    // promise so that the next acquireBusy() waits for it to complete.
    const drainPromise = (async () => {
      try {
        if (!this._outputStreamOpened) {
          const schema = await this._reader.openNextStream();
          if (schema) {
            while ((await this._reader.readNextBatch()) !== null) {}
          }
        } else {
          while ((await this._reader.readNextBatch()) !== null) {}
        }
      } catch {
        // Suppress errors during drain
      } finally {
        this._releaseBusy();
      }
    })();
    this._setDrainPromise(drainPromise);
  }
}

// ---------------------------------------------------------------------------
// pipeConnect — create an RpcClient over raw readable/writable streams
// ---------------------------------------------------------------------------

export function pipeConnect(
  readable: ReadableStream<Uint8Array>,
  writable: PipeWritable,
  options?: PipeConnectOptions,
): RpcClient {
  const onLog = options?.onLog;

  let reader: IpcStreamReader | null = null;
  let readerPromise: Promise<IpcStreamReader> | null = null;
  let methodCache: Map<string, MethodInfo> | null = null;
  let protocolName = "";
  let _busy = false;
  let _drainPromise: Promise<void> | null = null;
  let closed = false;

  const writeFn: WriteFn = (bytes: Uint8Array) => {
    writable.write(bytes);
    writable.flush?.();
  };

  // The IpcStreamReader.create() blocks until the first IPC schema arrives
  // on the readable. To avoid deadlock, we must send our first request
  // (the __describe__ call) BEFORE opening the reader. After that, the
  // response bytes are in the pipe buffer and the reader can consume them.
  async function ensureReader(): Promise<IpcStreamReader> {
    if (reader) return reader;
    if (!readerPromise) {
      readerPromise = IpcStreamReader.create(readable);
    }
    reader = await readerPromise;
    return reader;
  }

  async function acquireBusy(): Promise<void> {
    // Wait for any pending drain from a previous close()
    if (_drainPromise) {
      await _drainPromise;
      _drainPromise = null;
    }
    if (_busy) {
      throw new Error(
        "Pipe transport is busy — another call or stream is in progress. " +
        "Pipe connections are single-threaded; wait for the current operation to complete.",
      );
    }
    _busy = true;
  }

  function releaseBusy(): void {
    _busy = false;
  }

  function setDrainPromise(p: Promise<void>): void {
    _drainPromise = p;
  }

  async function ensureMethodCache(): Promise<Map<string, MethodInfo>> {
    if (methodCache) return methodCache;

    await acquireBusy();
    try {
      // Send __describe__ request BEFORE opening the reader.
      // IpcStreamReader.create() blocks on reader.open() which reads the
      // first schema message. The server won't write anything until it
      // receives a request. Sending first avoids deadlock.
      const emptySchema = new Schema([]);
      const body = buildRequestIpc(emptySchema, {}, DESCRIBE_METHOD_NAME);
      writeFn(body);

      const r = await ensureReader();

      // Read response (first IPC stream = describe response schema + batches)
      // ensureReader() consumed the schema via open(). Use readStream()
      // which — on the first call (initialized=false) — returns the current
      // stream without calling reset().
      const response = await r.readStream();
      if (!response) {
        throw new Error("EOF reading __describe__ response");
      }

      const desc = await parseDescribeResponse(response.batches, onLog);
      protocolName = desc.protocolName;
      methodCache = new Map(desc.methods.map((m) => [m.name, m]));
      return methodCache;
    } finally {
      releaseBusy();
    }
  }

  return {
    async call(
      method: string,
      params?: Record<string, any>,
    ): Promise<Record<string, any> | null> {
      const methods = await ensureMethodCache();
      await acquireBusy();
      try {
        const info = methods.get(method);
        if (!info) {
          throw new Error(`Unknown method: '${method}'`);
        }

        const r = await ensureReader();

        // Apply defaults
        const fullParams = { ...(info.defaults ?? {}), ...(params ?? {}) };

        // Send request
        const body = buildRequestIpc(info.paramsSchema, fullParams, method);
        writeFn(body);

        // Read response
        const response = await r.readStream();
        if (!response) {
          throw new Error("EOF reading response");
        }

        // Process batches: dispatch logs, find result
        let resultBatch: RecordBatch | null = null;
        for (const batch of response.batches) {
          if (batch.numRows === 0) {
            dispatchLogOrError(batch, onLog);
            continue;
          }
          resultBatch = batch;
        }

        if (!resultBatch) {
          return null;
        }

        const rows = extractBatchRows(resultBatch);
        if (rows.length === 0) return null;

        if (info.resultSchema.fields.length === 0) return null;

        return rows[0];
      } finally {
        releaseBusy();
      }
    },

    async stream(
      method: string,
      params?: Record<string, any>,
    ): Promise<StreamSession> {
      const methods = await ensureMethodCache();
      await acquireBusy();

      try {
        const info = methods.get(method);
        if (!info) {
          throw new Error(`Unknown method: '${method}'`);
        }

        const r = await ensureReader();

        // Apply defaults
        const fullParams = { ...(info.defaults ?? {}), ...(params ?? {}) };

        // Send init request (params as a complete IPC stream)
        const body = buildRequestIpc(info.paramsSchema, fullParams, method);
        writeFn(body);

        // Read header if method has headerSchema
        let header: Record<string, any> | null = null;
        if (info.headerSchema) {
          const headerStream = await r.readStream();
          if (headerStream) {
            for (const batch of headerStream.batches) {
              if (batch.numRows === 0) {
                dispatchLogOrError(batch, onLog);
                continue;
              }
              const rows = extractBatchRows(batch);
              if (rows.length > 0) {
                header = rows[0];
              }
            }
          }
        }

        const outputSchema = info.outputSchema ?? info.resultSchema;

        // Don't release busy here — PipeStreamSession owns the lock
        // and will release it when done
        return new PipeStreamSession({
          reader: r,
          writeFn,
          onLog,
          header,
          outputSchema,
          releaseBusy,
          setDrainPromise,
        });
      } catch (e) {
        // Init error (e.g., server raised exception during init).
        // Send empty input stream so server's drain unblocks, then
        // drain the server's output stream if needed.
        try {
          const r = await ensureReader();
          const emptySchema = new Schema([]);
          const ipc = serializeIpcStream(emptySchema, []);
          writeFn(ipc);
          // Drain server's output stream (error response + EOS)
          const outStream = await r.readStream();
          // outStream may be null or contain remaining batches — just consume
          void outStream;
        } catch {
          // Suppress errors during cleanup
        }
        releaseBusy();
        throw e;
      }
    },

    async describe(): Promise<ServiceDescription> {
      const methods = await ensureMethodCache();
      return {
        protocolName,
        methods: [...methods.values()],
      };
    },

    close(): void {
      if (closed) return;
      closed = true;
      writable.end();
    },
  };
}

// ---------------------------------------------------------------------------
// subprocessConnect — spawn a process and wrap with pipeConnect
// ---------------------------------------------------------------------------

export function subprocessConnect(
  cmd: string[],
  options?: SubprocessConnectOptions,
): RpcClient {
  const proc = Bun.spawn(cmd, {
    stdin: "pipe",
    stdout: "pipe",
    stderr: options?.stderr ?? "ignore",
    cwd: options?.cwd,
    env: options?.env ? { ...process.env, ...options.env } : undefined,
  });

  const stdout = proc.stdout as ReadableStream<Uint8Array>;

  const writable: PipeWritable = {
    write(data: Uint8Array) {
      (proc.stdin as any).write(data);
    },
    flush() {
      (proc.stdin as any).flush();
    },
    end() {
      (proc.stdin as any).end();
    },
  };

  const client = pipeConnect(stdout, writable, {
    onLog: options?.onLog,
  });

  // Wrap close to also kill the subprocess
  const originalClose = client.close;
  client.close = () => {
    originalClose.call(client);
    try {
      proc.kill();
    } catch {
      // Process may have already exited
    }
  };

  return client;
}
