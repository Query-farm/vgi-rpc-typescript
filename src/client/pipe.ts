// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import {
  Field,
  makeData,
  RecordBatch,
  RecordBatchStreamWriter,
  Schema,
  Struct,
  vectorFromArray,
} from "@query-farm/apache-arrow";
import { CANCEL_KEY } from "../constants.js";
import { RpcError } from "../errors.js";
import { type ExternalLocationConfig, isExternalLocationBatch, resolveExternalLocation } from "../external.js";
import { serializeIpcStream } from "../http/common.js";
import {
  decodeProtocolList,
  decodeServiceDescription,
  type ProtocolListDesc,
  REFLECTION_DESCRIBE,
  REFLECTION_LIST_PROTOCOLS,
} from "../reflection.js";
import { IpcStreamReader } from "../wire/reader.js";
import { MAX_STREAM_CHUNK } from "../wire/writer.js";
import type { RpcClient } from "./connect.js";
import {
  adaptServiceDescription,
  type MethodInfo,
  pickApplicationProtocol,
  reflectionRequest,
  reflectionResult,
  type ServiceDescription,
} from "./introspect.js";
import { buildRequestIpc, dispatchLogOrError, extractBatchRows, inferArrowType } from "./ipc.js";
import type { RawBatch, RawBatchWithToken, RawStreamSession } from "./raw.js";
import { rawBatchOf, rawInputBatch } from "./raw-util.js";
import type {
  ExchangeInput,
  LogMessage,
  PipeConnectOptions,
  StreamSession,
  SubprocessConnectOptions,
} from "./types.js";

// ---------------------------------------------------------------------------
// Writable abstraction
// ---------------------------------------------------------------------------

interface PipeWritable {
  write(data: Uint8Array): void;
  flush?(): void;
  end(): void;
}

type WriteFn = (bytes: Uint8Array) => void;

function fieldsMatch(left: Field, right: Field): boolean {
  if (left.name !== right.name || left.nullable !== right.nullable || String(left.type) !== String(right.type)) {
    return false;
  }
  // arrow-js leaves `children` unset on a primitive type rather than giving
  // it an empty array, so reading `.length` off it throws the moment two
  // declared exchange batches are compared -- which only happens on the
  // *second* exchange of a session, and so stayed invisible.
  const leftChildren = left.type.children ?? [];
  const rightChildren = right.type.children ?? [];
  return (
    leftChildren.length === rightChildren.length &&
    leftChildren.every((child: Field, index: number) => fieldsMatch(child, rightChildren[index]))
  );
}

function schemasMatch(left: Schema, right: Schema): boolean {
  return (
    left.fields.length === right.fields.length &&
    left.fields.every((field, index) => fieldsMatch(field, right.fields[index]))
  );
}

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

/**
 * {@link StreamSession} implementation for the pipe/subprocess transport.
 * Drives lockstep streaming over a single bidirectional pipe: each
 * {@link PipeStreamSession.exchange} or iteration step writes one input batch
 * and reads one output batch. Holds the connection's single-threaded busy lock
 * until closed.
 */
export class PipeStreamSession implements StreamSession, RawStreamSession {
  /**
   * Opening the reader blocks until the peer's first IPC schema message
   * arrives, and a headerless producer sends nothing until it has been
   * ticked — so a session that resolved its reader eagerly would deadlock on
   * open. Held as a thunk and resolved on the first read instead.
   */
  private _openReader: () => Promise<IpcStreamReader>;
  private _readerCache: IpcStreamReader | null;
  private _writeFn: WriteFn;
  private _onLog?: (msg: LogMessage) => void;
  private _header: Record<string, any> | null;
  private _rawHeader: RawBatch | null;
  private _inputWriter: PipeIncrementalWriter | null = null;
  private _inputSchema: Schema | null = null;
  private _outputStreamOpened = false;
  private _closed = false;
  private _outputSchema: Schema;
  private _releaseBusy: () => void;
  private _setDrainPromise: (p: Promise<void>) => void;
  private _externalConfig?: ExternalLocationConfig;

  constructor(opts: {
    reader: IpcStreamReader | (() => Promise<IpcStreamReader>);
    writeFn: WriteFn;
    onLog?: (msg: LogMessage) => void;
    header: Record<string, any> | null;
    rawHeader?: RawBatch | null;
    outputSchema: Schema;
    releaseBusy: () => void;
    setDrainPromise: (p: Promise<void>) => void;
    externalConfig?: ExternalLocationConfig;
  }) {
    this._openReader = typeof opts.reader === "function" ? opts.reader : async () => opts.reader as IpcStreamReader;
    this._readerCache = typeof opts.reader === "function" ? null : opts.reader;
    this._writeFn = opts.writeFn;
    this._onLog = opts.onLog;
    this._header = opts.header;
    this._rawHeader = opts.rawHeader ?? null;
    this._outputSchema = opts.outputSchema;
    this._releaseBusy = opts.releaseBusy;
    this._setDrainPromise = opts.setDrainPromise;
    this._externalConfig = opts.externalConfig;
  }

  /** The stream's one-time header row, or `null` if the method declares no header. */
  get header(): Record<string, any> | null {
    return this._header;
  }

  /** The stream's header batch and its custom metadata, undecoded. */
  get rawHeader(): RawBatch | null {
    return this._rawHeader;
  }

  /** The connection's IPC reader, opened on first use. */
  private async _reader(): Promise<IpcStreamReader> {
    this._readerCache ??= await this._openReader();
    return this._readerCache;
  }

  /**
   * Read output batches from the server until a data batch is found.
   * Dispatches log/error batches along the way.
   * Returns null when server closes output stream (EOS).
   */
  private async _readOutputBatch(): Promise<RecordBatch | null> {
    while (true) {
      const batch = await (await this._reader()).readNextBatch();
      if (batch === null) return null; // Server closed output stream

      if (batch.numRows === 0) {
        // Check for external location pointer batch
        if (isExternalLocationBatch(batch as any)) {
          return (await resolveExternalLocation(batch as any, this._externalConfig, this._onLog)) as any;
        }
        // Check if it's a log/error batch. If so, dispatch and continue.
        // Otherwise it's a zero-row data batch — return it.
        if (dispatchLogOrError(batch as any, this._onLog)) {
          continue;
        }
      }

      return batch as any;
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
    const schema = await (await this._reader()).openNextStream();
    if (!schema) {
      throw new RpcError("ProtocolError", "Expected output stream but got EOF", "");
    }
  }

  /** Send one producer tick, preserving application message metadata. */
  async tick(metadata?: ReadonlyMap<string, string>): Promise<Record<string, any>[]> {
    const outputBatch = await this._tickBatch(metadata);
    return outputBatch === null ? [] : extractBatchRows(outputBatch);
  }

  /** Send one producer tick and return the server's batch undecoded. */
  async tickRaw(metadata?: ReadonlyMap<string, string>): Promise<RawBatch | null> {
    const outputBatch = await this._tickBatch(metadata);
    return outputBatch === null ? null : rawBatchOf(outputBatch);
  }

  /**
   * A byte-stream transport carries no resumable stream state, so the token
   * is always `null`. Declared so one caller can drive either transport.
   */
  async nextWithTokenRaw(): Promise<RawBatchWithToken | null> {
    const item = await this.tickRaw();
    return item === null ? null : { item, token: null };
  }

  /** One producer turn: write the tick batch, read the server's answer. */
  private async _tickBatch(metadata?: ReadonlyMap<string, string>): Promise<RecordBatch | null> {
    if (this._closed) {
      throw new RpcError("ProtocolError", "Stream session is closed", "");
    }
    const tickSchema = new Schema([]);
    if (!this._inputWriter) {
      this._inputWriter = new PipeIncrementalWriter(this._writeFn, tickSchema);
    }
    const tickData = makeData({ type: new Struct([]), length: 0, children: [], nullCount: 0 });
    const tickBatch = new RecordBatch(tickSchema, tickData, metadata ? new Map(metadata) : undefined);
    this._inputWriter.write(tickBatch);
    await this._ensureOutputStream();
    let outputBatch: RecordBatch | null;
    try {
      outputBatch = await this._readOutputBatch();
    } catch (e) {
      // A mid-stream error batch throws out of here, and the connection's
      // busy lock is held by this session. Without the unwind the pipe stays
      // wedged and the *next* call on the same connection fails with
      // "transport is busy" — reporting the recovery as the failure.
      // `exchange()` already did this; `tick()` did not, because nothing drove
      // it outside the iterator, whose `finally` covers the same ground.
      await this._cleanup();
      throw e;
    }
    if (outputBatch === null) {
      this._closed = true;
      this._inputWriter.close();
      this._inputWriter = null;
      this._releaseBusy();
      return null;
    }
    return outputBatch;
  }

  /**
   * Send one encoded batch with its custom metadata and read the reply.
   *
   * The declared-batch branch of {@link PipeStreamSession.exchange} without
   * the row decoding: input schema and buffers cross verbatim, and the
   * server's answer comes back as it was encoded.
   */
  async exchangeRaw(input: RawBatch): Promise<RawBatch | null> {
    if (this._closed) {
      throw new RpcError("ProtocolError", "Stream session is closed", "");
    }
    const batch = rawInputBatch(input);
    const inputSchema = batch.schema;
    if (this._inputSchema && !schemasMatch(this._inputSchema, inputSchema)) {
      throw new RpcError(
        "ProtocolError",
        `Exchange input schema changed: expected ${this._inputSchema}, got ${inputSchema}`,
        "",
      );
    }
    this._inputSchema ??= inputSchema;
    if (!this._inputWriter) {
      this._inputWriter = new PipeIncrementalWriter(this._writeFn, inputSchema);
    }
    this._inputWriter.write(batch);
    await this._ensureOutputStream();
    try {
      const outputBatch = await this._readOutputBatch();
      return outputBatch === null ? null : rawBatchOf(outputBatch);
    } catch (e) {
      await this._cleanup();
      throw e;
    }
  }

  /**
   * Signal the server to stop processing and discard the stream's state.
   *
   * Writes a zero-row batch carrying `vgi_rpc.cancel`, closes the input
   * stream, and drains whatever the server still had queued. Idempotent and
   * best-effort: a transport that has already failed is not worth a second
   * failure during teardown. Mirrors Python's `StreamSession.cancel`.
   */
  async cancel(): Promise<void> {
    if (this._closed) return;
    this._closed = true;
    const cancelMetadata = new Map([[CANCEL_KEY, "1"]]);
    try {
      const schema = this._inputSchema ?? new Schema([]);
      if (!this._inputWriter) {
        this._inputWriter = new PipeIncrementalWriter(this._writeFn, schema);
      }
      const children = schema.fields.map((f) => makeData({ type: f.type, length: 0, nullCount: 0 }));
      const data = makeData({ type: new Struct(schema.fields), length: 0, children, nullCount: 0 });
      this._inputWriter.write(new RecordBatch(schema, data, cancelMetadata));
      this._inputWriter.close();
      this._inputWriter = null;
    } catch {
      this._releaseBusy();
      return;
    }
    try {
      if (!this._outputStreamOpened) {
        this._outputStreamOpened = true;
        const schema = await (await this._reader()).openNextStream();
        if (!schema) {
          this._releaseBusy();
          return;
        }
      }
      while ((await (await this._reader()).readNextBatch()) !== null) {}
    } catch {
      // Suppress errors during drain — the stream is over either way.
    }
    this._releaseBusy();
  }

  /**
   * Send an exchange request and return the data rows.
   */
  async exchange(input: ExchangeInput): Promise<Record<string, any>[]> {
    if (this._closed) {
      throw new RpcError("ProtocolError", "Stream session is closed", "");
    }

    // Build input batch
    let inputSchema: Schema;
    let batch: RecordBatch;

    if (!Array.isArray(input)) {
      inputSchema = input.schema;
      batch = input;

      // One raw exchange session is one Arrow IPC stream, whose schema is
      // fixed by its first batch. Refuse a later explicit batch whose declared
      // schema differs instead of writing bytes the peer must misinterpret.
      if (this._inputSchema && !schemasMatch(this._inputSchema, inputSchema)) {
        throw new RpcError(
          "ProtocolError",
          `Exchange input schema changed: expected ${this._inputSchema}, got ${inputSchema}`,
          "",
        );
      }
      this._inputSchema ??= inputSchema;
    } else if (input.length === 0) {
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
        let sample: any;
        for (const row of input) {
          if (row[key] != null) {
            sample = row[key];
            break;
          }
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
        while ((await (await this._reader()).readNextBatch()) !== null) {}
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

      while (true) {
        const rows = await this.tick();
        if (this._closed) {
          // Server finished — EOS on output stream
          break;
        }
        yield rows;
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
          while ((await (await this._reader()).readNextBatch()) !== null) {}
        }
      } catch {
        // Suppress errors during drain
      }
      this._closed = true;
      this._releaseBusy();
    }
  }

  /**
   * End the stream: close the input side (or send an empty stream if nothing
   * was sent yet) and drain the server's remaining output in the background,
   * releasing the connection's busy lock once the drain completes.
   */
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
          const schema = await (await this._reader()).openNextStream();
          if (schema) {
            while ((await (await this._reader()).readNextBatch()) !== null) {}
          }
        } else {
          while ((await (await this._reader()).readNextBatch()) !== null) {}
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

/**
 * Connect to a vgi-rpc server over a raw bidirectional pipe (a readable stream
 * of server output plus a writable for client input). The connection is
 * single-threaded: only one call or stream may be in flight at a time. The
 * first introspection request is sent before the reader is opened to avoid
 * deadlock.
 */
export function pipeConnect(
  readable: ReadableStream<Uint8Array>,
  writable: PipeWritable,
  options?: PipeConnectOptions,
): RpcClient {
  const onLog = options?.onLog;
  const externalConfig = options?.externalLocation;

  let reader: IpcStreamReader | null = null;
  let readerPromise: Promise<IpcStreamReader> | null = null;
  let methodCache: Map<string, MethodInfo> | null = null;
  // The routing key, learned from the introspection response. Reflection has a
  // fixed name, so it is the bootstrap: ask the one protocol whose name a
  // client can know a priori what else the server speaks, then address that.
  let protocolName = "";
  let serverProtocolVersion = "";
  let describedHash = "";
  let hostedProtocols: string[] = [];
  let describedServerId: string | undefined;
  let describedRequestVersion: string | undefined;
  // Naming a protocol skips the `list_protocols` hop -- worth it against a
  // server whose primary is not the one this client wants, and against one
  // hosting several.
  const requestedProtocol = options?.protocol;
  let _busy = false;
  let _drainPromise: Promise<void> | null = null;
  let closed = false;

  // Offer the bytes in MAX_STREAM_CHUNK pieces. Every writable behind this —
  // Bun's subprocess-stdin FileSink, a `net.Socket` (tcp/unix) — bottoms out
  // in one `send(2)`, which fails with EINVAL on macOS once a single call
  // carries more than 2 GiB. `subarray` is a view, so this costs nothing for
  // the ordinary small request. `do`, not `while`, so a zero-length write
  // still reaches the writable as it did before.
  const writeFn: WriteFn = (bytes: Uint8Array) => {
    let offset = 0;
    do {
      const end = Math.min(offset + MAX_STREAM_CHUNK, bytes.length);
      writable.write(bytes.subarray(offset, end));
      offset = end;
    } while (offset < bytes.length);
    writable.flush?.();
  };

  // The IpcStreamReader.create() blocks until the first IPC schema arrives
  // on the readable. To avoid deadlock, we must send our first request (the
  // first reflection call) BEFORE opening the reader. After that, the response
  // bytes are in the pipe buffer and the reader can consume them.
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
      // One unary reflection call. The *first* one has to be written before
      // the reader is opened: IpcStreamReader.create() blocks on reader.open()
      // reading the first schema message, and the server writes nothing until
      // it has a request. Sending first is what avoids the deadlock.
      const call = async (method: string, protocol?: string): Promise<Uint8Array> => {
        writeFn(reflectionRequest(method, protocol));
        const r = await ensureReader();
        // ensureReader() consumed the schema via open(). readStream() — on the
        // first call (initialized=false) — returns the current stream without
        // calling reset().
        const response = await r.readStream();
        if (!response) {
          throw new RpcError("TransportError", `EOF reading the '${method}' reflection response`, "");
        }
        return reflectionResult(response.batches as any, onLog, externalConfig);
      };

      // Two round trips: what does this server host, then describe one of
      // them. The first is unavoidable now that a server may host several
      // protocols -- there is no longer a single "the" protocol to ask about
      // without asking. An explicit `protocol` skips it.
      let listing: ProtocolListDesc | undefined;
      let target = requestedProtocol;
      if (!target) {
        listing = decodeProtocolList(await call(REFLECTION_LIST_PROTOCOLS));
        target = pickApplicationProtocol(listing);
      }
      const desc = adaptServiceDescription(decodeServiceDescription(await call(REFLECTION_DESCRIBE, target)), listing);

      protocolName = desc.protocolName;
      serverProtocolVersion = desc.protocolVersion;
      describedHash = desc.protocolHash;
      hostedProtocols = desc.hostedProtocols;
      describedServerId = desc.serverId;
      describedRequestVersion = desc.requestVersion;
      methodCache = new Map(desc.methods.map((m) => [m.name, m]));
      return methodCache;
    } finally {
      releaseBusy();
    }
  }

  return {
    async call(method: string, params?: Record<string, any>): Promise<Record<string, any> | null> {
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
        const body = buildRequestIpc(info.paramsSchema, fullParams, method, {
          protocolVersion: serverProtocolVersion,
          protocol: protocolName,
        });
        writeFn(body);

        // Read response
        const response = await r.readStream();
        if (!response) {
          throw new Error("EOF reading response");
        }

        // Process batches: dispatch logs, resolve external pointers, find result
        let resultBatch: RecordBatch | null = null;
        for (let batch of response.batches as any[]) {
          if (batch.numRows === 0) {
            if (isExternalLocationBatch(batch)) {
              batch = await resolveExternalLocation(batch, externalConfig, onLog);
            } else {
              dispatchLogOrError(batch, onLog);
              continue;
            }
          }
          if (resultBatch !== null) {
            throw new RpcError("ProtocolError", "A unary response returned more than one data batch", "");
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

    async callRaw(_method: string, input: RawBatch): Promise<RawBatch | null> {
      await acquireBusy();
      try {
        // No introspection: a byte-stream request carries its own routing —
        // `vgi_rpc.protocol` and `vgi_rpc.method` are in `input.metadata`, and
        // the caller holding encoded Arrow already knows the schema. The
        // first write still precedes `ensureReader()` for the same reason
        // `ensureMethodCache` does: the reader blocks on a schema message the
        // server does not send until it has a request.
        const batch = rawInputBatch(input);
        writeFn(serializeIpcStream(batch.schema, [batch]));
        const r = await ensureReader();
        const response = await r.readStream();
        if (!response) {
          throw new RpcError("TransportError", "EOF reading response", "");
        }
        let resultBatch: RecordBatch | null = null;
        for (let batch of response.batches as any[]) {
          if (batch.numRows === 0) {
            if (isExternalLocationBatch(batch)) {
              batch = await resolveExternalLocation(batch, externalConfig, onLog);
            } else {
              dispatchLogOrError(batch, onLog);
              continue;
            }
          }
          if (resultBatch !== null) {
            throw new RpcError("ProtocolError", "A unary response returned more than one data batch", "");
          }
          resultBatch = batch;
        }
        return resultBatch === null ? null : rawBatchOf(resultBatch);
      } finally {
        releaseBusy();
      }
    },

    async streamRaw(
      _method: string,
      input: RawBatch,
      options: { isExchange: boolean; hasHeader: boolean },
    ): Promise<RawStreamSession> {
      await acquireBusy();
      try {
        const batch = rawInputBatch(input);
        writeFn(serializeIpcStream(batch.schema, [batch]));

        // Only a header-bearing method writes anything before its first tick,
        // so only that case may open the reader here. Opening it for a
        // headerless producer would block on a schema message the server does
        // not send until the client has ticked -- a deadlock the row-oriented
        // path never hits only because introspection opened the reader first.
        let rawHeader: RawBatch | null = null;
        if (options.hasHeader) {
          const headerStream = await (await ensureReader()).readStream();
          if (headerStream) {
            for (const headerBatch of headerStream.batches as any[]) {
              if (headerBatch.numRows === 0) {
                // A header is data like any other, so a server that
                // externalizes its responses sends it as a pointer — zero-row,
                // like a log batch, and dropped by the check below until it is
                // resolved first. `connect.ts` learned this over HTTP; the
                // byte-stream reader is separate code and had not.
                if (isExternalLocationBatch(headerBatch)) {
                  rawHeader ??= rawBatchOf((await resolveExternalLocation(headerBatch, externalConfig, onLog)) as any);
                  continue;
                }
                dispatchLogOrError(headerBatch, onLog);
                continue;
              }
              rawHeader ??= rawBatchOf(headerBatch);
            }
          }
        }

        return new PipeStreamSession({
          reader: ensureReader,
          writeFn,
          onLog,
          header: null,
          rawHeader,
          outputSchema: new Schema([]),
          releaseBusy,
          setDrainPromise,
          externalConfig,
        });
      } catch (e) {
        // Same unwind as `stream()`: the server is blocked reading our input
        // stream, so send an empty one and drain its output before releasing.
        try {
          const r = await ensureReader();
          writeFn(serializeIpcStream(new Schema([]), []));
          void (await r.readStream());
        } catch {
          // Suppress errors during cleanup.
        }
        releaseBusy();
        throw e;
      }
    },

    async stream(method: string, params?: Record<string, any>): Promise<StreamSession> {
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
        const body = buildRequestIpc(info.paramsSchema, fullParams, method, {
          protocolVersion: serverProtocolVersion,
          protocol: protocolName,
        });
        writeFn(body);

        // Read header if method has headerSchema
        let header: Record<string, any> | null = null;
        if (info.headerSchema) {
          const headerStream = await r.readStream();
          if (headerStream) {
            for (let batch of headerStream.batches as any[]) {
              if (batch.numRows === 0) {
                // See the raw-stream reader above: an externalized header
                // arrives as a zero-row pointer, and resolving it is what
                // keeps `session.header` from being null against precisely
                // the servers whose headers are big enough to externalize.
                if (isExternalLocationBatch(batch)) {
                  batch = await resolveExternalLocation(batch, externalConfig, onLog);
                } else {
                  dispatchLogOrError(batch, onLog);
                  continue;
                }
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
          externalConfig,
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
        protocolVersion: serverProtocolVersion,
        protocolHash: describedHash,
        hostedProtocols,
        methods: [...methods.values()],
        serverId: describedServerId,
        requestVersion: describedRequestVersion,
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

/**
 * Spawn a server process (via `Bun.spawn`) and connect to it over its
 * stdin/stdout using {@link pipeConnect}. The returned client's
 * {@link RpcClient.close} also kills the subprocess.
 */
export function subprocessConnect(cmd: string[], options?: SubprocessConnectOptions): RpcClient {
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
    externalLocation: options?.externalLocation,
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
