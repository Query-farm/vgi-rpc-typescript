// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import {
  RecordBatch,
  Schema,
  Field,
  makeData,
  Struct,
  vectorFromArray,
} from "apache-arrow";
import { STATE_KEY } from "../constants.js";
import { RpcError } from "../errors.js";
import { ARROW_CONTENT_TYPE, serializeIpcStream } from "../http/common.js";
import {
  inferArrowType,
  dispatchLogOrError,
  extractBatchRows,
  readResponseBatches,
} from "./ipc.js";
import type { LogMessage, StreamSession } from "./types.js";

type CompressFn = (data: Uint8Array, level: number) => Uint8Array;
type DecompressFn = (data: Uint8Array) => Uint8Array;

export class HttpStreamSession implements StreamSession {
  private _baseUrl: string;
  private _prefix: string;
  private _method: string;
  private _stateToken: string | null;
  private _outputSchema: Schema;
  private _inputSchema?: Schema;
  private _onLog?: (msg: LogMessage) => void;
  private _pendingBatches: RecordBatch[];
  private _finished: boolean;
  private _header: Record<string, any> | null;
  private _compressionLevel?: number;
  private _compressFn?: CompressFn;
  private _decompressFn?: DecompressFn;

  constructor(opts: {
    baseUrl: string;
    prefix: string;
    method: string;
    stateToken: string | null;
    outputSchema: Schema;
    inputSchema?: Schema;
    onLog?: (msg: LogMessage) => void;
    pendingBatches: RecordBatch[];
    finished: boolean;
    header: Record<string, any> | null;
    compressionLevel?: number;
    compressFn?: CompressFn;
    decompressFn?: DecompressFn;
  }) {
    this._baseUrl = opts.baseUrl;
    this._prefix = opts.prefix;
    this._method = opts.method;
    this._stateToken = opts.stateToken;
    this._outputSchema = opts.outputSchema;
    this._inputSchema = opts.inputSchema;
    this._onLog = opts.onLog;
    this._pendingBatches = opts.pendingBatches;
    this._finished = opts.finished;
    this._header = opts.header;
    this._compressionLevel = opts.compressionLevel;
    this._compressFn = opts.compressFn;
    this._decompressFn = opts.decompressFn;
  }

  get header(): Record<string, any> | null {
    return this._header;
  }

  private _buildHeaders(): Record<string, string> {
    const headers: Record<string, string> = {
      "Content-Type": ARROW_CONTENT_TYPE,
    };
    if (this._compressionLevel != null) {
      headers["Content-Encoding"] = "zstd";
      headers["Accept-Encoding"] = "zstd";
    }
    return headers;
  }

  private _prepareBody(content: Uint8Array): Uint8Array {
    if (this._compressionLevel != null && this._compressFn) {
      return this._compressFn(content, this._compressionLevel);
    }
    return content;
  }

  private async _readResponse(resp: Response): Promise<Uint8Array<ArrayBuffer>> {
    let body = new Uint8Array(await resp.arrayBuffer());
    if (resp.headers.get("Content-Encoding") === "zstd" && this._decompressFn) {
      body = new Uint8Array(this._decompressFn(body));
    }
    return body;
  }

  /**
   * Send an exchange request and return the data rows.
   */
  async exchange(input: Record<string, any>[]): Promise<Record<string, any>[]> {
    if (this._stateToken === null) {
      throw new RpcError(
        "ProtocolError",
        "Stream has finished \u2014 no state token available",
        "",
      );
    }

    // We need to determine the input schema from the data.
    // Build a batch from the input rows using the output schema's field types.
    // For exchange, the input schema matches what the server expects.
    // We'll use the keys from input[0] to figure out columns.
    if (input.length === 0) {
      // Zero-row exchange: build an empty batch with state token.
      // Use inputSchema from __describe__ if available; fall back to
      // outputSchema so the server sees the correct column names.
      const zeroSchema = this._inputSchema ?? this._outputSchema;
      const emptyBatch = this._buildEmptyBatch(zeroSchema);
      const metadata = new Map<string, string>();
      metadata.set(STATE_KEY, this._stateToken);
      const batchWithMeta = new RecordBatch(
        zeroSchema,
        emptyBatch.data,
        metadata,
      );
      return this._doExchange(zeroSchema, [batchWithMeta]);
    }

    // Infer schema from first row values (input schema may differ from output).
    const keys = Object.keys(input[0]);
    const fields = keys.map((key) => {
      // Find first non-null value to infer type
      let sample: any = undefined;
      for (const row of input) {
        if (row[key] != null) { sample = row[key]; break; }
      }
      const arrowType = inferArrowType(sample);
      const nullable = input.some((row) => row[key] == null);
      return new Field(key, arrowType, nullable);
    });

    const inputSchema = new Schema(fields);
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

    const metadata = new Map<string, string>();
    metadata.set(STATE_KEY, this._stateToken);
    const batch = new RecordBatch(inputSchema, data, metadata);

    return this._doExchange(inputSchema, [batch]);
  }

  private async _doExchange(
    schema: Schema,
    batches: RecordBatch[],
  ): Promise<Record<string, any>[]> {
    const body = serializeIpcStream(schema, batches);
    const resp = await fetch(
      `${this._baseUrl}${this._prefix}/${this._method}/exchange`,
      {
        method: "POST",
        headers: this._buildHeaders(),
        body: this._prepareBody(body) as unknown as BodyInit,
      },
    );

    const responseBody = await this._readResponse(resp);
    const { batches: responseBatches } = await readResponseBatches(responseBody);

    let resultRows: Record<string, any>[] = [];
    for (const batch of responseBatches) {
      if (batch.numRows === 0) {
        // Could be log/error or state token
        dispatchLogOrError(batch, this._onLog);
        // Check for state token
        const token = batch.metadata?.get(STATE_KEY);
        if (token) {
          this._stateToken = token;
        }
        continue;
      }

      // Data batch — extract state token from metadata
      const token = batch.metadata?.get(STATE_KEY);
      if (token) {
        this._stateToken = token;
      }

      resultRows = extractBatchRows(batch);
    }

    return resultRows;
  }

  private _buildEmptyBatch(schema: Schema): RecordBatch {
    const children = schema.fields.map((f) => {
      return makeData({ type: f.type, length: 0, nullCount: 0 });
    });
    const structType = new Struct(schema.fields);
    const data = makeData({
      type: structType,
      length: 0,
      children,
      nullCount: 0,
    });
    return new RecordBatch(schema, data);
  }

  /**
   * Iterate over producer stream batches.
   */
  async *[Symbol.asyncIterator](): AsyncIterableIterator<Record<string, any>[]> {
    // Yield pre-loaded batches from init
    for (const batch of this._pendingBatches) {
      if (batch.numRows === 0) {
        dispatchLogOrError(batch, this._onLog);
        continue;
      }
      yield extractBatchRows(batch);
    }
    this._pendingBatches = [];

    if (this._finished) return;
    if (this._stateToken === null) return;

    // Follow continuation tokens
    while (true) {
      const responseBody = await this._sendContinuation(this._stateToken);
      const { batches } = await readResponseBatches(responseBody);

      let gotContinuation = false;
      for (const batch of batches) {
        if (batch.numRows === 0) {
          // Check for continuation token
          const token = batch.metadata?.get(STATE_KEY);
          if (token) {
            this._stateToken = token;
            gotContinuation = true;
            continue;
          }
          // Log/error batch
          dispatchLogOrError(batch, this._onLog);
          continue;
        }

        yield extractBatchRows(batch);
      }

      if (!gotContinuation) break;
    }
  }

  private async _sendContinuation(token: string): Promise<Uint8Array> {
    const emptySchema = new Schema([]);
    const metadata = new Map<string, string>();
    metadata.set(STATE_KEY, token);

    const structType = new Struct(emptySchema.fields);
    const data = makeData({
      type: structType,
      length: 1,
      children: [],
      nullCount: 0,
    });
    const batch = new RecordBatch(emptySchema, data, metadata);
    const body = serializeIpcStream(emptySchema, [batch]);

    const resp = await fetch(
      `${this._baseUrl}${this._prefix}/${this._method}/exchange`,
      {
        method: "POST",
        headers: this._buildHeaders(),
        body: this._prepareBody(body) as unknown as BodyInit,
      },
    );

    return this._readResponse(resp);
  }

  close(): void {
    // No-op for HTTP (stateless)
  }
}
