// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { Field, makeData, RecordBatch, Schema, Struct, vectorFromArray } from "@query-farm/apache-arrow";
import { CALL_STATE_KEY, STATE_KEY } from "../constants.js";
import { RpcError } from "../errors.js";
import { type ExternalLocationConfig, isExternalLocationBatch, resolveExternalLocation } from "../external.js";
import { ARROW_CONTENT_TYPE, serializeIpcStream } from "../http/common.js";
import { clientAcceptEncoding, VGI_ACCEPT_ENCODING_HEADER } from "../http/codec.js";
import { decodeResponseBody } from "./decode.js";
import { dispatchLogOrError, extractBatchRows, inferArrowType, readResponseBatches } from "./ipc.js";
import type { LogMessage, StreamSession } from "./types.js";

type CompressFn = (data: Uint8Array, level: number) => Promise<Uint8Array>;
type DecompressFn = (data: Uint8Array) => Promise<Uint8Array>;

/**
 * Posts an Arrow IPC request body to *url*, transparently handling
 * client-vended request externalization. Provided by the parent connection
 * so a single capability cache can drive both unary and stream call paths.
 */
export type PostFn = (url: string, body: Uint8Array) => Promise<Response>;

/**
 * One producer batch's rows paired with the continuation token that resumes
 * the stream AFTER that batch. Returned by {@link HttpStreamSession.nextWithToken}.
 */
export interface RowsWithToken {
  /** The data batch's rows. */
  rows: Record<string, any>[];
  /**
   * The resume token continuing the stream after this batch — the worker's
   * own serialized producer state — or `null` when the producer emitted this
   * batch as its final turn (no further continuation).
   *
   * Since a stream's state may travel as two tokens (call + cursor), this is
   * the *pair*, packed into one opaque string by {@link packResumeToken}.
   * Treat it as unstructured text; only {@link HttpStreamSession.seekToToken}
   * and the client's `resumeStream` need to know its shape.
   */
  token: string | null;
}

/**
 * Pack a stream's cursor and call tokens into one opaque resume blob.
 *
 * Both halves have to travel: a node resuming from this token may have no
 * cached knowledge of the stream, and the server never re-issues the call
 * token.
 *
 * Layout is `<cursorLen>:<cursor><call>`, and a stream with no call token
 * packs to the bare cursor. Both tokens are base64, whose alphabet contains
 * no `:`, so a bare cursor can never be mistaken for a packed pair — which is
 * what keeps tokens minted before the split readable.
 */
export function packResumeToken(cursor: string, callToken: string | null): string {
  return callToken === null ? cursor : `${cursor.length}:${cursor}${callToken}`;
}

/**
 * Unpack a blob produced by {@link packResumeToken}.
 *
 * A blob with no length prefix is a bare cursor — either from a server that
 * does not split its stream state, or from a client predating the split.
 */
export function unpackResumeToken(token: string): { cursor: string; callToken: string | null } {
  const sep = token.indexOf(":");
  if (sep < 0) return { cursor: token, callToken: null };
  const cursorLen = Number(token.slice(0, sep));
  if (!Number.isInteger(cursorLen) || cursorLen < 0) return { cursor: token, callToken: null };
  const rest = token.slice(sep + 1);
  if (cursorLen > rest.length) return { cursor: token, callToken: null };
  const call = rest.slice(cursorLen);
  return { cursor: rest.slice(0, cursorLen), callToken: call === "" ? null : call };
}

/**
 * {@link StreamSession} implementation for the HTTP transport. Stream state is
 * carried statelessly across requests via an HMAC state token: each
 * {@link HttpStreamSession.exchange} or producer-continuation POST sends the
 * current token and receives the next one in the response metadata.
 */
export class HttpStreamSession implements StreamSession {
  private _baseUrl: string;
  private _prefix: string;
  private _method: string;
  private _stateToken: string | null;
  /**
   * The stream's call token: handed over once by `/init` and echoed on every
   * subsequent request. The server never re-issues it, so this is the only
   * copy once the init response is parsed.
   */
  private _callStateToken: string | null;
  private _outputSchema: Schema;
  private _inputSchema?: Schema;
  private _onLog?: (msg: LogMessage) => void;
  private _pendingBatches: RecordBatch[];
  private _finished: boolean;
  private _header: Record<string, any> | null;
  private _compressionLevel?: number;
  private _compressFn?: CompressFn;
  private _decompressFn?: DecompressFn;
  private _authorization?: string;
  private _externalConfig?: ExternalLocationConfig;
  private _postFn?: PostFn;

  constructor(opts: {
    baseUrl: string;
    prefix: string;
    method: string;
    stateToken: string | null;
    callStateToken?: string | null;
    outputSchema: Schema;
    inputSchema?: Schema;
    onLog?: (msg: LogMessage) => void;
    pendingBatches: RecordBatch[];
    finished: boolean;
    header: Record<string, any> | null;
    compressionLevel?: number;
    compressFn?: CompressFn;
    decompressFn?: DecompressFn;
    authorization?: string;
    externalConfig?: ExternalLocationConfig;
    postFn?: PostFn;
  }) {
    this._baseUrl = opts.baseUrl;
    this._prefix = opts.prefix;
    this._method = opts.method;
    this._stateToken = opts.stateToken;
    this._callStateToken = opts.callStateToken ?? null;
    this._outputSchema = opts.outputSchema;
    this._inputSchema = opts.inputSchema;
    this._onLog = opts.onLog;
    this._pendingBatches = opts.pendingBatches;
    this._finished = opts.finished;
    this._header = opts.header;
    this._compressionLevel = opts.compressionLevel;
    this._compressFn = opts.compressFn;
    this._decompressFn = opts.decompressFn;
    this._authorization = opts.authorization;
    this._externalConfig = opts.externalConfig;
    this._postFn = opts.postFn;
  }

  private async _post(url: string, body: Uint8Array): Promise<Response> {
    if (this._postFn) return this._postFn(url, body);
    return fetch(url, {
      method: "POST",
      headers: this._buildHeaders(),
      body: (await this._prepareBody(body)) as unknown as BodyInit,
    });
  }

  /** The stream's one-time header row, or `null` if the method declares no header. */
  get header(): Record<string, any> | null {
    return this._header;
  }

  /**
   * Build request metadata carrying the cursor token and the call token.
   *
   * The call token is echoed on every request because the server does not
   * re-issue it; a request that omitted it would still succeed while the
   * server's call-state cache is warm and fail once it is not — exactly the
   * kind of load-dependent bug worth designing out.
   */
  private _tokenMetadata(token: string): Map<string, string> {
    const metadata = new Map<string, string>();
    metadata.set(STATE_KEY, token);
    if (this._callStateToken !== null) {
      metadata.set(CALL_STATE_KEY, this._callStateToken);
    }
    return metadata;
  }

  /** Encode this session's current position as one opaque resume blob. */
  private _resumeToken(): string | null {
    return this._stateToken === null ? null : packResumeToken(this._stateToken, this._callStateToken);
  }

  private _buildHeaders(): Record<string, string> {
    const headers: Record<string, string> = {
      "Content-Type": ARROW_CONTENT_TYPE,
    };
    if (this._compressionLevel != null && this._compressFn) {
      headers["Content-Encoding"] = "zstd";
    }
    if (this._compressionLevel != null && this._decompressFn) {
      headers["Accept-Encoding"] = "zstd";
    }
    // See connect.ts: states what we can decode, regardless of request compression.
    headers[VGI_ACCEPT_ENCODING_HEADER] = clientAcceptEncoding(this._decompressFn != null);
    if (this._authorization) {
      headers.Authorization = this._authorization;
    }
    return headers;
  }

  private async _prepareBody(content: Uint8Array): Promise<Uint8Array> {
    if (this._compressionLevel != null && this._compressFn) {
      return await this._compressFn(content, this._compressionLevel);
    }
    return content;
  }

  private async _readResponse(resp: Response): Promise<Uint8Array<ArrayBuffer>> {
    const body = new Uint8Array(await resp.arrayBuffer());
    return new Uint8Array(await decodeResponseBody(resp.headers, body, this._decompressFn));
  }

  /**
   * Send an exchange request and return the data rows.
   */
  async exchange(input: Record<string, any>[]): Promise<Record<string, any>[]> {
    if (this._stateToken === null) {
      throw new RpcError("ProtocolError", "Stream has finished \u2014 no state token available", "");
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
      const batchWithMeta = new RecordBatch(zeroSchema, emptyBatch.data, this._tokenMetadata(this._stateToken));
      return this._doExchange(zeroSchema, [batchWithMeta]);
    }

    // Infer schema from first row values (input schema may differ from output).
    const keys = Object.keys(input[0]);
    const fields = keys.map((key) => {
      // Find first non-null value to infer type
      let sample: any;
      for (const row of input) {
        if (row[key] != null) {
          sample = row[key];
          break;
        }
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

    const batch = new RecordBatch(inputSchema, data, this._tokenMetadata(this._stateToken));

    return this._doExchange(inputSchema, [batch]);
  }

  private async _doExchange(schema: Schema, batches: RecordBatch[]): Promise<Record<string, any>[]> {
    const body = serializeIpcStream(schema, batches);
    const resp = await this._post(`${this._baseUrl}${this._prefix}/${this._method}/exchange`, body);
    if (resp.status === 401) {
      throw new RpcError("AuthenticationError", "Authentication required", "");
    }

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
    for (let batch of this._pendingBatches) {
      if (batch.numRows === 0) {
        if (isExternalLocationBatch(batch)) {
          batch = (await resolveExternalLocation(batch as any, this._externalConfig)) as any;
        } else {
          dispatchLogOrError(batch, this._onLog);
          continue;
        }
      }
      yield extractBatchRows(batch);
    }
    this._pendingBatches = [];

    if (this._finished) return;
    if (this._stateToken === null) return;

    // Follow continuation tokens
    while (true) {
      const stateToken = this._stateToken;
      if (stateToken === null) return;
      const responseBody = await this._sendContinuation(stateToken);
      const { batches } = await readResponseBatches(responseBody);

      let gotContinuation = false;
      for (let batch of batches) {
        if (batch.numRows === 0) {
          // Check for continuation token
          const token = batch.metadata?.get(STATE_KEY);
          if (token) {
            this._stateToken = token;
            gotContinuation = true;
            continue;
          }
          // Check for external location pointer
          if (isExternalLocationBatch(batch)) {
            batch = (await resolveExternalLocation(batch as any, this._externalConfig)) as any;
          } else {
            // Log/error batch
            dispatchLogOrError(batch, this._onLog);
            continue;
          }
        }

        yield extractBatchRows(batch);
      }

      if (!gotContinuation) break;
    }
  }

  /**
   * Read one producer batch and surface the worker's continuation token.
   *
   * Reads exactly one data batch and returns it paired with the resume token
   * that continues the stream AFTER that batch — the worker's own serialized
   * producer state. A fresh session positioned at that token (see
   * {@link HttpStreamSession.seekToToken} / the client's `resumeStream`)
   * resumes on any node, which is the basis for stateless, load-balanced
   * relays that must not pin a scan to one process.
   *
   * Returns `null` at end-of-stream. Requires per-batch continuation tokens
   * (the default server behaviour — i.e. the worker is not configured with
   * `max_response_bytes`); throws if a single response carries more than one
   * data batch (coarser-than-batch resume is not representable here).
   *
   * Drives the same wire protocol as async iteration but yields one
   * `{ rows, token }` per call instead of auto-following the token. Do not
   * interleave with iteration/`exchange` on the same session.
   *
   * Mirrors Python's `HttpStreamSession.next_with_token`.
   */
  async nextWithToken(): Promise<RowsWithToken | null> {
    const multi =
      "nextWithToken requires one data batch per response; the upstream " +
      "worker buffered multiple (configured max_response_bytes?)";

    // Init may have preloaded data batches; their resume point is the
    // current state token. Zero-row log/error batches deferred from init are
    // dispatched in order (an EXCEPTION batch throws, like iteration would).
    while (this._pendingBatches.length > 0) {
      let batch = this._pendingBatches.shift()!;
      if (batch.numRows === 0) {
        if (isExternalLocationBatch(batch)) {
          batch = (await resolveExternalLocation(batch as any, this._externalConfig)) as any;
        } else {
          dispatchLogOrError(batch, this._onLog);
          continue;
        }
      }
      if (this._pendingBatches.some((b) => b.numRows > 0 || isExternalLocationBatch(b))) {
        throw new Error(multi);
      }
      return { rows: extractBatchRows(batch), token: this._resumeToken() };
    }

    if (this._finished || this._stateToken === null) {
      this._finished = true;
      return null;
    }

    const responseBody = await this._sendContinuation(this._stateToken);
    const { batches } = await readResponseBatches(responseBody);

    let dataRows: Record<string, any>[] | null = null;
    let nextToken: string | null = null;
    for (let batch of batches) {
      if (batch.numRows === 0) {
        // Continuation token (zero-row batch with STATE_KEY).
        const token = batch.metadata?.get(STATE_KEY);
        if (token) {
          nextToken = token;
          continue;
        }
        if (isExternalLocationBatch(batch)) {
          batch = (await resolveExternalLocation(batch as any, this._externalConfig)) as any;
        } else {
          dispatchLogOrError(batch, this._onLog);
          continue;
        }
      }
      if (dataRows !== null) {
        throw new Error(multi);
      }
      dataRows = extractBatchRows(batch);
    }

    this._stateToken = nextToken;
    if (dataRows === null) {
      // No data this turn -> the producer finished (out.finish(), no token).
      this._finished = true;
      return null;
    }
    return { rows: dataRows, token: this._resumeToken() };
  }

  /**
   * Reposition a freshly-initialised session to resume from `token`.
   *
   * Discards any init-preloaded batches and points the session at the given
   * resume token (as returned by {@link HttpStreamSession.nextWithToken}), so
   * the next `nextWithToken()` continues from exactly there. Used to resume a
   * scan on a new process/node — which is why the call token travels inside
   * the blob too: that node may never have seen this stream's `/init`.
   * Mirrors Python's `seek_to_token`.
   */
  seekToToken(token: string): void {
    const { cursor, callToken } = unpackResumeToken(token);
    this._pendingBatches = [];
    this._stateToken = cursor;
    this._callStateToken = callToken;
    this._finished = false;
  }

  private async _sendContinuation(token: string): Promise<Uint8Array> {
    const emptySchema = new Schema([]);
    const metadata = this._tokenMetadata(token);

    const structType = new Struct(emptySchema.fields);
    const data = makeData({
      type: structType,
      length: 1,
      children: [],
      nullCount: 0,
    });
    const batch = new RecordBatch(emptySchema, data, metadata);
    const body = serializeIpcStream(emptySchema, [batch]);

    const resp = await this._post(`${this._baseUrl}${this._prefix}/${this._method}/exchange`, body);
    if (resp.status === 401) {
      throw new RpcError("AuthenticationError", "Authentication required", "");
    }

    return this._readResponse(resp);
  }

  /** No-op: the HTTP transport is stateless, so there is nothing to tear down. */
  close(): void {
    // No-op for HTTP (stateless)
  }
}
