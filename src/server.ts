// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { RecordBatchStreamWriter, Schema } from "@query-farm/apache-arrow";
import { DESCRIBE_METHOD_NAME } from "./constants.js";
import { buildDescribeBatch } from "./dispatch/describe.js";
import { dispatchStream } from "./dispatch/stream.js";
import { dispatchUnary } from "./dispatch/unary.js";
import { RpcError, VersionError } from "./errors.js";
import type { ExternalLocationConfig } from "./external.js";
import type { Protocol } from "./protocol.js";
import { type CallStatistics, type DispatchHook, type DispatchInfo, MethodType } from "./types.js";
import { IpcStreamReader } from "./wire/reader.js";
import { applyDefaults, parseRequest } from "./wire/request.js";
import { buildErrorBatch } from "./wire/response.js";
import { IpcStreamWriter } from "./wire/writer.js";

const EMPTY_SCHEMA = new Schema([]);

function randomStreamId(): string {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  let out = "";
  for (let i = 0; i < bytes.length; i++) {
    out += bytes[i].toString(16).padStart(2, "0");
  }
  return out;
}

/**
 * RPC server that reads Arrow IPC requests from stdin and writes responses to stdout.
 * Supports unary and streaming (producer/exchange) methods.
 */
export class VgiRpcServer {
  private protocol: Protocol;
  private enableDescribe: boolean;
  private serverId: string;
  private describeBatch: import("@query-farm/apache-arrow").RecordBatch | null = null;
  private protocolHash: string;
  private protocolVersion: string;
  private dispatchHook: DispatchHook | null = null;
  private externalConfig: ExternalLocationConfig | undefined;

  constructor(
    protocol: Protocol,
    options?: {
      enableDescribe?: boolean;
      serverId?: string;
      dispatchHook?: DispatchHook;
      externalLocation?: ExternalLocationConfig;
      protocolVersion?: string;
    },
  ) {
    this.protocol = protocol;
    this.enableDescribe = options?.enableDescribe ?? true;
    this.serverId = options?.serverId ?? crypto.randomUUID().replace(/-/g, "").slice(0, 12);
    this.dispatchHook = options?.dispatchHook ?? null;
    this.externalConfig = options?.externalLocation;
    this.protocolVersion = options?.protocolVersion ?? "";

    // Build the describe batch once, regardless of enableDescribe — its
    // metadata carries the protocol_hash that every access-log record needs.
    const { batch, metadata } = buildDescribeBatch(protocol.name, protocol.getMethods(), this.serverId);
    this.protocolHash = metadata.get("vgi_rpc.protocol_hash") ?? "";
    if (this.enableDescribe) {
      this.describeBatch = batch;
    }
  }

  /** Start the server loop. Reads requests until stdin closes. */
  async run(): Promise<void> {
    const stdin = process.stdin as unknown as ReadableStream<Uint8Array>;

    // Warn if running interactively
    if (process.stdin.isTTY || process.stdout.isTTY) {
      process.stderr.write(
        "WARNING: This process communicates via Arrow IPC on stdin/stdout " +
          "and is not intended to be run interactively.\n" +
          "It should be launched as a subprocess by an RPC client " +
          "(e.g. vgi_rpc.connect()).\n",
      );
    }

    const reader = await IpcStreamReader.create(stdin);
    const writer = new IpcStreamWriter();

    try {
      while (true) {
        await this.serveOne(reader, writer);
      }
    } catch (e: any) {
      // EOF or broken pipe → clean exit
      if (
        e.message?.includes("closed") ||
        e.message?.includes("Expected Schema Message") ||
        e.message?.includes("null or length 0") ||
        e.code === "EPIPE" ||
        e.code === "ERR_STREAM_PREMATURE_CLOSE" ||
        e.code === "ERR_STREAM_DESTROYED" ||
        (e instanceof Error && e.message.includes("EOF"))
      ) {
        return;
      }
      // ArrowInvalid or unexpected error
      throw e;
    } finally {
      await reader.cancel();
    }
  }

  private async serveOne(reader: IpcStreamReader, writer: IpcStreamWriter): Promise<void> {
    const stream = await reader.readStream();
    if (!stream) {
      throw new Error("EOF");
    }

    const { schema, batches } = stream;
    if (batches.length === 0) {
      const err = new RpcError("ProtocolError", "Request stream contains no batches", "");
      const errBatch = buildErrorBatch(EMPTY_SCHEMA, err, this.serverId, null);
      writer.writeStream(EMPTY_SCHEMA, [errBatch]);
      return;
    }

    const batch = batches[0];
    let methodName: string;
    let params: Record<string, any>;
    let requestId: string | null;

    try {
      const parsed = parseRequest(schema, batch);
      methodName = parsed.methodName;
      params = parsed.params;
      requestId = parsed.requestId;
    } catch (e: any) {
      // Write error response for protocol/version errors
      const errBatch = buildErrorBatch(EMPTY_SCHEMA, e, this.serverId, null);
      writer.writeStream(EMPTY_SCHEMA, [errBatch]);
      if (e instanceof VersionError || e instanceof RpcError) {
        return; // Continue serving
      }
      throw e;
    }

    // Handle __describe__
    if (methodName === DESCRIBE_METHOD_NAME && this.describeBatch) {
      writer.writeStream(this.describeBatch.schema, [this.describeBatch]);
      return;
    }

    // Look up method
    const methods = this.protocol.getMethods();
    const method = methods.get(methodName);
    if (!method) {
      const available = [...methods.keys()].sort();
      const err = new Error(`Unknown method: '${methodName}'. Available methods: [${available.join(", ")}]`);
      const errBatch = buildErrorBatch(EMPTY_SCHEMA, err, this.serverId, requestId);
      writer.writeStream(EMPTY_SCHEMA, [errBatch]);
      return;
    }

    // Dispatch based on method type, with optional hook
    const methodType = method.type === MethodType.UNARY ? "unary" : "stream";

    // Capture self-contained IPC bytes of the request batch for the access log.
    let requestData: Uint8Array | undefined;
    try {
      const w = new RecordBatchStreamWriter();
      w.reset(undefined, batch.schema);
      // biome-ignore lint/suspicious/noExplicitAny: bypass schema-cmp like elsewhere in this file
      (w as any)._writeRecordBatch(batch);
      w.close();
      requestData = w.toUint8Array(true);
    } catch {
      // best-effort; observability must not fail dispatch
    }

    let streamId: string | undefined;
    if (methodType === "stream") {
      streamId = randomStreamId();
    }

    const info: DispatchInfo = {
      method: methodName,
      methodType,
      serverId: this.serverId,
      requestId,
      protocol: this.protocol.name,
      protocolHash: this.protocolHash,
      protocolVersion: this.protocolVersion,
      principal: "",
      authDomain: "",
      authenticated: false,
      remoteAddr: "",
      requestData,
      streamId,
    };
    const stats: CallStatistics = {
      inputBatches: 0,
      outputBatches: 0,
      inputRows: 0,
      outputRows: 0,
      inputBytes: 0,
      outputBytes: 0,
    };

    const token = this.dispatchHook?.onDispatchStart(info);
    let dispatchError: Error | undefined;

    applyDefaults(params, method.defaults);

    try {
      if (method.type === MethodType.UNARY) {
        await dispatchUnary(method, params, writer, this.serverId, requestId, this.externalConfig);
      } else {
        await dispatchStream(method, params, writer, reader, this.serverId, requestId, this.externalConfig);
      }
    } catch (e) {
      dispatchError = e instanceof Error ? e : new Error(String(e));
      throw e;
    } finally {
      this.dispatchHook?.onDispatchEnd(token, info, stats, dispatchError);
    }
  }
}
