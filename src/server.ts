// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { Schema } from "apache-arrow";
import { Protocol } from "./protocol.js";
import { IpcStreamReader } from "./wire/reader.js";
import { IpcStreamWriter } from "./wire/writer.js";
import { parseRequest } from "./wire/request.js";
import { buildErrorBatch } from "./wire/response.js";
import { buildDescribeBatch } from "./dispatch/describe.js";
import { dispatchUnary } from "./dispatch/unary.js";
import { dispatchStream } from "./dispatch/stream.js";
import { DESCRIBE_METHOD_NAME } from "./constants.js";
import { MethodType } from "./types.js";
import { RpcError, VersionError } from "./errors.js";

const EMPTY_SCHEMA = new Schema([]);

/**
 * RPC server that reads Arrow IPC requests from stdin and writes responses to stdout.
 * Supports unary and streaming (producer/exchange) methods.
 */
export class VgiRpcServer {
  private protocol: Protocol;
  private enableDescribe: boolean;
  private serverId: string;
  private describeBatch: import("apache-arrow").RecordBatch | null = null;

  constructor(
    protocol: Protocol,
    options?: { enableDescribe?: boolean; serverId?: string },
  ) {
    this.protocol = protocol;
    this.enableDescribe = options?.enableDescribe ?? true;
    this.serverId =
      options?.serverId ?? crypto.randomUUID().replace(/-/g, "").slice(0, 12);

    if (this.enableDescribe) {
      const { batch } = buildDescribeBatch(
        protocol.name,
        protocol.getMethods(),
        this.serverId,
      );
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

  private async serveOne(
    reader: IpcStreamReader,
    writer: IpcStreamWriter,
  ): Promise<void> {
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
      const err = new Error(
        `Unknown method: '${methodName}'. Available methods: [${available.join(", ")}]`,
      );
      const errBatch = buildErrorBatch(EMPTY_SCHEMA, err, this.serverId, requestId);
      writer.writeStream(EMPTY_SCHEMA, [errBatch]);
      return;
    }

    // Dispatch based on method type
    if (method.type === MethodType.UNARY) {
      await dispatchUnary(method, params, writer, this.serverId, requestId);
    } else {
      await dispatchStream(
        method,
        params,
        writer,
        reader,
        this.serverId,
        requestId,
      );
    }
  }
}
