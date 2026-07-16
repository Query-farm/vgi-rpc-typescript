// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { schema as makeSchema, serializeBatch } from "./arrow/index.js";
import { DESCRIBE_METHOD_NAME, PROTOCOL_VERSION_KEY } from "./constants.js";
import { buildDescribeBatch } from "./dispatch/describe.js";
import { dispatchStream } from "./dispatch/stream.js";
import { dispatchUnary } from "./dispatch/unary.js";
import {
  MethodNotImplementedError,
  ProtocolVersionError,
  parseProtocolVersion,
  RpcError,
  VersionError,
} from "./errors.js";
import type { ExternalLocationConfig } from "./external.js";
import type { Protocol } from "./protocol.js";
import {
  type CallStatistics,
  type DispatchHook,
  type DispatchInfo,
  MethodType,
  type ServeStartHook,
  TransportKind,
} from "./types.js";
import { IpcStreamReader } from "./wire/reader.js";
import { applyDefaults, parseRequest } from "./wire/request.js";
import { buildErrorBatch } from "./wire/response.js";
import { type ByteSink, IpcStreamWriter } from "./wire/writer.js";

const EMPTY_SCHEMA = makeSchema([]);

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
  // Lazily initialized — `buildDescribeBatch` is async because the protocol
  // hash is computed via `crypto.subtle.digest` (Web Crypto). The dispatch
  // path awaits the cached promise on first use.
  private _describePromise: Promise<{
    batch: import("./arrow/index.js").VgiBatch;
    protocolHash: string;
  }> | null = null;
  private protocolVersion: string;
  private dispatchHook: DispatchHook | null = null;
  private externalConfig: ExternalLocationConfig | undefined;
  private onServeStart: ServeStartHook | null = null;
  /** True once the on_serve_start hook has fired successfully. The bind
   *  state is committed only after the hook returns, so a transient
   *  failure on first request leaves it `false` and the next request
   *  re-fires rather than silently skipping. Mirrors Python 7b3999c. */
  private serveStartFired = false;

  constructor(
    protocol: Protocol,
    options?: {
      /** Enable the `describe` RPC method (service self-description). Default `true`. */
      enableDescribe?: boolean;
      /** Opaque per-process server identifier surfaced to clients and the landing page. */
      serverId?: string;
      /** Hook invoked around each dispatched request (tracing/metrics/auth enrichment). */
      dispatchHook?: DispatchHook;
      /** Configuration for externalizing oversized record batches to blob storage. */
      externalLocation?: ExternalLocationConfig;
      /** Protocol version string reported in the service description. */
      protocolVersion?: string;
      /** Lifecycle hook fired once before the first dispatched request. */
      onServeStart?: ServeStartHook;
    },
  ) {
    this.protocol = protocol;
    this.enableDescribe = options?.enableDescribe ?? true;
    this.serverId = options?.serverId ?? crypto.randomUUID().replace(/-/g, "").slice(0, 12);
    this.dispatchHook = options?.dispatchHook ?? null;
    this.externalConfig = options?.externalLocation;
    this.protocolVersion = options?.protocolVersion ?? "";
    this.onServeStart = options?.onServeStart ?? null;
  }

  /** Fire the on_serve_start hook once for this transport. Idempotent
   *  on success — re-throws on failure without committing the bind. */
  private async notifyTransport(kind: TransportKind): Promise<void> {
    if (this.serveStartFired) return;
    if (this.onServeStart) {
      await this.onServeStart(kind);
    }
    this.serveStartFired = true;
  }

  /** Build (or retrieve cached) describe batch + protocol hash. */
  private async describeInfo(): Promise<{
    batch: import("./arrow/index.js").VgiBatch;
    protocolHash: string;
  }> {
    if (!this._describePromise) {
      this._describePromise = buildDescribeBatch(
        this.protocol.name,
        this.protocol.getMethods(),
        this.serverId,
        this.protocol.protocolVersion || undefined,
      ).then(({ batch, metadata }) => ({
        batch,
        protocolHash: metadata.get("vgi_rpc.protocol_hash") ?? "",
      }));
    }
    return this._describePromise;
  }

  /** Validate a client's declared protocol_version against the Protocol's
   *  declared version. Caller invokes only when
   *  `protocol.protocolVersionParts` is non-null. Mirrors Python's
   *  `RpcServer._check_protocol_version`: exact major+minor match, patch
   *  ignored; directional error message names which side is older. */
  private checkProtocolVersion(clientVersion: string | undefined): void {
    const serverParts = this.protocol.protocolVersionParts!;
    const serverVersion = this.protocol.protocolVersion;
    if (clientVersion === undefined) {
      throw new ProtocolVersionError(
        "VGI client/worker protocol_version mismatch.\n" +
          "  Client: <not declared>\n" +
          `  Server: ${serverVersion}\n` +
          "  Direction: the client did not send a vgi_rpc.protocol_version " +
          "metadata key. This is either a vgi-rpc framework bug or a " +
          "non-VGI client connecting to a VGI worker.",
      );
    }
    let clientParts: readonly [number, number, number];
    try {
      clientParts = parseProtocolVersion(clientVersion);
    } catch {
      throw new ProtocolVersionError(
        "VGI client/worker protocol_version mismatch.\n" +
          `  Client: ${clientVersion}\n` +
          `  Server: ${serverVersion}\n` +
          "  Direction: client sent a malformed protocol_version. " +
          "Expected canonical semver MAJOR.MINOR.PATCH.",
      );
    }
    if (clientParts[0] === serverParts[0] && clientParts[1] === serverParts[1]) {
      return;
    }
    const clientOlder =
      clientParts[0] < serverParts[0] || (clientParts[0] === serverParts[0] && clientParts[1] < serverParts[1]);
    const direction = clientOlder
      ? `client is too old; upgrade the VGI extension/client to a version supporting protocol_version ${serverVersion}.`
      : `server is too old; upgrade the VGI worker to a version supporting protocol_version ${clientVersion}.`;
    throw new ProtocolVersionError(
      "VGI client/worker protocol_version mismatch.\n" +
        `  Client: ${clientVersion}\n` +
        `  Server: ${serverVersion}\n` +
        `  Direction: ${direction}`,
    );
  }

  /** Start the server loop over stdin/stdout. Reads requests until stdin closes. */
  async run(): Promise<void> {
    // Warn if running interactively
    if (process.stdin.isTTY || process.stdout.isTTY) {
      process.stderr.write(
        "WARNING: This process communicates via Arrow IPC on stdin/stdout " +
          "and is not intended to be run interactively.\n" +
          "It should be launched as a subprocess by an RPC client " +
          "(e.g. vgi_rpc.connect()).\n",
      );
    }
    const stdin = process.stdin as unknown as ReadableStream<Uint8Array>;
    // writable omitted → IpcStreamWriter defaults to the stdout fd.
    await this.serveConnection(stdin);
  }

  /**
   * Serve requests over an explicit byte-stream pair until the readable ends —
   * the transport-agnostic core that {@link run} (stdin/stdout) is built on.
   *
   * Use this to serve over any duplex channel that the stdio/unix/tcp helpers
   * don't cover: a Web Worker / `MessagePort` bridge, an in-memory pipe, or a
   * pre-connected socket. The loop, on_serve_start firing, and EOF/broken-pipe
   * handling are identical to {@link run}.
   *
   * @param readable incoming request bytes — a web `ReadableStream<Uint8Array>`
   *   or a Node `Readable` (e.g. a `Duplex` bridging a MessagePort).
   * @param writable outgoing response sink — a stdout-like fd number, or a
   *   `net.Socket` / structurally-compatible `Duplex`. Omit for the stdout fd.
   * @param transportKind reported to the `on_serve_start` hook (default `PIPE`).
   */
  async serveConnection(
    readable: ReadableStream<Uint8Array> | NodeJS.ReadableStream,
    writable?: number | import("node:net").Socket | ByteSink,
    transportKind: TransportKind = TransportKind.PIPE,
  ): Promise<void> {
    const reader = await IpcStreamReader.create(readable);
    const writer = new IpcStreamWriter(writable);

    try {
      while (true) {
        // Fire on_serve_start lazily so the hook can do work that depends on
        // the transport binding. Inside the loop so a failure on the very
        // first request can be retried.
        await this.notifyTransport(transportKind);
        await this.serveOne(reader, writer);
      }
    } catch (e: any) {
      // EOF or broken pipe / closed channel → clean exit
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
      await writer.writeStream(EMPTY_SCHEMA, [errBatch]);
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
      await writer.writeStream(EMPTY_SCHEMA, [errBatch]);
      if (e instanceof VersionError || e instanceof RpcError) {
        return; // Continue serving
      }
      throw e;
    }

    // Handle __describe__ — lazy-build on first request.
    if (methodName === DESCRIBE_METHOD_NAME && this.enableDescribe) {
      const { batch } = await this.describeInfo();
      await writer.writeStream(batch.schema, [batch]);
      return;
    }

    // Look up method
    const methods = this.protocol.getMethods();
    const method = methods.get(methodName);
    if (!method) {
      const available = [...methods.keys()].sort();
      const err = new MethodNotImplementedError(
        `Unknown method: '${methodName}'. Available methods: [${available.join(", ")}]`,
      );
      const errBatch = buildErrorBatch(EMPTY_SCHEMA, err, this.serverId, requestId);
      await writer.writeStream(EMPTY_SCHEMA, [errBatch]);
      return;
    }

    // Application-protocol-version gate. Fires only when the Protocol
    // declared a `protocolVersion`. `__describe__` is exempt — it is the
    // diagnostic path a mismatched client uses to introspect the server's
    // version. Mirrors Python's serve_one dispatch-boundary check.
    if (this.protocol.protocolVersionParts !== null) {
      try {
        const md = batch.metadata;
        this.checkProtocolVersion(md?.get(PROTOCOL_VERSION_KEY));
      } catch (exc) {
        const errSchema = method.type === MethodType.UNARY ? method.resultSchema : EMPTY_SCHEMA;
        const errBatch = buildErrorBatch(errSchema, exc as Error, this.serverId, requestId);
        await writer.writeStream(errSchema, [errBatch]);
        return;
      }
    }

    // Dispatch based on method type, with optional hook
    const methodType = method.type === MethodType.UNARY ? "unary" : "stream";

    // Capture self-contained IPC bytes of the request batch for the access log.
    let requestData: Uint8Array | undefined;
    try {
      requestData = serializeBatch(batch as any);
    } catch {
      // best-effort; observability must not fail dispatch
    }

    let streamId: string | undefined;
    if (methodType === "stream") {
      streamId = randomStreamId();
    }

    const { protocolHash } = await this.describeInfo();
    const info: DispatchInfo = {
      method: methodName,
      methodType,
      serverId: this.serverId,
      requestId,
      protocol: this.protocol.name,
      protocolHash,
      protocolVersion: this.protocolVersion,
      kind: TransportKind.PIPE,
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
        await dispatchUnary(method, params, writer, this.serverId, requestId, this.externalConfig, TransportKind.PIPE);
      } else {
        await dispatchStream(
          method,
          params,
          writer,
          reader,
          this.serverId,
          requestId,
          this.externalConfig,
          TransportKind.PIPE,
        );
      }
    } catch (e) {
      dispatchError = e instanceof Error ? e : new Error(String(e));
      throw e;
    } finally {
      this.dispatchHook?.onDispatchEnd(token, info, stats, dispatchError);
    }
  }
}
