#!/usr/bin/env bun
// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Conformance client-driver: the JSONL bridge that lets the shared Python
 * conformance suite drive *this* port's client.
 *
 * The suite normally points the Python reference client at a foreign server.
 * This is the other direction, and it is the direction that has never been
 * tested here: `httpConnect` / `pipeConnect` have only ever run against this
 * repository's own server, which accepts both bare and namespaced request
 * paths. A permissive server cannot validate a client — the same shape of
 * blindness let the Rust client ship bare URL paths for weeks, green against
 * its own server and 730 failures against the reference.
 *
 * The control protocol is specified in the reference repository at
 * `tools/cross-port/specs/CLIENT_DRIVER_PROTOCOL.md`; that document, not this
 * file, is the contract. The Python half
 * (`vgi_rpc.conformance.client_driver`) is shared across ports and already
 * written.
 *
 * What crosses this boundary is **Arrow IPC bytes plus a method name**, never
 * typed values: the driver reads exactly one batch and its custom metadata,
 * hands both to the client, and re-serialises whatever comes back. Every
 * decision that could paper over a client defect — routing, the stream kind,
 * external-pointer resolution, retries, error spelling — belongs to the
 * client and is deliberately absent here.
 *
 * stdout is the control channel and nothing else. Diagnostics go to stderr.
 */

import { Buffer } from "node:buffer";
import { writeSync } from "node:fs";
import type { Schema } from "@query-farm/apache-arrow";
import { type HttpRpcClient, httpConnect, type RpcClient } from "../src/client/connect.js";
import type { LogMessage, ServiceDescription } from "../src/client/index.js";
import { readResponseBatches } from "../src/client/ipc.js";
import { serializeRequest, withMetadata } from "../src/client/outbound.js";
import { subprocessConnect } from "../src/client/pipe.js";
import type { RawBatch, RawStreamSession } from "../src/client/raw.js";
import { tcpConnect } from "../src/client/tcp.js";
import { RPC_METHOD_KEY } from "../src/constants.js";
import { RpcError } from "../src/errors.js";
import type { ExternalLocationConfig } from "../src/external.js";
import { serializeIpcStream } from "../src/http/common.js";

/**
 * The introspection *format* version, which reflection does not report.
 *
 * `describe_version` is vestigial — introspection is a protocol whose major
 * version is part of its own name — and the spec pins it to the string "5",
 * so there is nothing for a client to learn and nothing for this to read.
 */
const DESCRIBE_VERSION = "5";

type Json = Record<string, unknown>;

interface LogRecord {
  level: string;
  message: string;
  extra: Record<string, string>;
}

let client: RpcClient | null = null;
let httpClient: HttpRpcClient | null = null;
let stream: RawStreamSession | null = null;
let logBuffer: LogRecord[] = [];

// --- control channel --------------------------------------------------------

/**
 * Write one control response, unbuffered.
 *
 * `writeSync` rather than `process.stdout.write` so a response is on the pipe
 * before the next statement runs: `shutdown` answers and then exits, and a
 * buffered last line would be lost to the exit.
 */
function write(response: Json): void {
  const bytes = Buffer.from(`${JSON.stringify(response)}\n`, "utf8");
  let offset = 0;
  while (offset < bytes.length) {
    offset += writeSync(1, bytes, offset, bytes.length - offset);
  }
}

function b64encode(bytes: Uint8Array): string {
  return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength).toString("base64");
}

function b64decode(text: string): Uint8Array {
  return new Uint8Array(Buffer.from(text, "base64"));
}

/** Drain the log buffer into the response being built. Exactly-once by design. */
function drainLogs(): LogRecord[] {
  const drained = logBuffer;
  logBuffer = [];
  return drained;
}

function onLog(message: LogMessage): void {
  const extra: Record<string, string> = {};
  for (const [key, value] of Object.entries(message.extra ?? {})) {
    // The harness drops non-strings rather than coercing them, so send only
    // what is already one; a structured value would vanish either way and
    // stringifying it here would invent a shape the server never sent.
    if (typeof value === "string") extra[key] = value;
  }
  logBuffer.push({
    // The harness looks the name up in an enum, so any other spelling raises.
    level: String(message.level ?? "INFO").toUpperCase(),
    message: message.message ?? "",
    extra,
  });
}

/**
 * Render a thrown value as the protocol's structured error object.
 *
 * `error_type` is asserted verbatim by the tests, so the peer's class name is
 * relayed untranslated; a failure the client library raised as something else
 * keeps that name too, because normalising it here would hide which layer
 * failed.
 */
function errorJson(error: unknown): Json {
  if (error instanceof RpcError) {
    // `errorMessage` / `remoteTraceback`, not `message` / `stack`: the former
    // are the peer's own, and `message` is this class's `"<type>: <message>"`
    // rendering, which the harness would render again as
    // `ProtocolError: ProtocolError: ...`.
    return {
      error_type: error.errorType,
      error_message: error.errorMessage,
      traceback: error.remoteTraceback ?? "",
    };
  }
  if (error instanceof Error) {
    return { error_type: error.name || "Error", error_message: error.message, traceback: "" };
  }
  return { error_type: "Error", error_message: String(error), traceback: "" };
}

/** Text for an `ok: false` refusal — the driver could not carry out the op. */
function refusalText(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}

// --- Arrow IPC framing ------------------------------------------------------

/** Read the single batch, and its custom metadata, out of one IPC stream. */
async function decodeOne(encoded: string): Promise<RawBatch> {
  const { batches } = await readResponseBatches(b64decode(encoded));
  const batch = batches.find((candidate) => candidate.constructor.name !== "_InternalEmptyPlaceholderRecordBatch");
  if (!batch) throw new Error("IPC stream carried no record batch");
  return { batch, metadata: new Map(batch.metadata ?? []) };
}

/** Write one batch and its custom metadata back as a complete IPC stream.
 *
 *  Through the client's own outbound path, so a batch of either Arrow
 *  implementation is written by its own -- the driver then runs unchanged
 *  under `--conditions=flechette`. */
function encodeOne(item: RawBatch): string {
  const batch = withMetadata(item.batch, new Map(item.metadata));
  return b64encode(serializeRequest(batch.schema, [batch]));
}

/** Encode a bare schema as an IPC stream; only its schema message is read. */
function schemaB64(schema: Schema | undefined): string | null {
  if (!schema) return null;
  return b64encode(serializeIpcStream(schema, []));
}

// --- connect ----------------------------------------------------------------

function externalConfigFor(enabled: boolean): ExternalLocationConfig | undefined {
  if (!enabled) return undefined;
  // Resolution-only: `storage` is the upload side and is never reached here.
  // `urlValidator: null` because the conformance fixtures vend http:// URLs,
  // and the default validator is HTTPS-only. Resolving the pointer is the
  // *client's* job — doing it in the driver is exactly the accommodation that
  // makes an external-location test pass without the client ever fetching.
  return { storage: { upload: async () => "" }, urlValidator: null };
}

/**
 * Wrap `fetch` so every request carries the caller's default headers.
 *
 * The sticky cross-principal tests hand `http_connect` a pre-built client
 * whose headers pin an identity; the control protocol forwards those on
 * `connect`, and this is where they go back on the wire.
 */
function headerInjectingFetch(headers: Record<string, string>): typeof globalThis.fetch {
  const base = globalThis.fetch;
  if (Object.keys(headers).length === 0) return base;
  return (async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const merged = new Headers(init?.headers);
    for (const [name, value] of Object.entries(headers)) merged.set(name, value);
    return base(input, { ...init, headers: merged });
  }) as typeof globalThis.fetch;
}

function doConnect(request: Json): void {
  const transport = String(request.transport ?? "");
  const target = request.target;
  // The routing key. Required, and never defaulted: a single-protocol server
  // refuses an unrouted call like any other, and a driver that substituted a
  // hardcoded name would hide exactly that.
  const protocol = typeof request.protocol === "string" ? request.protocol : "";
  if (!protocol) {
    write({ ok: false, error: "connect requires a 'protocol' routing key" });
    return;
  }
  const external = externalConfigFor(request.external === true);

  try {
    if (transport === "stdio") {
      if (!Array.isArray(target)) throw new Error("stdio target must be an argv array");
      client = subprocessConnect(target.map(String), {
        onLog,
        externalLocation: external,
        stderr: "inherit",
      });
    } else if (transport === "tcp") {
      if (typeof target !== "string") throw new Error("tcp target must be a 'host:port' string");
      const separator = target.lastIndexOf(":");
      const host = separator <= 0 ? "127.0.0.1" : target.slice(0, separator);
      const port = Number(separator < 0 ? target : target.slice(separator + 1));
      if (!Number.isInteger(port)) throw new Error(`tcp target has no port: ${target}`);
      client = tcpConnect(host || "127.0.0.1", port, { onLog, externalLocation: external });
    } else if (transport === "http") {
      if (typeof target !== "string") throw new Error("http target must be a url string");
      const headers: Record<string, string> = {};
      if (request.headers && typeof request.headers === "object") {
        for (const [name, value] of Object.entries(request.headers as Record<string, unknown>)) {
          if (typeof value === "string") headers[name] = value;
        }
      }
      // Tri-state: absent -> this client's default (no request compression);
      // null -> disabled; an integer -> that zstd level.
      const level = request.compression_level;
      httpClient = httpConnect(target, {
        onLog,
        externalLocation: external,
        compressionLevel: typeof level === "number" ? level : undefined,
        fetch: headerInjectingFetch(headers),
      });
      client = httpClient;
    } else if (transport === "unix" || transport === "shm") {
      throw new Error(`this port's client has no ${transport} transport`);
    } else {
      throw new Error(`unknown transport: ${transport}`);
    }
    write({ ok: true });
  } catch (error) {
    write({ ok: false, error: refusalText(error) });
  }
}

// --- describe ---------------------------------------------------------------

function describeJson(description: ServiceDescription): Json {
  return {
    protocol_name: description.protocolName,
    // Both come from reflection's `list_protocols` hop: server identity is a
    // property of the server, so the per-protocol description does not carry
    // it. Empty when the client bound to a named protocol and skipped it.
    request_version: description.requestVersion ?? "",
    server_id: description.serverId ?? "",
    describe_version: DESCRIBE_VERSION,
    protocol_hash: description.protocolHash,
    protocol_version: description.protocolVersion,
    methods: description.methods.map((method) => ({
      name: method.name,
      method_type: method.type,
      has_return: method.hasReturn === true,
      has_header: method.headerSchema != null,
      // `null`, not `false`, for a unary method and for a stream the server
      // declined to classify — the three states are distinct.
      is_exchange:
        method.type !== "stream"
          ? null
          : method.streamKind === "exchange"
            ? true
            : method.streamKind === "producer"
              ? false
              : null,
      params_schema_b64: schemaB64(method.paramsSchema),
      result_schema_b64: schemaB64(method.resultSchema),
      header_schema_b64: schemaB64(method.headerSchema),
    })),
  };
}

// --- ops --------------------------------------------------------------------

/** Pull the method name the request batch names, or fail loudly. */
function methodOf(item: RawBatch): string {
  const method = item.metadata.get(RPC_METHOD_KEY);
  if (!method) {
    // Never defaulted: an earlier driver in another port defaulted to
    // `__describe__`, and every lost-metadata bug was then reported as a
    // retired-method error instead of as the framing bug it was.
    throw new Error(`request metadata names no '${RPC_METHOD_KEY}'`);
  }
  return method;
}

async function handleUnary(request: Json): Promise<Json> {
  if (!client) return { ok: false, error: "not connected" };
  let input: RawBatch;
  let method: string;
  try {
    input = await decodeOne(String(request.request_b64 ?? ""));
    method = methodOf(input);
  } catch (error) {
    return { ok: false, error: refusalText(error) };
  }
  try {
    const result = await client.callRaw(method, input);
    return {
      ok: true,
      result_b64: result === null ? null : encodeOne(result),
      logs: drainLogs(),
      error: null,
    };
  } catch (error) {
    return { ok: true, result_b64: null, logs: drainLogs(), error: errorJson(error) };
  }
}

async function handleDescribe(): Promise<Json> {
  if (!client) return { ok: false, error: "not connected" };
  try {
    const description = await client.describe();
    return { ok: true, describe: describeJson(description), logs: drainLogs(), error: null };
  } catch (error) {
    return { ok: true, describe: null, logs: drainLogs(), error: errorJson(error) };
  }
}

async function handleStreamOpen(request: Json): Promise<Json> {
  if (!client) return { ok: false, error: "not connected" };
  if (stream) return { ok: false, error: "a stream is already open on this connection" };
  let input: RawBatch;
  let method: string;
  try {
    input = await decodeOne(String(request.request_b64 ?? ""));
    method = methodOf(input);
  } catch (error) {
    return { ok: false, error: refusalText(error) };
  }
  try {
    // `is_exchange` comes from the protocol declaration and is authoritative.
    // Inferring it from the method name would be a fixture-shaped accident.
    const opened = await client.streamRaw(method, input, {
      isExchange: request.is_exchange === true,
      hasHeader: request.has_header === true,
    });
    stream = opened;
    const header = opened.rawHeader;
    return { ok: true, header_b64: header === null ? null : encodeOne(header), logs: drainLogs() };
  } catch (error) {
    // The op *was* carried out; the server refused. That is a call error, and
    // no stream is left open.
    stream = null;
    return { ok: true, header_b64: null, logs: drainLogs(), error: errorJson(error) };
  }
}

/** Shape one stream turn's outcome, and end the stream on a terminal one. */
function streamItem(item: RawBatch | null, token?: string | null): Json {
  if (item === null) {
    stream = null;
    const response: Json = { ok: true, done: true, batch_b64: null, logs: drainLogs(), error: null };
    if (token !== undefined) response.token = null;
    return response;
  }
  const response: Json = { ok: true, done: false, batch_b64: encodeOne(item), logs: drainLogs(), error: null };
  if (token !== undefined) response.token = token;
  return response;
}

function streamError(error: unknown, withToken: boolean): Json {
  stream = null;
  const response: Json = { ok: true, done: true, batch_b64: null, logs: drainLogs(), error: errorJson(error) };
  if (withToken) response.token = null;
  return response;
}

async function handleTick(request: Json): Promise<Json> {
  if (!stream) return { ok: false, error: "no stream is open" };
  let metadata: Map<string, string> | undefined;
  if (typeof request.input_b64 === "string") {
    try {
      // The batch is a carrier for the per-tick metadata and nothing else.
      metadata = (await decodeOne(request.input_b64)).metadata;
    } catch (error) {
      return { ok: false, error: refusalText(error) };
    }
  }
  try {
    return streamItem(await stream.tickRaw(metadata));
  } catch (error) {
    return streamError(error, false);
  }
}

async function handleNextWithToken(): Promise<Json> {
  if (!stream) return { ok: false, error: "no stream is open" };
  try {
    const next = await stream.nextWithTokenRaw();
    return next === null ? streamItem(null, null) : streamItem(next.item, next.token);
  } catch (error) {
    return streamError(error, true);
  }
}

async function handleExchange(request: Json): Promise<Json> {
  if (!stream) return { ok: false, error: "no stream is open" };
  let input: RawBatch;
  try {
    input = await decodeOne(String(request.input_b64 ?? ""));
  } catch (error) {
    return { ok: false, error: refusalText(error) };
  }
  try {
    return streamItem(await stream.exchangeRaw(input));
  } catch (error) {
    return streamError(error, false);
  }
}

async function handleCancel(): Promise<Json> {
  const open = stream;
  stream = null;
  if (!open) return { ok: true, logs: drainLogs() };
  try {
    await open.cancel();
  } catch {
    // Cancellation is best-effort on every transport; the stream is over.
  }
  return { ok: true, logs: drainLogs() };
}

function handleClose(): Json {
  const open = stream;
  stream = null;
  if (open) open.close();
  return { ok: true };
}

// --- HTTP-only ops ----------------------------------------------------------

function expiresAtSeconds(value: Date): number {
  return Math.floor(value.getTime() / 1000);
}

async function handleHttpOp(op: string, request: Json): Promise<Json> {
  if (!client) return { ok: false, error: "not connected" };
  if (!httpClient) return { ok: false, error: "op requires http transport" };
  const http = httpClient;
  switch (op) {
    case "capabilities": {
      const caps = await http.capabilities();
      return {
        ok: true,
        caps: {
          sticky_enabled: caps.stickyEnabled,
          sticky_default_ttl: caps.stickyDefaultTtl,
          sticky_echo_headers: caps.stickyEchoHeaders,
          upload_url_support: caps.uploadUrlSupport,
          max_request_bytes: caps.maxRequestBytes,
          max_response_bytes: caps.maxResponseBytes,
          max_externalized_response_bytes: caps.maxExternalizedResponseBytes,
          externalization_enabled: caps.externalizationEnabled,
          max_upload_bytes: caps.maxUploadBytes,
          supported_encodings: caps.supportedEncodings,
        },
      };
    }
    case "request_upload_urls": {
      const count = typeof request.count === "number" ? request.count : 1;
      const urls = await http.requestUploadUrls(count);
      return {
        ok: true,
        urls: urls.map((url) => ({
          upload_url: url.uploadUrl,
          download_url: url.downloadUrl,
          expires_at: expiresAtSeconds(url.expiresAt),
        })),
      };
    }
    case "session_begin": {
      // An absent, null or empty token means "let the server mint one".
      const token = typeof request.token === "string" && request.token !== "" ? request.token : null;
      http.beginSession(token);
      return { ok: true };
    }
    case "session_token":
      return { ok: true, token: http.currentSessionToken() };
    case "session_echo_headers":
      return { ok: true, headers: http.currentEchoHeaders() };
    case "session_detach":
      return { ok: true, token: http.detachSession() };
    case "session_end":
      await http.endSession();
      return { ok: true };
    default:
      return { ok: false, error: `unknown op: ${op}` };
  }
}

const HTTP_OPS = new Set([
  "capabilities",
  "request_upload_urls",
  "session_begin",
  "session_token",
  "session_echo_headers",
  "session_detach",
  "session_end",
]);

// --- teardown ---------------------------------------------------------------

/**
 * Terminate any open stream and release the transport.
 *
 * Closing the connection is part of the contract, not an optimisation: the
 * harness runs thousands of connections, and a driver that leaked a
 * subprocess or a socket per connection would exhaust the runner rather than
 * fail a test.
 */
async function teardown(): Promise<void> {
  const open = stream;
  stream = null;
  if (open) {
    try {
      open.close();
    } catch {
      // Best-effort.
    }
  }
  if (httpClient) {
    try {
      await httpClient.endSession();
    } catch {
      // Best-effort.
    }
  }
  const connection = client;
  client = null;
  httpClient = null;
  if (connection) {
    try {
      connection.close();
    } catch {
      // Best-effort.
    }
  }
}

// --- main loop --------------------------------------------------------------

async function* controlLines(): AsyncGenerator<string> {
  const decoder = new TextDecoder();
  let buffer = "";
  for await (const chunk of Bun.stdin.stream()) {
    buffer += decoder.decode(chunk as Uint8Array, { stream: true });
    let index = buffer.indexOf("\n");
    while (index >= 0) {
      yield buffer.slice(0, index);
      buffer = buffer.slice(index + 1);
      index = buffer.indexOf("\n");
    }
  }
  if (buffer.trim().length > 0) yield buffer;
}

async function dispatch(request: Json): Promise<Json | null> {
  const op = String(request.op ?? "");
  if (op === "connect") {
    doConnect(request);
    return null; // doConnect writes its own response.
  }
  if (op === "unary") return handleUnary(request);
  if (op === "describe") return handleDescribe();
  if (op === "stream_open") return handleStreamOpen(request);
  if (op === "tick") return handleTick(request);
  if (op === "next_with_token") return handleNextWithToken();
  if (op === "exchange") return handleExchange(request);
  if (op === "cancel") return handleCancel();
  if (op === "close") return handleClose();
  if (HTTP_OPS.has(op)) {
    try {
      return await handleHttpOp(op, request);
    } catch (error) {
      return { ok: false, error: refusalText(error) };
    }
  }
  return { ok: false, error: `unknown op: ${op}` };
}

async function main(): Promise<void> {
  for await (const line of controlLines()) {
    const trimmed = line.trim();
    if (trimmed.length === 0) continue;
    let request: Json;
    try {
      request = JSON.parse(trimmed) as Json;
    } catch (error) {
      write({ ok: false, error: `bad json: ${refusalText(error)}` });
      continue;
    }
    if (request.op === "shutdown") {
      await teardown();
      write({ ok: true });
      process.exit(0);
    }
    let response: Json | null;
    try {
      response = await dispatch(request);
    } catch (error) {
      response = { ok: false, error: refusalText(error) };
    }
    if (response !== null) write(response);
  }
  // EOF on stdin is a shutdown with no response.
  await teardown();
  process.exit(0);
}

await main();
