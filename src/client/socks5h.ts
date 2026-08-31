// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/** Strict, credential-free SOCKS5h support for the Node client transports. */

import { isIP, type Socket, connect as tcpDial } from "node:net";
import { connect as tlsDial } from "node:tls";
import { domainToASCII } from "node:url";
import type { HttpRpcClient, RpcClient } from "./connect.js";
import { httpConnect } from "./connect.js";
import { pipeConnect } from "./pipe.js";
import type { Socks5hHttpConnectOptions, Socks5hTcpConnectOptions } from "./types.js";

const DEFAULT_CONNECT_TIMEOUT_MS = 5_000;
const DEFAULT_REQUEST_TIMEOUT_MS = 300_000;
const DEFAULT_MAX_RESPONSE_BYTES = 256 * 1024 * 1024;
const DEFAULT_MAX_RESPONSE_HEADER_BYTES = 64 * 1024;

/** Parsed address of a credential-free SOCKS5h proxy. */
export interface Socks5hProxy {
  /** Proxy host name or normalized IP literal. */
  readonly host: string;
  /** Proxy TCP port in the range 1 through 65535. */
  readonly port: number;
}

/** Parse a credential-free `socks5h://host:port` URI. */
export function parseSocks5hProxy(value: string): Socks5hProxy {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new TypeError("invalid SOCKS5h proxy URI");
  }
  if (
    url.protocol !== "socks5h:" ||
    url.username !== "" ||
    url.password !== "" ||
    (url.pathname !== "" && url.pathname !== "/") ||
    url.search !== "" ||
    url.hash !== "" ||
    url.hostname === "" ||
    url.port === ""
  ) {
    throw new TypeError("SOCKS5h proxy must be socks5h://host:port without credentials or options");
  }
  const port = Number(url.port);
  if (!Number.isInteger(port) || port < 1 || port > 65_535) throw new TypeError("invalid SOCKS5h proxy port");
  return Object.freeze({ host: stripIpv6Brackets(url.hostname), port });
}

function stripIpv6Brackets(host: string): string {
  return host.startsWith("[") && host.endsWith("]") ? host.slice(1, -1) : host;
}

function checkedPort(port: number): number {
  if (!Number.isInteger(port) || port < 1 || port > 65_535) throw new TypeError("target port must be 1..65535");
  return port;
}

function targetAddress(host: string): Uint8Array {
  if (
    host === "" ||
    Array.from(host).some((character) => {
      const code = character.codePointAt(0) as number;
      return code <= 0x1f || code === 0x7f;
    })
  )
    throw new TypeError("invalid SOCKS5h target host");
  const kind = isIP(host);
  if (kind === 4) return Uint8Array.of(1, ...host.split(".").map(Number));
  if (kind === 6) {
    const bytes = ipv6Bytes(host);
    return Uint8Array.of(4, ...bytes);
  }
  const ascii = domainToASCII(host);
  if (!ascii || ascii.length > 255 || /[^\x21-\x7e]/u.test(ascii)) {
    throw new TypeError("SOCKS5h target domain must contain 1..255 IDNA bytes");
  }
  const encoded = new TextEncoder().encode(ascii);
  return Uint8Array.of(3, encoded.length, ...encoded);
}

function ipv6Bytes(input: string): number[] {
  const host = input.split("%")[0].toLowerCase();
  const pieces = host.split("::");
  if (pieces.length > 2) throw new TypeError("invalid SOCKS5h IPv6 target");
  const parseSide = (side: string): number[] => {
    if (!side) return [];
    const words: number[] = [];
    for (const part of side.split(":")) {
      if (part.includes(".")) {
        const octets = part.split(".").map(Number);
        if (octets.length !== 4 || octets.some((value) => !Number.isInteger(value) || value < 0 || value > 255)) {
          throw new TypeError("invalid SOCKS5h IPv6 target");
        }
        words.push((octets[0] << 8) | octets[1], (octets[2] << 8) | octets[3]);
      } else {
        const value = Number.parseInt(part, 16);
        if (!/^[0-9a-f]{1,4}$/u.test(part) || !Number.isInteger(value)) {
          throw new TypeError("invalid SOCKS5h IPv6 target");
        }
        words.push(value);
      }
    }
    return words;
  };
  const left = parseSide(pieces[0]);
  const right = parseSide(pieces[1] ?? "");
  const missing = 8 - left.length - right.length;
  if ((pieces.length === 1 && missing !== 0) || (pieces.length === 2 && missing < 1)) {
    throw new TypeError("invalid SOCKS5h IPv6 target");
  }
  const words = [...left, ...Array(missing).fill(0), ...right];
  return words.flatMap((word) => [word >>> 8, word & 0xff]);
}

function setupSignal(
  timeoutMs: number,
  signal?: AbortSignal,
): {
  signal: AbortSignal;
  finish: () => void;
} {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) throw new TypeError("SOCKS5h connect timeout must be positive");
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(new Error("SOCKS5h connection deadline elapsed")), timeoutMs);
  const abort = () => controller.abort(signal?.reason ?? new Error("SOCKS5h connection cancelled"));
  if (signal?.aborted) abort();
  else signal?.addEventListener("abort", abort, { once: true });
  return {
    signal: controller.signal,
    finish: () => {
      clearTimeout(timeout);
      signal?.removeEventListener("abort", abort);
    },
  };
}

function waitConnect(socket: Socket, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const cleanup = () => {
      socket.off("connect", connected);
      socket.off("error", failed);
      signal.removeEventListener("abort", aborted);
    };
    const connected = () => {
      cleanup();
      resolve();
    };
    const failed = (error: Error) => {
      cleanup();
      reject(error);
    };
    const aborted = () => {
      cleanup();
      socket.destroy();
      reject(signal.reason instanceof Error ? signal.reason : new Error("SOCKS5h connection cancelled"));
    };
    socket.once("connect", connected);
    socket.once("error", failed);
    if (signal.aborted) aborted();
    else signal.addEventListener("abort", aborted, { once: true });
  });
}

function write(socket: Socket, bytes: Uint8Array, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) return reject(signal.reason);
    const aborted = () => {
      socket.destroy();
      reject(signal.reason);
    };
    signal.addEventListener("abort", aborted, { once: true });
    socket.write(bytes, (error?: Error | null) => {
      signal.removeEventListener("abort", aborted);
      if (error) reject(error);
      else resolve();
    });
  });
}

class SocketReader {
  private buffered: Buffer<ArrayBufferLike> = Buffer.alloc(0);
  private ended = false;
  private failure: unknown;
  private wake?: () => void;

  constructor(
    private readonly socket: Socket,
    private readonly signal: AbortSignal,
  ) {
    socket.on("data", this.onData);
    socket.once("end", this.onEnd);
    socket.once("error", this.onError);
    signal.addEventListener("abort", this.onAbort, { once: true });
  }

  private onData = (chunk: Buffer): void => {
    this.buffered = this.buffered.length === 0 ? chunk : Buffer.concat([this.buffered, chunk]);
    this.wake?.();
  };
  private onEnd = (): void => {
    this.ended = true;
    this.wake?.();
  };
  private onError = (error: Error): void => {
    this.failure = error;
    this.wake?.();
  };
  private onAbort = (): void => {
    this.failure = this.signal.reason;
    this.socket.destroy();
    this.wake?.();
  };

  async exact(size: number): Promise<Uint8Array> {
    while (this.buffered.length < size) {
      if (this.failure) throw this.failure;
      if (this.ended) throw new Error("SOCKS5h reply was truncated");
      await new Promise<void>((resolve) => {
        this.wake = resolve;
      });
      this.wake = undefined;
    }
    const value = this.buffered.subarray(0, size);
    this.buffered = this.buffered.subarray(size);
    return value;
  }

  release(): void {
    this.socket.off("data", this.onData);
    this.socket.off("end", this.onEnd);
    this.socket.off("error", this.onError);
    this.signal.removeEventListener("abort", this.onAbort);
    if (this.buffered.length > 0) this.socket.unshift(this.buffered);
  }
}

async function negotiate(socket: Socket, host: string, port: number, signal: AbortSignal): Promise<void> {
  const reader = new SocketReader(socket, signal);
  try {
    await write(socket, Uint8Array.of(5, 1, 0), signal);
    const method = await reader.exact(2);
    if (method[0] !== 5 || method[1] !== 0) throw new Error("SOCKS5h proxy rejected the NO AUTH method");
    const address = targetAddress(host);
    await write(socket, Uint8Array.of(5, 1, 0, ...address, port >>> 8, port & 0xff), signal);
    const head = await reader.exact(4);
    if (head[0] !== 5 || head[2] !== 0) throw new Error("malformed SOCKS5h connect response");
    if (head[1] !== 0) throw new Error(`SOCKS5h proxy rejected target connection (reply ${head[1]})`);
    let addressLength: number;
    if (head[3] === 1) addressLength = 4;
    else if (head[3] === 4) addressLength = 16;
    else if (head[3] === 3) addressLength = (await reader.exact(1))[0];
    else throw new Error("SOCKS5h response used an invalid address type");
    await reader.exact(addressLength + 2);
  } finally {
    reader.release();
  }
}

/**
 * Open one TCP tunnel through a SOCKS5h proxy. Target DNS names are encoded as
 * SOCKS domain names and therefore resolved only by the proxy.
 */
export async function dialSocks5h(
  proxyUri: string | Socks5hProxy,
  targetHost: string,
  targetPort: number,
  options: { connectTimeoutMs?: number; signal?: AbortSignal } = {},
): Promise<Socket> {
  const proxy = typeof proxyUri === "string" ? parseSocks5hProxy(proxyUri) : proxyUri;
  if (
    !proxy.host ||
    Array.from(proxy.host).some((character) => {
      const code = character.codePointAt(0) as number;
      return code <= 0x1f || code === 0x7f;
    })
  )
    throw new TypeError("invalid SOCKS5h proxy host");
  checkedPort(proxy.port);
  const port = checkedPort(targetPort);
  // Validate before touching the network.
  targetAddress(targetHost);
  const setup = setupSignal(options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS, options.signal);
  const socket = tcpDial({ host: proxy.host, port: proxy.port });
  try {
    await waitConnect(socket, setup.signal);
    socket.setNoDelay(true);
    await negotiate(socket, targetHost, port, setup.signal);
    return socket;
  } catch (error) {
    socket.destroy();
    throw error instanceof Error ? error : new Error("SOCKS5h connection failed");
  } finally {
    setup.finish();
  }
}

/** Connect the raw Arrow/TCP client through SOCKS5h. */
export async function tcpConnectSocks5h(
  host: string,
  port: number,
  proxy: string,
  options: Socks5hTcpConnectOptions = {},
): Promise<RpcClient> {
  const socket = await dialSocks5h(proxy, host, port, options);
  socket.on("error", () => {});
  const client = pipeConnect(
    socket as unknown as ReadableStream<Uint8Array>,
    {
      write(data: Uint8Array): void {
        socket.write(data);
      },
      end(): void {
        socket.end();
      },
    },
    options,
  );
  const close = client.close;
  client.close = () => {
    close.call(client);
    socket.destroy();
  };
  return client;
}

function nodeHeaders(input: HeadersInit | undefined): Record<string, string> {
  const headers: Record<string, string> = {};
  new Headers(input).forEach((value, name) => {
    headers[name] = value;
  });
  return headers;
}

function requestBody(input: URL | RequestInfo, init?: RequestInit): Uint8Array {
  const body = init?.body ?? (input instanceof Request ? input.body : null);
  if (body == null) return new Uint8Array();
  if (typeof body === "string") return new TextEncoder().encode(body);
  if (body instanceof Uint8Array) return body;
  if (body instanceof ArrayBuffer) return new Uint8Array(body);
  throw new TypeError("SOCKS5h fetch supports string, ArrayBuffer, and Uint8Array request bodies");
}

const HEADER_NAME = /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/u;

function invalidHeaderValue(value: string): boolean {
  for (let index = 0; index < value.length; index++) {
    const code = value.charCodeAt(index);
    if ((code < 32 && code !== 9) || code === 127) return true;
  }
  return false;
}

function responseHeaders(
  raw: Buffer,
  headerEnd: number,
): {
  statusCode: number;
  contentLength?: number;
  chunked: boolean;
} {
  const lines = raw.toString("latin1", 0, headerEnd).split("\r\n");
  const status = /^HTTP\/1\.[01] (\d{3})(?: [^\r\n]*)?$/u.exec(lines.shift() ?? "");
  if (!status) throw new Error("invalid HTTP status line");
  const statusCode = Number(status[1]);
  if (statusCode < 200 || statusCode > 599) throw new Error("unsupported informational HTTP response");
  const contentLengths: string[] = [];
  const transferEncodings: string[] = [];
  for (const line of lines) {
    const colon = line.indexOf(":");
    if (colon <= 0 || /^[ \t]/u.test(line)) throw new Error("invalid HTTP response header");
    const name = line.slice(0, colon);
    const value = line.slice(colon + 1).trim();
    if (!HEADER_NAME.test(name) || invalidHeaderValue(value)) {
      throw new Error("invalid HTTP response header");
    }
    if (name.toLowerCase() === "content-length") contentLengths.push(value);
    if (name.toLowerCase() === "transfer-encoding") transferEncodings.push(value);
  }
  if (contentLengths.length > 1 || transferEncodings.length > 1) {
    throw new Error("ambiguous HTTP response framing");
  }
  if (contentLengths.length > 0 && transferEncodings.length > 0) {
    throw new Error("conflicting HTTP response framing");
  }
  if (transferEncodings.length === 1 && transferEncodings[0].toLowerCase() !== "chunked") {
    throw new Error("unsupported HTTP Transfer-Encoding");
  }
  let contentLength: number | undefined;
  if (contentLengths.length === 1) {
    if (!/^\d+$/u.test(contentLengths[0])) throw new Error("invalid HTTP Content-Length");
    contentLength = Number(contentLengths[0]);
    if (!Number.isSafeInteger(contentLength)) throw new Error("invalid HTTP Content-Length");
  }
  return { statusCode, contentLength, chunked: transferEncodings.length === 1 };
}

/** A bounded, geometrically growing buffer: total copying is O(response bytes). */
class HttpResponseFramer {
  private bytes: Buffer;
  private length = 0;
  private headerSearchFrom = 0;
  private headerEnd?: number;
  private expectedLength?: number;
  private chunkOffset?: number;
  private chunkLineSearchFrom?: number;
  private chunkDataEnd?: number;
  private trailerOffset?: number;
  private trailerSearchFrom?: number;
  private closeDelimited = false;

  constructor(
    private readonly method: string,
    private readonly maxResponseBytes: number,
    private readonly maxHeaderBytes: number,
  ) {
    this.bytes = Buffer.allocUnsafe(Math.min(8192, maxResponseBytes));
  }

  append(chunk: Buffer): number | null {
    const nextLength = this.length + chunk.length;
    if (nextLength > this.maxResponseBytes) throw new Error("HTTP response exceeds configured limit");
    this.ensureCapacity(nextLength);
    chunk.copy(this.bytes, this.length);
    this.length = nextLength;
    return this.inspect();
  }

  finish(): number {
    const complete = this.inspect();
    if (complete !== null) return complete;
    if (this.closeDelimited && this.headerEnd !== undefined) return this.length;
    throw new Error("truncated HTTP response");
  }

  materialize(length: number): Buffer {
    return Buffer.from(this.bytes.subarray(0, length));
  }

  private ensureCapacity(required: number): void {
    if (required <= this.bytes.length) return;
    let capacity = this.bytes.length;
    while (capacity < required) capacity = Math.min(this.maxResponseBytes, Math.max(capacity * 2, required));
    const replacement = Buffer.allocUnsafe(capacity);
    this.bytes.copy(replacement, 0, 0, this.length);
    this.bytes = replacement;
  }

  private inspect(): number | null {
    if (this.headerEnd === undefined) {
      const marker = this.bytes.subarray(0, this.length).indexOf("\r\n\r\n", this.headerSearchFrom);
      if (marker < 0) {
        if (this.length >= this.maxHeaderBytes) throw new Error("oversized HTTP response headers");
        this.headerSearchFrom = Math.max(0, this.length - 3);
        return null;
      }
      if (marker + 4 > this.maxHeaderBytes) throw new Error("oversized HTTP response headers");
      this.headerEnd = marker + 4;
      const framing = responseHeaders(this.bytes, marker);
      // A bodyless response still has to pass the framing checks above.
      if (
        this.method === "HEAD" ||
        framing.statusCode === 204 ||
        framing.statusCode === 205 ||
        framing.statusCode === 304
      ) {
        return this.headerEnd;
      }
      if (framing.contentLength !== undefined) {
        this.expectedLength = this.headerEnd + framing.contentLength;
        if (!Number.isSafeInteger(this.expectedLength) || this.expectedLength > this.maxResponseBytes) {
          throw new Error("HTTP response exceeds configured limit");
        }
      } else if (framing.chunked) {
        this.chunkOffset = this.headerEnd;
        this.chunkLineSearchFrom = this.headerEnd;
      } else {
        this.closeDelimited = true;
      }
    }
    if (this.expectedLength !== undefined) return this.length >= this.expectedLength ? this.expectedLength : null;
    if (this.chunkOffset === undefined) return null;
    return this.inspectChunks();
  }

  private inspectChunks(): number | null {
    while (this.chunkOffset !== undefined) {
      if (this.trailerOffset !== undefined) {
        if (
          this.length >= this.trailerOffset + 2 &&
          this.bytes[this.trailerOffset] === 13 &&
          this.bytes[this.trailerOffset + 1] === 10
        ) {
          return this.trailerOffset + 2;
        }
        const trailerEnd = this.bytes
          .subarray(0, this.length)
          .indexOf("\r\n\r\n", this.trailerSearchFrom ?? this.trailerOffset);
        if (trailerEnd < 0) {
          this.trailerSearchFrom = Math.max(this.trailerOffset, this.length - 3);
          return null;
        }
        const trailers = this.bytes.toString("latin1", this.trailerOffset, trailerEnd).split("\r\n");
        for (const trailer of trailers) {
          const colon = trailer.indexOf(":");
          const name = trailer.slice(0, colon);
          const value = trailer.slice(colon + 1).trim();
          if (
            colon <= 0 ||
            /^[ \t]/u.test(trailer) ||
            !HEADER_NAME.test(name) ||
            invalidHeaderValue(value) ||
            name.toLowerCase() === "content-length" ||
            name.toLowerCase() === "transfer-encoding"
          ) {
            throw new Error("invalid chunked HTTP trailer");
          }
        }
        return trailerEnd + 4;
      }
      if (this.chunkDataEnd !== undefined) {
        if (this.length < this.chunkDataEnd) return null;
        if (this.bytes[this.chunkDataEnd - 2] !== 13 || this.bytes[this.chunkDataEnd - 1] !== 10) {
          throw new Error("invalid chunked HTTP response");
        }
        this.chunkOffset = this.chunkDataEnd;
        this.chunkLineSearchFrom = this.chunkDataEnd;
        this.chunkDataEnd = undefined;
        continue;
      }
      const lineEnd = this.bytes.subarray(0, this.length).indexOf("\r\n", this.chunkLineSearchFrom ?? this.chunkOffset);
      if (lineEnd < 0) {
        this.chunkLineSearchFrom = Math.max(this.chunkOffset, this.length - 1);
        return null;
      }
      const sizeText = this.bytes.toString("ascii", this.chunkOffset, lineEnd).split(";", 1)[0].trim();
      if (!/^[0-9A-Fa-f]+$/u.test(sizeText)) throw new Error("invalid chunked HTTP response");
      const size = Number.parseInt(sizeText, 16);
      if (!Number.isSafeInteger(size)) throw new Error("invalid chunked HTTP response");
      const dataOffset = lineEnd + 2;
      if (size === 0) {
        this.trailerOffset = dataOffset;
        this.trailerSearchFrom = dataOffset;
        continue;
      }
      const nextOffset = dataOffset + size + 2;
      if (!Number.isSafeInteger(nextOffset) || nextOffset > this.maxResponseBytes) {
        throw new Error("HTTP response exceeds configured limit");
      }
      this.chunkDataEnd = nextOffset;
    }
    return null;
  }
}

function readSocketResponse(
  socket: Socket,
  signal: AbortSignal,
  method: string,
  maxResponseBytes: number,
  maxHeaderBytes: number,
): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const framer = new HttpResponseFramer(method, maxResponseBytes, maxHeaderBytes);
    let settled = false;
    const cleanup = () => {
      socket.off("data", data);
      socket.off("end", end);
      socket.off("error", fail);
      signal.removeEventListener("abort", abort);
    };
    const succeed = (length: number) => {
      if (settled) return;
      settled = true;
      cleanup();
      socket.pause();
      resolve(framer.materialize(length));
    };
    const rejectOnce = (error: unknown) => {
      if (settled) return;
      settled = true;
      cleanup();
      socket.destroy();
      reject(error);
    };
    const data = (chunk: Buffer) => {
      try {
        const complete = framer.append(chunk);
        if (complete !== null) succeed(complete);
      } catch (error) {
        rejectOnce(error);
      }
    };
    const end = () => {
      try {
        succeed(framer.finish());
      } catch (error) {
        rejectOnce(error);
      }
    };
    const fail = (error: Error) => {
      rejectOnce(error);
    };
    const abort = () => {
      rejectOnce(signal.reason ?? new Error("SOCKS5h HTTP request cancelled"));
    };
    socket.on("data", data);
    socket.once("end", end);
    socket.once("error", fail);
    if (signal.aborted) abort();
    else signal.addEventListener("abort", abort, { once: true });
  });
}

function decodeChunked(body: Buffer): Buffer {
  const chunks: Buffer[] = [];
  let offset = 0;
  while (true) {
    const lineEnd = body.indexOf("\r\n", offset);
    if (lineEnd < 0) throw new Error("truncated chunked HTTP response");
    const sizeText = body.toString("ascii", offset, lineEnd).split(";", 1)[0].trim();
    if (!/^[0-9A-Fa-f]+$/u.test(sizeText)) throw new Error("invalid chunked HTTP response");
    const size = Number.parseInt(sizeText, 16);
    offset = lineEnd + 2;
    if (size === 0) return Buffer.concat(chunks);
    if (offset + size + 2 > body.length || body[offset + size] !== 13 || body[offset + size + 1] !== 10) {
      throw new Error("truncated chunked HTTP response");
    }
    chunks.push(body.subarray(offset, offset + size));
    offset += size + 2;
  }
}

function decodeHttpResponse(raw: Buffer, maxHeaderBytes: number): Response {
  const headerEnd = raw.indexOf("\r\n\r\n");
  if (headerEnd < 0 || headerEnd + 4 > maxHeaderBytes) throw new Error("invalid or oversized HTTP response headers");
  const lines = raw.toString("latin1", 0, headerEnd).split("\r\n");
  const status = /^HTTP\/1\.[01] (\d{3})(?: (.*))?$/u.exec(lines.shift() ?? "");
  if (!status) throw new Error("invalid HTTP status line");
  const headers = new Headers();
  const contentLengths: string[] = [];
  const transferEncodings: string[] = [];
  for (const line of lines) {
    const colon = line.indexOf(":");
    if (colon <= 0 || /^[ \t]/u.test(line)) throw new Error("invalid HTTP response header");
    const name = line.slice(0, colon);
    const value = line.slice(colon + 1).trim();
    if (name.toLowerCase() === "content-length") contentLengths.push(value);
    if (name.toLowerCase() === "transfer-encoding") transferEncodings.push(value);
    headers.append(name, value);
  }
  if (contentLengths.length > 1 || transferEncodings.length > 1) throw new Error("ambiguous HTTP response framing");
  if (contentLengths.length > 0 && transferEncodings.length > 0) throw new Error("conflicting HTTP response framing");
  let body = raw.subarray(headerEnd + 4);
  if (transferEncodings.length === 1) {
    if (transferEncodings[0].toLowerCase() !== "chunked") throw new Error("unsupported HTTP Transfer-Encoding");
    body = decodeChunked(body);
    headers.delete("Transfer-Encoding");
  } else if (contentLengths.length === 1) {
    if (!/^\d+$/u.test(contentLengths[0])) throw new Error("invalid HTTP Content-Length");
    const length = Number(contentLengths[0]);
    if (!Number.isSafeInteger(length) || body.length < length) throw new Error("invalid HTTP Content-Length");
    body = body.subarray(0, length);
  }
  const statusCode = Number(status[1]);
  const noBody = statusCode === 204 || statusCode === 205 || statusCode === 304;
  return new Response(noBody ? null : Uint8Array.from(body), {
    status: statusCode,
    statusText: status[2] ?? "",
    headers,
  });
}

/** Create a Node fetch implementation whose every connection uses SOCKS5h. */
export function createSocks5hFetch(
  proxy: string,
  connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS,
  requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS,
  maxResponseBytes = DEFAULT_MAX_RESPONSE_BYTES,
  maxResponseHeaderBytes = DEFAULT_MAX_RESPONSE_HEADER_BYTES,
): typeof fetch {
  // Parse once so malformed configuration fails before an RPC can begin.
  const parsedProxy = parseSocks5hProxy(proxy);
  if (!Number.isFinite(connectTimeoutMs) || connectTimeoutMs <= 0) {
    throw new TypeError("SOCKS5h connectTimeoutMs must be positive");
  }
  if (!Number.isFinite(requestTimeoutMs) || requestTimeoutMs <= 0) {
    throw new TypeError("SOCKS5h requestTimeoutMs must be positive");
  }
  if (!Number.isSafeInteger(maxResponseBytes) || maxResponseBytes < 1) {
    throw new TypeError("SOCKS5h maxResponseBytes must be a positive integer");
  }
  if (
    !Number.isSafeInteger(maxResponseHeaderBytes) ||
    maxResponseHeaderBytes < 1 ||
    maxResponseHeaderBytes > maxResponseBytes
  ) {
    throw new TypeError("SOCKS5h maxResponseHeaderBytes must be a positive integer within maxResponseBytes");
  }
  return (async (input: URL | RequestInfo, init?: RequestInit): Promise<Response> => {
    const requestUrl = new URL(input instanceof Request ? input.url : input.toString());
    if (requestUrl.protocol !== "http:" && requestUrl.protocol !== "https:") {
      throw new TypeError("SOCKS5h fetch supports only HTTP and HTTPS URLs");
    }
    const secure = requestUrl.protocol === "https:";
    const targetPort = requestUrl.port ? Number(requestUrl.port) : secure ? 443 : 80;
    const targetHost = stripIpv6Brackets(requestUrl.hostname);
    const signal = init?.signal ?? (input instanceof Request ? input.signal : undefined);
    const body = requestBody(input, init);
    const method = (init?.method ?? (input instanceof Request ? input.method : "GET")).toUpperCase();
    if (!HEADER_NAME.test(method)) throw new TypeError("invalid HTTP method");
    const headers = nodeHeaders(init?.headers ?? (input instanceof Request ? input.headers : undefined));
    headers.host = requestUrl.host;
    headers.connection = "close";
    if (headers["transfer-encoding"] !== undefined)
      throw new TypeError("SOCKS5h fetch does not accept Transfer-Encoding");
    if (headers["content-length"] !== undefined) {
      if (!/^\d+$/u.test(headers["content-length"]) || Number(headers["content-length"]) !== body.byteLength) {
        throw new TypeError("request Content-Length does not match the buffered body");
      }
    } else if (body.byteLength > 0) {
      headers["content-length"] = String(body.byteLength);
    }
    const requestBudget = setupSignal(requestTimeoutMs, signal ?? undefined);
    const setup = setupSignal(connectTimeoutMs, requestBudget.signal);
    let connection: Socket;
    try {
      connection = await dialSocks5h(parsedProxy, targetHost, targetPort, {
        connectTimeoutMs,
        signal: setup.signal,
      });
      if (secure) {
        connection = await new Promise<Socket>((resolve, reject) => {
          const tls = tlsDial({ socket: connection, servername: isIP(targetHost) ? undefined : targetHost });
          const aborted = () => tls.destroy(setup.signal.reason);
          const failed = (error: Error) => {
            setup.signal.removeEventListener("abort", aborted);
            tls.destroy();
            reject(error);
          };
          tls.once("error", failed);
          tls.once("secureConnect", () => {
            tls.off("error", failed);
            setup.signal.removeEventListener("abort", aborted);
            tls.setNoDelay(true);
            resolve(tls);
          });
          if (setup.signal.aborted) aborted();
          else setup.signal.addEventListener("abort", aborted, { once: true });
        });
      }
    } catch (error) {
      setup.finish();
      requestBudget.finish();
      throw error;
    }
    setup.finish();
    try {
      const start = `${method} ${requestUrl.pathname}${requestUrl.search} HTTP/1.1\r\n`;
      const head = `${start}${Object.entries(headers)
        .map(([name, value]) => `${name}: ${value}\r\n`)
        .join("")}\r\n`;
      await write(connection, new TextEncoder().encode(head), requestBudget.signal);
      if (body.byteLength > 0) await write(connection, body, requestBudget.signal);
      const rawResponse = await readSocketResponse(
        connection,
        requestBudget.signal,
        method,
        maxResponseBytes,
        maxResponseHeaderBytes,
      );
      connection.destroy();
      const response = decodeHttpResponse(rawResponse, maxResponseHeaderBytes);
      requestBudget.finish();
      return response;
    } catch (error) {
      connection.destroy();
      requestBudget.finish();
      throw error;
    }
  }) as typeof fetch;
}

/** Connect the HTTP RPC client through SOCKS5h with no direct fallback. */
export function httpConnectSocks5h(
  baseUrl: string,
  proxy: string,
  options: Socks5hHttpConnectOptions = {},
): HttpRpcClient {
  const { connectTimeoutMs, requestTimeoutMs, maxResponseBytes, maxResponseHeaderBytes, signal, ...httpOptions } =
    options;
  const socksFetch = createSocks5hFetch(
    proxy,
    connectTimeoutMs,
    requestTimeoutMs,
    maxResponseBytes,
    maxResponseHeaderBytes,
  );
  return httpConnect(baseUrl, {
    ...httpOptions,
    fetch: ((input, init) => socksFetch(input, { ...init, signal: init?.signal ?? signal })) as typeof fetch,
  });
}
