// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/** Node-only Tailscale Serve and LocalAPI peer-identity adapters. */

import { type Socket, connect as tcpConnect } from "node:net";
import {
  IdentityAssurance,
  type JsonValue,
  PeerIdentity,
  type PeerIdentityProvider,
  PeerIdentityResult,
  PeerIdentityStatus,
  type PeerResolutionContext,
  PeerSubjectKind,
  SubjectStability,
} from "./identity.js";
import { normalizeIpLiteral, normalizeTrustedProxyAddresses } from "./ip.js";

const PROVIDER = "tailscale";
const LOCALAPI_HOST = "local-tailscaled.sock";
const DEFAULT_SOCKET = "/var/run/tailscale/tailscaled.sock";
const SERVICE_NAME = /^svc:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$/u;
const ENCODED_WORDS = /^=\?[Uu][Tt][Ff]-8\?[Qq]\?[^?]*\?=(?: +=\?[Uu][Tt][Ff]-8\?[Qq]\?[^?]*\?=)*$/u;

function result(status: PeerIdentityStatus, identity?: PeerIdentity): PeerIdentityResult {
  return new PeerIdentityResult(PROVIDER, status, identity ? [identity] : []);
}

function hasControl(value: string): boolean {
  return Array.from(value).some((character) => {
    const code = character.codePointAt(0) as number;
    return code <= 0x1f || code === 0x7f;
  });
}

function validText(value: string): boolean {
  try {
    for (let index = 0; index < value.length; index++) {
      const code = value.charCodeAt(index);
      if (code >= 0xd800 && code <= 0xdbff) {
        const low = value.charCodeAt(++index);
        if (low < 0xdc00 || low > 0xdfff) return false;
      } else if (code >= 0xdc00 && code <= 0xdfff) return false;
    }
    return !hasControl(value);
  } catch {
    return false;
  }
}

function decodeServeHeader(raw: string, maxBytes: number): string {
  if (new TextEncoder().encode(raw).length > maxBytes || !validText(raw)) throw new Error("invalid Serve header");
  if (Array.from(raw).some((character) => (character.codePointAt(0) as number) > 0x7f)) {
    throw new Error("Serve header must be ASCII or encoded-word text");
  }
  if (!raw.startsWith("=?")) return raw;
  if (!ENCODED_WORDS.test(raw)) throw new Error("invalid Serve encoded-word syntax");
  const output: number[] = [];
  const words = raw.split(/ +/u);
  for (const word of words) {
    const encoded = word.slice(word.indexOf("?q?") >= 0 ? word.indexOf("?q?") + 3 : word.indexOf("?Q?") + 3, -2);
    for (let index = 0; index < encoded.length; index++) {
      const character = encoded[index];
      if (character === "_") output.push(0x20);
      else if (character === "=") {
        const hex = encoded.slice(index + 1, index + 3);
        if (!/^[0-9A-Fa-f]{2}$/u.test(hex)) throw new Error("invalid Serve Q escape");
        output.push(Number.parseInt(hex, 16));
        index += 2;
      } else output.push(character.charCodeAt(0));
    }
  }
  const decoded = new TextDecoder("utf-8", { fatal: true }).decode(Uint8Array.from(output));
  if (!validText(decoded) || new TextEncoder().encode(decoded).length > maxBytes) throw new Error("invalid Serve text");
  return decoded;
}

class StrictJsonParser {
  private index = 0;
  private values = 0;
  constructor(private readonly text: string) {}

  parse(): JsonValue {
    const value = this.value(0);
    this.space();
    if (this.index !== this.text.length) throw new Error("trailing JSON data");
    return value;
  }

  private value(depth: number): JsonValue {
    if (depth > 16) throw new Error("JSON exceeds maximum depth");
    if (++this.values > 4096) throw new Error("JSON exceeds maximum value count");
    this.space();
    const char = this.text[this.index];
    if (char === '"') return this.string();
    if (char === "{") return this.object(depth);
    if (char === "[") return this.array(depth);
    for (const [token, value] of [
      ["true", true],
      ["false", false],
      ["null", null],
    ] as const) {
      if (this.text.startsWith(token, this.index)) {
        this.index += token.length;
        return value;
      }
    }
    const match = /^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/u.exec(this.text.slice(this.index));
    if (!match) throw new Error("invalid JSON value");
    this.index += match[0].length;
    const number = Number(match[0]);
    if (!Number.isFinite(number)) throw new Error("non-finite JSON number");
    return number;
  }

  private string(): string {
    const start = this.index++;
    let escaped = false;
    while (this.index < this.text.length) {
      const char = this.text[this.index++];
      if (!escaped && char === '"') {
        const value = JSON.parse(this.text.slice(start, this.index)) as string;
        if (!validText(value)) throw new Error("invalid JSON string");
        return value;
      }
      if (!escaped && char.charCodeAt(0) < 0x20) throw new Error("control in JSON string");
      if (!escaped && char === "\\") escaped = true;
      else escaped = false;
    }
    throw new Error("unterminated JSON string");
  }

  private object(depth: number): JsonValue {
    this.index++;
    const object: Record<string, JsonValue> = {};
    const keys = new Set<string>();
    this.space();
    if (this.text[this.index] === "}") {
      this.index++;
      return object;
    }
    while (true) {
      this.space();
      if (this.text[this.index] !== '"') throw new Error("JSON object key must be a string");
      const key = this.string();
      if (keys.has(key)) throw new Error("duplicate JSON object key");
      keys.add(key);
      this.space();
      if (this.text[this.index++] !== ":") throw new Error("missing JSON colon");
      Object.defineProperty(object, key, {
        value: this.value(depth + 1),
        enumerable: true,
        configurable: true,
        writable: true,
      });
      this.space();
      const delimiter = this.text[this.index++];
      if (delimiter === "}") return object;
      if (delimiter !== ",") throw new Error("invalid JSON object delimiter");
    }
  }

  private array(depth: number): JsonValue {
    this.index++;
    const array: JsonValue[] = [];
    this.space();
    if (this.text[this.index] === "]") {
      this.index++;
      return array;
    }
    while (true) {
      array.push(this.value(depth + 1));
      this.space();
      const delimiter = this.text[this.index++];
      if (delimiter === "]") return array;
      if (delimiter !== ",") throw new Error("invalid JSON array delimiter");
    }
  }

  private space(): void {
    while (" \t\r\n".includes(this.text[this.index] ?? "\0")) this.index++;
  }
}

function parseStrictJson(bytes: Uint8Array, limit: number): JsonValue {
  if (bytes.byteLength > limit) throw new Error("JSON exceeds byte limit");
  const text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  return new StrictJsonParser(text).parse();
}

function capabilities(
  value: JsonValue,
  requireSlash: boolean,
  requireObjectEntries: boolean,
): Readonly<Record<string, JsonValue>> {
  if (value === null || Array.isArray(value) || typeof value !== "object")
    throw new Error("capabilities must be an object");
  for (const [name, entries] of Object.entries(value)) {
    if (!validText(name) || name.length > 512 || (requireSlash && (name.startsWith("/") || !name.includes("/")))) {
      throw new Error("invalid capability name");
    }
    if (
      !Array.isArray(entries) ||
      (requireObjectEntries &&
        entries.some((entry) => entry === null || Array.isArray(entry) || typeof entry !== "object"))
    ) {
      throw new Error("capability entries must be objects");
    }
  }
  return value as Readonly<Record<string, JsonValue>>;
}

/** Trust boundary and resource limits for Tailscale Serve identity headers. */
export interface TailscaleServeOptions {
  /** Operator-defined namespace that distinguishes identities from different tailnets. */
  readonly issuer: string;
  /** Exact normalized IP literals allowed to supply Tailscale Serve headers. */
  readonly trustedProxyAddresses: Iterable<string>;
  /** Maximum encoded bytes accepted in any identity or capability header. */
  readonly maxHeaderBytes?: number;
}

/** Consume identity and application-capability headers from an exact trusted Serve peer. */
export function tailscaleServeIdentityProvider(options: TailscaleServeOptions): PeerIdentityProvider {
  if (!options.issuer || !validText(options.issuer))
    throw new TypeError("Tailscale issuer must be non-empty text without controls");
  const trusted = normalizeTrustedProxyAddresses(
    options.trustedProxyAddresses,
    "Tailscale Serve trustedProxyAddresses",
  );
  const maxHeaderBytes = options.maxHeaderBytes ?? 16_384;
  if (!Number.isSafeInteger(maxHeaderBytes) || maxHeaderBytes <= 0)
    throw new TypeError("maxHeaderBytes must be positive");

  return {
    provider: PROVIDER,
    resolve(context) {
      const immediate = context ? normalizeIpLiteral(context.immediatePeer ?? "") : null;
      if (!immediate || !trusted.has(immediate)) return result(PeerIdentityStatus.UNTRUSTED_PROXY);
      try {
        const funnel = context.header("Tailscale-Funnel-Request");
        const loginRaw = context.header("Tailscale-User-Login");
        const nameRaw = context.header("Tailscale-User-Name");
        const profileRaw = context.header("Tailscale-User-Profile-Pic");
        const capRaw = context.header("Tailscale-App-Capabilities");
        if (funnel !== undefined)
          return result(funnel === "?1" ? PeerIdentityStatus.NOT_APPLICABLE : PeerIdentityStatus.INVALID);
        const login = loginRaw === undefined ? "" : decodeServeHeader(loginRaw, maxHeaderBytes);
        const displayName = nameRaw === undefined ? "" : decodeServeHeader(nameRaw, maxHeaderBytes);
        if (profileRaw !== undefined) decodeServeHeader(profileRaw, maxHeaderBytes);
        const caps =
          capRaw === undefined
            ? {}
            : capabilities(
                parseStrictJson(new TextEncoder().encode(decodeServeHeader(capRaw, maxHeaderBytes)), maxHeaderBytes),
                true,
                true,
              );
        if (
          (loginRaw !== undefined && login === "") ||
          ((nameRaw !== undefined || profileRaw !== undefined) && login === "")
        ) {
          return result(PeerIdentityStatus.INVALID);
        }
        if (!login && Object.keys(caps).length === 0) return result(PeerIdentityStatus.NO_MATCH);
        const attributes: Record<string, JsonValue> = {};
        if (login) attributes.user_login = login;
        if (displayName) attributes.user_display_name = displayName;
        const identity = new PeerIdentity({
          provider: PROVIDER,
          evidenceSource: "serve_proxy",
          assurance: IdentityAssurance.CONFIGURED_PROXY,
          issuer: options.issuer,
          transport: "http",
          subjectKind: login ? PeerSubjectKind.USER : PeerSubjectKind.UNKNOWN,
          subjectKey: login ? `login:${login}` : undefined,
          subjectStability: login ? SubjectStability.LOGIN : SubjectStability.NONE,
          subjectVerified: !!login,
          attributes,
          capabilities: caps,
          capabilitiesVerified: capRaw !== undefined,
          sourceAddress: context.assertedPeer,
          proxyAddress: context.immediatePeer,
        });
        return result(PeerIdentityStatus.AVAILABLE, identity);
      } catch {
        return result(PeerIdentityStatus.INVALID);
      }
    },
  };
}

/** Connection, namespace, and response limits for tailscaled LocalAPI WhoIs. */
export interface TailscaleLocalApiOptions {
  /** Operator-defined namespace that distinguishes identities from different tailnets. */
  readonly issuer: string;
  /** Unix-domain socket. Linux defaults to `/var/run/tailscale/tailscaled.sock`. */
  readonly unixSocket?: string;
  /** Explicit plain-HTTP local origin (for example a same-user-proof endpoint). */
  readonly endpoint?: string;
  /** Basic-auth password, valid only with `endpoint`. */
  readonly password?: string;
  /** Total LocalAPI lookup timeout in milliseconds. */
  readonly timeoutMs?: number;
  /** Maximum decoded LocalAPI response body size. */
  readonly maxResponseBytes?: number;
  /** Maximum LocalAPI response-header size. */
  readonly maxResponseHeaderBytes?: number;
}

function destinationIp(value: string): string | null {
  const direct = normalizeIpLiteral(value);
  if (direct) return direct;
  try {
    const url = new URL(`tcp://${value}`);
    return normalizeIpLiteral(url.hostname.startsWith("[") ? url.hostname.slice(1, -1) : url.hostname);
  } catch {
    return null;
  }
}

function remainingTimeout(context: PeerResolutionContext, configured: number): number {
  const candidates = [configured];
  if (context.deadline !== undefined) candidates.push(context.deadline - Date.now());
  const budget = context.remainingBudgetMs();
  if (budget !== undefined) candidates.push(budget);
  return Math.max(0, Math.min(...candidates));
}

async function localApiGet(
  transport: { socket?: string; endpoint?: URL; password?: string },
  path: string,
  timeoutMs: number,
  maxBody: number,
  maxHeaders: number,
  signal?: AbortSignal,
): Promise<{ status: number; rawHeaders: string[]; body: Uint8Array }> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error("LocalAPI deadline elapsed")), timeoutMs);
  const cancel = () => controller.abort(signal?.reason ?? new Error("LocalAPI request cancelled"));
  if (signal?.aborted) cancel();
  else signal?.addEventListener("abort", cancel, { once: true });
  const headers: Record<string, string> = { Accept: "application/json", Host: LOCALAPI_HOST };
  if (transport.password) headers.Authorization = `Basic ${Buffer.from(`:${transport.password}`).toString("base64")}`;
  const socket = transport.socket
    ? tcpConnect({ path: transport.socket })
    : tcpConnect({ host: transport.endpoint!.hostname, port: Number(transport.endpoint!.port || 80) });
  try {
    await localApiWaitConnect(socket, controller.signal);
    const request = `GET ${path} HTTP/1.1\r\n${Object.entries(headers)
      .map(([name, value]) => `${name}: ${value}\r\n`)
      .join("")}Connection: close\r\n\r\n`;
    await localApiWrite(socket, new TextEncoder().encode(request), controller.signal);
    const raw = await localApiRead(socket, maxHeaders + maxBody + 4, controller.signal);
    try {
      return decodeLocalApiResponse(raw, maxHeaders, maxBody);
    } catch (error) {
      throw new LocalApiInvalidResponseError(error instanceof Error ? error.message : "invalid LocalAPI response");
    }
  } finally {
    socket.destroy();
    clearTimeout(timer);
    signal?.removeEventListener("abort", cancel);
  }
}

class LocalApiInvalidResponseError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "LocalApiInvalidResponseError";
  }
}

function localApiWaitConnect(socket: Socket, signal: AbortSignal): Promise<void> {
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
      reject(signal.reason);
    };
    socket.once("connect", connected);
    socket.once("error", failed);
    signal.addEventListener("abort", aborted, { once: true });
  });
}

function localApiWrite(socket: Socket, bytes: Uint8Array, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
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

function localApiRead(socket: Socket, maxBytes: number, signal: AbortSignal): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let length = 0;
    const cleanup = () => {
      socket.off("data", data);
      socket.off("end", ended);
      socket.off("error", failed);
      signal.removeEventListener("abort", aborted);
    };
    const data = (chunk: Buffer) => {
      length += chunk.length;
      if (length > maxBytes) {
        cleanup();
        socket.destroy();
        reject(new LocalApiInvalidResponseError("LocalAPI response exceeds configured limits"));
      } else chunks.push(chunk);
    };
    const ended = () => {
      cleanup();
      resolve(Buffer.concat(chunks, length));
    };
    const failed = (error: Error) => {
      cleanup();
      reject(error);
    };
    const aborted = () => {
      cleanup();
      socket.destroy();
      reject(signal.reason);
    };
    socket.on("data", data);
    socket.once("end", ended);
    socket.once("error", failed);
    signal.addEventListener("abort", aborted, { once: true });
  });
}

function decodeLocalApiChunked(body: Buffer, maxBody: number): Buffer {
  const chunks: Buffer[] = [];
  let total = 0;
  let offset = 0;
  while (true) {
    const lineEnd = body.indexOf("\r\n", offset);
    if (lineEnd < 0) throw new Error("truncated LocalAPI chunk");
    const sizeText = body.toString("ascii", offset, lineEnd).split(";", 1)[0].trim();
    if (!/^[0-9A-Fa-f]+$/u.test(sizeText)) throw new Error("invalid LocalAPI chunk size");
    const size = Number.parseInt(sizeText, 16);
    offset = lineEnd + 2;
    if (size === 0) return Buffer.concat(chunks, total);
    total += size;
    if (total > maxBody || offset + size + 2 > body.length) throw new Error("LocalAPI response exceeds body limit");
    chunks.push(body.subarray(offset, offset + size));
    offset += size + 2;
  }
}

function decodeLocalApiResponse(
  raw: Buffer,
  maxHeaders: number,
  maxBody: number,
): { status: number; rawHeaders: string[]; body: Uint8Array } {
  const headerEnd = raw.indexOf("\r\n\r\n");
  if (headerEnd < 0 || headerEnd > maxHeaders) throw new Error("invalid or oversized LocalAPI response headers");
  const lines = raw.toString("latin1", 0, headerEnd).split("\r\n");
  const statusLine = /^HTTP\/1\.[01] (\d{3})(?: .*)?$/u.exec(lines.shift() ?? "");
  if (!statusLine) throw new Error("invalid LocalAPI status line");
  const rawHeaders: string[] = [];
  let chunked = false;
  let contentLength: number | undefined;
  for (const line of lines) {
    const colon = line.indexOf(":");
    if (colon <= 0) throw new Error("invalid LocalAPI response header");
    const name = line.slice(0, colon);
    const value = line.slice(colon + 1).trim();
    rawHeaders.push(name, value);
    if (name.toLowerCase() === "transfer-encoding" && /\bchunked\b/iu.test(value)) chunked = true;
    if (name.toLowerCase() === "content-length") contentLength = Number(value);
  }
  let body = raw.subarray(headerEnd + 4);
  if (chunked) body = decodeLocalApiChunked(body, maxBody);
  else if (contentLength !== undefined) {
    if (!Number.isSafeInteger(contentLength) || contentLength < 0 || body.length < contentLength) {
      throw new Error("invalid LocalAPI Content-Length");
    }
    body = body.subarray(0, contentLength);
  }
  if (body.length > maxBody) throw new Error("LocalAPI response exceeds body limit");
  return { status: Number(statusLine[1]), rawHeaders, body };
}

function optionalString(object: Record<string, JsonValue>, key: string): string {
  const value = object[key];
  if (value === undefined || value === null) return "";
  if (typeof value !== "string" || !validText(value)) throw new Error(`${key} must be valid text`);
  return value;
}

/** Resolve a fresh LocalAPI `/localapi/v0/whois` snapshot for every call. */
export function tailscaleLocalApiIdentityProvider(options: TailscaleLocalApiOptions): PeerIdentityProvider {
  if (!options.issuer || !validText(options.issuer))
    throw new TypeError("Tailscale issuer must be non-empty text without controls");
  if (options.unixSocket && options.endpoint) throw new TypeError("configure only one Tailscale LocalAPI transport");
  if (options.password && !options.endpoint)
    throw new TypeError("LocalAPI password requires an explicit HTTP endpoint");
  if (options.password && !validText(options.password)) throw new TypeError("invalid LocalAPI password");
  const timeoutMs = options.timeoutMs ?? 5_000;
  const maxBody = options.maxResponseBytes ?? 65_536;
  const maxHeaders = options.maxResponseHeaderBytes ?? 32_768;
  for (const [name, value] of Object.entries({
    timeoutMs,
    maxResponseBytes: maxBody,
    maxResponseHeaderBytes: maxHeaders,
  })) {
    if (!Number.isSafeInteger(value) || value <= 0) throw new TypeError(`${name} must be positive`);
  }
  let transport: { socket?: string; endpoint?: URL; password?: string };
  if (options.endpoint) {
    const endpoint = new URL(options.endpoint);
    if (
      endpoint.protocol !== "http:" ||
      !endpoint.host ||
      endpoint.username ||
      endpoint.password ||
      (endpoint.pathname !== "" && endpoint.pathname !== "/") ||
      endpoint.search ||
      endpoint.hash
    ) {
      throw new TypeError("LocalAPI endpoint must be a plain HTTP origin without userinfo or path");
    }
    transport = { endpoint, password: options.password };
  } else {
    const socket = options.unixSocket ?? (process.platform === "linux" ? DEFAULT_SOCKET : undefined);
    if (!socket) {
      throw new TypeError(
        `native Tailscale LocalAPI discovery is not implemented for Node on ${process.platform}; configure endpoint explicitly`,
      );
    }
    if (socket.includes("\0")) throw new TypeError("invalid LocalAPI Unix socket path");
    transport = { socket };
  }

  return {
    provider: PROVIDER,
    async resolve(context, signal) {
      if (!context) return result(PeerIdentityStatus.INVALID);
      const source = context.assertedPeer ?? context.sourceEndpoint ?? context.immediatePeer;
      if (!source) return result(PeerIdentityStatus.NOT_APPLICABLE);
      if (!validText(source) || new TextEncoder().encode(source).length > 4096)
        return result(PeerIdentityStatus.INVALID);
      const query = new URLSearchParams({ addr: source, proto: "tcp" });
      let target: Record<string, JsonValue> = { kind: "node" };
      if (context.serviceName) {
        if (!SERVICE_NAME.test(context.serviceName)) return result(PeerIdentityStatus.INVALID);
        query.set("svc_name", context.serviceName);
        target = { kind: "service", value: context.serviceName };
      } else if (context.destinationAddress) {
        const address = destinationIp(context.destinationAddress);
        if (!address) return result(PeerIdentityStatus.INVALID);
        query.set("dst_ip", address);
        target = { kind: "destination_ip", value: address };
      }
      const budget = remainingTimeout(context, timeoutMs);
      if (budget <= 0) return result(PeerIdentityStatus.UNAVAILABLE);
      let response: Awaited<ReturnType<typeof localApiGet>>;
      try {
        response = await localApiGet(
          transport,
          `/localapi/v0/whois?${query.toString()}`,
          budget,
          maxBody,
          maxHeaders,
          signal,
        );
      } catch (error) {
        return result(
          error instanceof LocalApiInvalidResponseError ? PeerIdentityStatus.INVALID : PeerIdentityStatus.UNAVAILABLE,
        );
      }
      if (response.status === 401 || response.status === 403) return result(PeerIdentityStatus.PERMISSION_DENIED);
      if (response.status === 404) return result(PeerIdentityStatus.NO_MATCH);
      if (response.status >= 500 && response.status <= 599) return result(PeerIdentityStatus.UNAVAILABLE);
      if (response.status !== 200) return result(PeerIdentityStatus.INVALID);
      const contentTypes: string[] = [];
      for (let index = 0; index < response.rawHeaders.length; index += 2) {
        if (response.rawHeaders[index].toLowerCase() === "content-type")
          contentTypes.push(response.rawHeaders[index + 1]);
      }
      if (contentTypes.length !== 1 || contentTypes[0].split(";", 1)[0].trim().toLowerCase() !== "application/json") {
        return result(PeerIdentityStatus.INVALID);
      }
      try {
        const payload = parseStrictJson(response.body, maxBody);
        if (payload === null || Array.isArray(payload) || typeof payload !== "object")
          throw new Error("WhoIs must be an object");
        const payloadObject = payload as Record<string, JsonValue>;
        const nodeValue = payloadObject.Node;
        if (nodeValue === null || Array.isArray(nodeValue) || typeof nodeValue !== "object")
          throw new Error("WhoIs lacks Node");
        const node = nodeValue as Record<string, JsonValue>;
        const stableId = optionalString(node, "StableID");
        const nodeName = optionalString(node, "Name");
        const rawTags = node.Tags ?? [];
        if (
          !Array.isArray(rawTags) ||
          rawTags.some((tag) => typeof tag !== "string" || !tag.startsWith("tag:") || !validText(tag))
        ) {
          throw new Error("invalid WhoIs tags");
        }
        const tags = rawTags as string[];
        const caps = payloadObject.CapMap == null ? {} : capabilities(payloadObject.CapMap, false, false);
        const attributes: Record<string, JsonValue> = { tags, capability_target: target };
        if (stableId) attributes.node_id = stableId;
        if (nodeName) attributes.node_name = nodeName;
        let subjectKind: PeerSubjectKind;
        let subjectKey: string;
        if (tags.length > 0) {
          if (!stableId) throw new Error("tagged node lacks StableID");
          subjectKind = PeerSubjectKind.TAGGED_NODE;
          subjectKey = `node:${stableId}`;
        } else {
          const profileValue = payloadObject.UserProfile;
          if (profileValue === null || Array.isArray(profileValue) || typeof profileValue !== "object") {
            throw new Error("untagged node lacks UserProfile");
          }
          const profile = profileValue as Record<string, JsonValue>;
          const id = profile.ID;
          if (typeof id !== "number" || !Number.isSafeInteger(id) || id <= 0) throw new Error("invalid stable user ID");
          subjectKind = PeerSubjectKind.USER;
          subjectKey = `user:${id}`;
          attributes.user_id = String(id);
          const login = optionalString(profile, "LoginName");
          const display = optionalString(profile, "DisplayName");
          if (login) attributes.user_login = login;
          if (display) attributes.user_display_name = display;
        }
        return result(
          PeerIdentityStatus.AVAILABLE,
          new PeerIdentity({
            provider: PROVIDER,
            evidenceSource: "localapi",
            assurance: IdentityAssurance.LOCAL_DAEMON,
            issuer: options.issuer,
            transport: context.transport,
            subjectKind,
            subjectKey,
            subjectStability: SubjectStability.STABLE,
            subjectVerified: true,
            attributes,
            capabilities: caps,
            capabilitiesVerified: true,
            sourceAddress: sourceIp(source),
            proxyAddress: context.assertedPeer ? context.immediatePeer : undefined,
          }),
        );
      } catch {
        return result(PeerIdentityStatus.INVALID);
      }
    },
  };
}

function sourceIp(source: string): string {
  if (source.startsWith("[")) {
    const closing = source.indexOf("]");
    if (closing > 1 && /^:\d+$/.test(source.slice(closing + 1))) return source.slice(1, closing);
  }
  const firstColon = source.indexOf(":");
  const lastColon = source.lastIndexOf(":");
  if (firstColon > 0 && firstColon === lastColon && /^\d+$/.test(source.slice(lastColon + 1))) {
    return source.slice(0, lastColon);
  }
  return source;
}
