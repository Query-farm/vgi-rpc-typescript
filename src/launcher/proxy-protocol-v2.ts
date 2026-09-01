// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import type { Socket } from "node:net";

const SIGNATURE = Buffer.from([0x0d, 0x0a, 0x0d, 0x0a, 0, 0x0d, 0x0a, 0x51, 0x55, 0x49, 0x54, 0x0a]);
const FIXED_BYTES = 16;

/** Default bound for a complete PROXY protocol v2 preamble, including TLVs. */
export const DEFAULT_MAX_PROXY_V2_BYTES = 536;

/** Fixed VGI identity TLV used only by an explicitly trusted Iroh bridge. */
export const VGI_IROH_ENDPOINT_TLV = 0xe0;

/** One TCP endpoint asserted by a trusted PROXY protocol sender. */
export interface ProxyProtocolV2Endpoint {
  /** Normalized IPv4 or IPv6 address asserted by the trusted proxy. */
  readonly address: string;
  /** TCP port asserted by the trusted proxy. */
  readonly port: number;
}

/** Asserted TCP endpoints from one strictly validated PROXY protocol v2 preamble. */
export interface ProxyProtocolV2Address {
  /** Original client endpoint asserted by the trusted proxy. */
  readonly source: ProxyProtocolV2Endpoint;
  /** Worker destination endpoint asserted by the trusted proxy. */
  readonly destination: ProxyProtocolV2Endpoint;
}

/** Non-IP peer identity carried by a trusted Iroh bridge. */
export interface ProxyProtocolV2IrohIdentity {
  /** Canonical lowercase hexadecimal encoding of the 32-byte EndpointId. */
  readonly endpointId: string;
}

/** A malformed, truncated, oversized, or timed-out PROXY protocol preamble. */
export class ProxyProtocolV2Error extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ProxyProtocolV2Error";
  }
}

interface NormalizedIp {
  address: string;
  key: string;
}

function ipv4Bytes(value: string): number[] | undefined {
  const parts = value.split(".");
  if (parts.length !== 4) return undefined;
  const bytes: number[] = [];
  for (const part of parts) {
    if (!/^(0|[1-9][0-9]{0,2})$/u.test(part)) return undefined;
    const byte = Number(part);
    if (byte > 255) return undefined;
    bytes.push(byte);
  }
  return bytes;
}

function ipv6Words(value: string): number[] | undefined {
  if (!value || value.includes("%") || value.split("::").length > 2) return undefined;
  const halves = value.split("::");
  const parseHalf = (half: string, allowIpv4: boolean): number[] | undefined => {
    if (!half) return [];
    const pieces = half.split(":");
    const words: number[] = [];
    for (let index = 0; index < pieces.length; index++) {
      const piece = pieces[index];
      if (piece.includes(".")) {
        if (!allowIpv4 || index !== pieces.length - 1) return undefined;
        const bytes = ipv4Bytes(piece);
        if (!bytes) return undefined;
        words.push((bytes[0] << 8) | bytes[1], (bytes[2] << 8) | bytes[3]);
      } else {
        if (!/^[0-9a-f]{1,4}$/iu.test(piece)) return undefined;
        words.push(Number.parseInt(piece, 16));
      }
    }
    return words;
  };
  const left = parseHalf(halves[0], halves.length === 1);
  const right = parseHalf(halves[1] ?? "", true);
  if (!left || !right) return undefined;
  if (halves.length === 1) return left.length === 8 ? left : undefined;
  const omitted = 8 - left.length - right.length;
  if (omitted < 1) return undefined;
  return [...left, ...Array.from({ length: omitted }, () => 0), ...right];
}

function formatIpv6(words: readonly number[]): string {
  let bestStart = -1;
  let bestLength = 0;
  for (let index = 0; index < words.length; ) {
    if (words[index] !== 0) {
      index++;
      continue;
    }
    let end = index;
    while (end < words.length && words[end] === 0) end++;
    if (end - index > bestLength && end - index >= 2) {
      bestStart = index;
      bestLength = end - index;
    }
    index = end;
  }
  if (bestStart < 0) return words.map((word) => word.toString(16)).join(":");
  const left = words
    .slice(0, bestStart)
    .map((word) => word.toString(16))
    .join(":");
  const right = words
    .slice(bestStart + bestLength)
    .map((word) => word.toString(16))
    .join(":");
  return `${left}::${right}`;
}

/** Validate and canonicalize one exact IPv4/IPv6 address (never a CIDR or hostname). */
export function normalizeProxyIpAddress(value: string): string {
  return normalizedIp(value).address;
}

function normalizedIp(value: string): NormalizedIp {
  const ipv4 = ipv4Bytes(value);
  if (ipv4) return { address: ipv4.join("."), key: `4:${ipv4.join(".")}` };
  const words = ipv6Words(value);
  if (!words) throw new TypeError(`trusted proxy must be an exact IPv4 or IPv6 address: ${JSON.stringify(value)}`);
  const mapped = words.slice(0, 5).every((word) => word === 0) && words[5] === 0xffff;
  if (mapped) {
    const bytes = [words[6] >> 8, words[6] & 0xff, words[7] >> 8, words[7] & 0xff];
    return { address: bytes.join("."), key: `4:${bytes.join(".")}` };
  }
  const key = words.map((word) => word.toString(16).padStart(4, "0")).join("");
  return { address: formatIpv6(words), key: `6:${key}` };
}

/** Internal comparison key used to match exact trusted proxy addresses. */
export function proxyIpAddressKey(value: string): string {
  return normalizedIp(value).key;
}

function endpoint(address: string, port: number): ProxyProtocolV2Endpoint {
  return Object.freeze({ address, port });
}

/** Format an endpoint for LocalAPI and peer-resolution contexts. */
export function formatProxyEndpoint(value: ProxyProtocolV2Endpoint): string {
  return `${value.address.includes(":") ? `[${value.address}]` : value.address}:${value.port}`;
}

/**
 * Parse one exact, bounded PROXY protocol v2 preamble.
 *
 * Only the PROXY command with TCP over IPv4 or IPv6 is accepted. LOCAL,
 * UNSPEC, UDP, Unix sockets, malformed address blocks, and malformed TLVs fail
 * closed. Unknown TLVs are structurally validated and otherwise ignored.
 */
export function parseProxyProtocolV2(
  input: Uint8Array,
  maximumBytes = DEFAULT_MAX_PROXY_V2_BYTES,
): ProxyProtocolV2Address {
  if (!Number.isInteger(maximumBytes) || maximumBytes < FIXED_BYTES) {
    throw new TypeError("maximum PROXY v2 bytes must be an integer of at least 16");
  }
  const preamble = Buffer.from(input.buffer, input.byteOffset, input.byteLength);
  if (preamble.length < FIXED_BYTES) throw new ProxyProtocolV2Error("truncated PROXY v2 fixed preamble");
  if (preamble.length > maximumBytes) throw new ProxyProtocolV2Error("PROXY v2 preamble exceeds configured limit");
  if (!preamble.subarray(0, SIGNATURE.length).equals(SIGNATURE)) {
    throw new ProxyProtocolV2Error("missing PROXY v2 signature");
  }
  if (preamble[12] >> 4 !== 2) throw new ProxyProtocolV2Error("unsupported PROXY protocol version");
  if ((preamble[12] & 0x0f) !== 1) throw new ProxyProtocolV2Error("PROXY v2 LOCAL command is not accepted");
  const expected = FIXED_BYTES + preamble.readUInt16BE(14);
  if (preamble.length !== expected) throw new ProxyProtocolV2Error("truncated or overlong PROXY v2 preamble");

  const body = preamble.subarray(FIXED_BYTES);
  let source: ProxyProtocolV2Endpoint;
  let destination: ProxyProtocolV2Endpoint;
  let addressBytes: number;
  if (preamble[13] === 0x11) {
    addressBytes = 12;
    if (body.length < addressBytes) throw new ProxyProtocolV2Error("truncated PROXY v2 TCP/IPv4 address block");
    source = endpoint(`${body[0]}.${body[1]}.${body[2]}.${body[3]}`, body.readUInt16BE(8));
    destination = endpoint(`${body[4]}.${body[5]}.${body[6]}.${body[7]}`, body.readUInt16BE(10));
  } else if (preamble[13] === 0x21) {
    addressBytes = 36;
    if (body.length < addressBytes) throw new ProxyProtocolV2Error("truncated PROXY v2 TCP/IPv6 address block");
    const sourceWords = Array.from({ length: 8 }, (_, index) => body.readUInt16BE(index * 2));
    const destinationWords = Array.from({ length: 8 }, (_, index) => body.readUInt16BE(16 + index * 2));
    source = endpoint(normalizedIp(formatIpv6(sourceWords)).address, body.readUInt16BE(32));
    destination = endpoint(normalizedIp(formatIpv6(destinationWords)).address, body.readUInt16BE(34));
  } else {
    throw new ProxyProtocolV2Error("PROXY v2 requires TCP over IPv4 or IPv6");
  }

  for (let offset = addressBytes; offset < body.length; ) {
    if (body.length - offset < 3) throw new ProxyProtocolV2Error("truncated PROXY v2 TLV header");
    const length = body.readUInt16BE(offset + 1);
    offset += 3;
    if (length > body.length - offset) throw new ProxyProtocolV2Error("truncated PROXY v2 TLV value");
    offset += length;
  }
  return Object.freeze({ source, destination });
}

/** Parse the dedicated PROXY/UNSPEC form emitted by a trusted Iroh bridge. */
export function parseIrohProxyProtocolV2(
  input: Uint8Array,
  maximumBytes = DEFAULT_MAX_PROXY_V2_BYTES,
): ProxyProtocolV2IrohIdentity {
  if (!Number.isInteger(maximumBytes) || maximumBytes < FIXED_BYTES) {
    throw new TypeError("maximum PROXY v2 bytes must be an integer of at least 16");
  }
  const preamble = Buffer.from(input.buffer, input.byteOffset, input.byteLength);
  if (preamble.length < FIXED_BYTES) throw new ProxyProtocolV2Error("truncated PROXY v2 fixed preamble");
  if (preamble.length > maximumBytes) throw new ProxyProtocolV2Error("PROXY v2 preamble exceeds configured limit");
  if (!preamble.subarray(0, SIGNATURE.length).equals(SIGNATURE)) {
    throw new ProxyProtocolV2Error("missing PROXY v2 signature");
  }
  if (preamble[12] !== 0x21) throw new ProxyProtocolV2Error("Iroh identity requires PROXY command version 2");
  if (preamble[13] !== 0x00) throw new ProxyProtocolV2Error("VGI Iroh identity requires PROXY/UNSPEC");
  const expected = FIXED_BYTES + preamble.readUInt16BE(14);
  if (preamble.length !== expected) throw new ProxyProtocolV2Error("truncated or overlong PROXY v2 preamble");

  const body = preamble.subarray(FIXED_BYTES);
  let endpointId: string | undefined;
  for (let offset = 0; offset < body.length; ) {
    if (body.length - offset < 3) throw new ProxyProtocolV2Error("truncated PROXY v2 TLV header");
    const type = body[offset];
    const length = body.readUInt16BE(offset + 1);
    offset += 3;
    if (length > body.length - offset) throw new ProxyProtocolV2Error("truncated PROXY v2 TLV value");
    if (type === VGI_IROH_ENDPOINT_TLV) {
      if (endpointId !== undefined) throw new ProxyProtocolV2Error("duplicate VGI Iroh identity TLV");
      if (length !== 33 || body[offset] !== 1) throw new ProxyProtocolV2Error("invalid VGI Iroh identity TLV");
      endpointId = body.subarray(offset + 1, offset + 33).toString("hex");
    }
    offset += length;
  }
  if (endpointId === undefined) throw new ProxyProtocolV2Error("PROXY/UNSPEC requires one VGI Iroh identity TLV");
  return Object.freeze({ endpointId });
}

/**
 * Consume exactly one preamble under a single independent deadline.
 * Bytes received after its declared length are pushed back for Arrow IPC.
 */
export function readProxyProtocolV2(
  socket: Socket,
  timeoutMs: number,
  maximumBytes = DEFAULT_MAX_PROXY_V2_BYTES,
): Promise<ProxyProtocolV2Address> {
  return readProxyProtocolV2Preamble(socket, timeoutMs, maximumBytes).then((preamble) =>
    parseProxyProtocolV2(preamble, maximumBytes),
  );
}

/** Consume and parse the dedicated trusted Iroh PROXY/UNSPEC preamble. */
export function readIrohProxyProtocolV2(
  socket: Socket,
  timeoutMs: number,
  maximumBytes = DEFAULT_MAX_PROXY_V2_BYTES,
): Promise<ProxyProtocolV2IrohIdentity> {
  return readProxyProtocolV2Preamble(socket, timeoutMs, maximumBytes).then((preamble) =>
    parseIrohProxyProtocolV2(preamble, maximumBytes),
  );
}

function readProxyProtocolV2Preamble(socket: Socket, timeoutMs: number, maximumBytes: number): Promise<Buffer> {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) throw new TypeError("PROXY preamble timeout must be positive");
  if (!Number.isInteger(maximumBytes) || maximumBytes < FIXED_BYTES) {
    throw new TypeError("maximum PROXY v2 bytes must be an integer of at least 16");
  }
  return new Promise((resolve, reject) => {
    const parts: Buffer[] = [];
    let received = 0;
    let expected = FIXED_BYTES;
    let settled = false;
    const cleanup = () => {
      clearTimeout(timer);
      socket.off("data", onData);
      socket.off("end", onEnd);
      socket.off("close", onClose);
      socket.off("error", onError);
    };
    const fail = (error: Error) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(error);
    };
    const complete = (excess?: Buffer) => {
      if (settled) return;
      settled = true;
      socket.pause();
      cleanup();
      if (excess && excess.length > 0) socket.unshift(excess);
      try {
        resolve(Buffer.concat(parts, received));
      } catch (error) {
        reject(error);
      }
    };
    const onData = (chunk: Buffer) => {
      let offset = 0;
      while (offset < chunk.length && received < expected) {
        const count = Math.min(chunk.length - offset, expected - received);
        parts.push(chunk.subarray(offset, offset + count));
        received += count;
        offset += count;
        if (received === FIXED_BYTES && expected === FIXED_BYTES) {
          const fixed = Buffer.concat(parts, FIXED_BYTES);
          expected = FIXED_BYTES + fixed.readUInt16BE(14);
          if (expected > maximumBytes) {
            fail(new ProxyProtocolV2Error("PROXY v2 preamble exceeds configured limit"));
            return;
          }
        }
      }
      if (received === expected) complete(offset < chunk.length ? chunk.subarray(offset) : undefined);
    };
    const onEnd = () => fail(new ProxyProtocolV2Error("truncated PROXY v2 preamble"));
    const onClose = () => fail(new ProxyProtocolV2Error("connection closed during PROXY v2 preamble"));
    const onError = () => fail(new ProxyProtocolV2Error("connection failed during PROXY v2 preamble"));
    const timer = setTimeout(() => fail(new ProxyProtocolV2Error("PROXY v2 preamble deadline elapsed")), timeoutMs);
    timer.unref?.();
    socket.on("data", onData);
    socket.once("end", onEnd);
    socket.once("close", onClose);
    socket.once("error", onError);
    socket.resume();
  });
}
