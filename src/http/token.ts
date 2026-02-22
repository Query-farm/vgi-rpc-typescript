// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { createHmac, timingSafeEqual } from "node:crypto";

const TOKEN_VERSION = 2;
const HMAC_LEN = 32;
// 1 (version) + 8 (created_at) + 4*3 (three length prefixes) + 32 (hmac)
const MIN_TOKEN_LEN = 1 + 8 + 12 + HMAC_LEN;

/**
 * Pack a state token matching the Python v2 wire format.
 *
 * Layout:
 *   [1B version=2]
 *   [8B created_at uint64 LE (seconds since epoch)]
 *   [4B state_len uint32 LE] [state_len bytes]
 *   [4B schema_len uint32 LE] [schema_len bytes]
 *   [4B input_schema_len uint32 LE] [input_schema_len bytes]
 *   [32B HMAC-SHA256(signing_key, all above bytes)]
 */
export function packStateToken(
  stateBytes: Uint8Array,
  schemaBytes: Uint8Array,
  inputSchemaBytes: Uint8Array,
  signingKey: Uint8Array,
  createdAt?: number,
): string {
  const now = createdAt ?? Math.floor(Date.now() / 1000);

  const payloadLen =
    1 + 8 + 4 + stateBytes.length + 4 + schemaBytes.length + 4 + inputSchemaBytes.length;
  const buf = Buffer.alloc(payloadLen);
  let offset = 0;

  // version
  buf.writeUInt8(TOKEN_VERSION, offset);
  offset += 1;

  // created_at as uint64 LE
  buf.writeBigUInt64LE(BigInt(now), offset);
  offset += 8;

  // state
  buf.writeUInt32LE(stateBytes.length, offset);
  offset += 4;
  buf.set(stateBytes, offset);
  offset += stateBytes.length;

  // output schema
  buf.writeUInt32LE(schemaBytes.length, offset);
  offset += 4;
  buf.set(schemaBytes, offset);
  offset += schemaBytes.length;

  // input schema
  buf.writeUInt32LE(inputSchemaBytes.length, offset);
  offset += 4;
  buf.set(inputSchemaBytes, offset);
  offset += inputSchemaBytes.length;

  // HMAC
  const mac = createHmac("sha256", signingKey).update(buf).digest();
  const token = Buffer.concat([buf, mac]);

  return token.toString("base64");
}

export interface UnpackedToken {
  stateBytes: Uint8Array;
  schemaBytes: Uint8Array;
  inputSchemaBytes: Uint8Array;
  createdAt: number;
}

/**
 * Unpack and verify a state token.
 * Throws on tampered, expired, or malformed tokens.
 */
export function unpackStateToken(
  tokenBase64: string,
  signingKey: Uint8Array,
  tokenTtl: number,
): UnpackedToken {
  const token = Buffer.from(tokenBase64, "base64");

  if (token.length < MIN_TOKEN_LEN) {
    throw new Error("State token too short");
  }

  // Split payload and mac
  const payload = token.subarray(0, token.length - HMAC_LEN);
  const receivedMac = token.subarray(token.length - HMAC_LEN);

  // Verify HMAC first (before inspecting any fields)
  const expectedMac = createHmac("sha256", signingKey).update(payload).digest();
  if (!timingSafeEqual(receivedMac, expectedMac)) {
    throw new Error("State token HMAC verification failed");
  }

  let offset = 0;

  // Version
  const version = payload.readUInt8(offset);
  offset += 1;
  if (version !== TOKEN_VERSION) {
    throw new Error(`Unsupported state token version: ${version}`);
  }

  // created_at
  const createdAt = Number(payload.readBigUInt64LE(offset));
  offset += 8;

  // TTL check
  if (tokenTtl > 0) {
    const now = Math.floor(Date.now() / 1000);
    if (now - createdAt > tokenTtl) {
      throw new Error("State token expired");
    }
  }

  // state bytes
  const stateLen = payload.readUInt32LE(offset);
  offset += 4;
  if (offset + stateLen > payload.length) {
    throw new Error("State token truncated (state)");
  }
  const stateBytes = payload.slice(offset, offset + stateLen);
  offset += stateLen;

  // output schema bytes
  const schemaLen = payload.readUInt32LE(offset);
  offset += 4;
  if (offset + schemaLen > payload.length) {
    throw new Error("State token truncated (schema)");
  }
  const schemaBytes = payload.slice(offset, offset + schemaLen);
  offset += schemaLen;

  // input schema bytes
  const inputSchemaLen = payload.readUInt32LE(offset);
  offset += 4;
  if (offset + inputSchemaLen > payload.length) {
    throw new Error("State token truncated (input schema)");
  }
  const inputSchemaBytes = payload.slice(offset, offset + inputSchemaLen);

  return { stateBytes, schemaBytes, inputSchemaBytes, createdAt };
}
