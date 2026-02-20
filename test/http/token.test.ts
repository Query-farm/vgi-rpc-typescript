import { describe, test, expect } from "bun:test";
import { packStateToken, unpackStateToken } from "../../src/http/token.js";
import { jsonStateSerializer } from "../../src/http/types.js";
import { randomBytes } from "node:crypto";

describe("State Token", () => {
  const signingKey = randomBytes(32);

  test("pack and unpack round-trips correctly", () => {
    const stateBytes = new TextEncoder().encode('{"count":5}');
    const schemaBytes = new Uint8Array([1, 2, 3, 4]);
    const inputSchemaBytes = new Uint8Array([5, 6, 7]);

    const token = packStateToken(
      stateBytes,
      schemaBytes,
      inputSchemaBytes,
      signingKey,
    );

    expect(typeof token).toBe("string");

    const unpacked = unpackStateToken(token, signingKey, 3600);
    expect(new TextDecoder().decode(unpacked.stateBytes)).toBe('{"count":5}');
    expect(Array.from(unpacked.schemaBytes)).toEqual([1, 2, 3, 4]);
    expect(Array.from(unpacked.inputSchemaBytes)).toEqual([5, 6, 7]);
    expect(unpacked.createdAt).toBeGreaterThan(0);
  });

  test("HMAC verification fails with wrong key", () => {
    const stateBytes = new TextEncoder().encode("{}");
    const schemaBytes = new Uint8Array([1]);
    const inputSchemaBytes = new Uint8Array([2]);

    const token = packStateToken(stateBytes, schemaBytes, inputSchemaBytes, signingKey);
    const wrongKey = randomBytes(32);

    expect(() => unpackStateToken(token, wrongKey, 3600)).toThrow(
      "HMAC verification failed",
    );
  });

  test("detects tampered token", () => {
    const stateBytes = new TextEncoder().encode("{}");
    const schemaBytes = new Uint8Array([1]);
    const inputSchemaBytes = new Uint8Array([2]);

    const token = packStateToken(stateBytes, schemaBytes, inputSchemaBytes, signingKey);

    // Decode, tamper, re-encode
    const buf = Buffer.from(token, "base64");
    buf[10] ^= 0xff; // flip a byte in the state section
    const tamperedToken = buf.toString("base64");

    expect(() => unpackStateToken(tamperedToken, signingKey, 3600)).toThrow(
      "HMAC verification failed",
    );
  });

  test("TTL expiration", () => {
    const stateBytes = new TextEncoder().encode("{}");
    const schemaBytes = new Uint8Array([1]);
    const inputSchemaBytes = new Uint8Array([2]);

    // Created 2 hours ago
    const twoHoursAgo = Math.floor(Date.now() / 1000) - 7200;
    const token = packStateToken(
      stateBytes,
      schemaBytes,
      inputSchemaBytes,
      signingKey,
      twoHoursAgo,
    );

    // 1-hour TTL should reject it
    expect(() => unpackStateToken(token, signingKey, 3600)).toThrow(
      "State token expired",
    );

    // 0 TTL (disabled) should accept it
    const unpacked = unpackStateToken(token, signingKey, 0);
    expect(unpacked.createdAt).toBe(twoHoursAgo);
  });

  test("rejects too-short token", () => {
    const shortToken = Buffer.from("too short").toString("base64");
    expect(() => unpackStateToken(shortToken, signingKey, 3600)).toThrow(
      "State token too short",
    );
  });

  test("handles empty state", () => {
    const stateBytes = new Uint8Array(0);
    const schemaBytes = new Uint8Array([1, 2]);
    const inputSchemaBytes = new Uint8Array([3, 4]);

    const token = packStateToken(stateBytes, schemaBytes, inputSchemaBytes, signingKey);
    const unpacked = unpackStateToken(token, signingKey, 3600);
    expect(unpacked.stateBytes.length).toBe(0);
  });

  test("jsonStateSerializer round-trips BigInt values", () => {
    const state = { count: 5, bigVal: BigInt("9007199254740993"), nested: { x: BigInt(-42) } };
    const bytes = jsonStateSerializer.serialize(state);
    const restored = jsonStateSerializer.deserialize(bytes);
    expect(restored.count).toBe(5);
    expect(restored.bigVal).toBe(BigInt("9007199254740993"));
    expect(restored.nested.x).toBe(BigInt(-42));
  });

  test("handles large state", () => {
    const stateBytes = randomBytes(10000);
    const schemaBytes = randomBytes(500);
    const inputSchemaBytes = randomBytes(500);

    const token = packStateToken(stateBytes, schemaBytes, inputSchemaBytes, signingKey);
    const unpacked = unpackStateToken(token, signingKey, 3600);
    expect(Buffer.from(unpacked.stateBytes).equals(stateBytes)).toBe(true);
    expect(Buffer.from(unpacked.schemaBytes).equals(schemaBytes)).toBe(true);
    expect(Buffer.from(unpacked.inputSchemaBytes).equals(inputSchemaBytes)).toBe(true);
  });
});
