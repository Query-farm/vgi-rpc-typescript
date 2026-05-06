// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import { derivePrincipalKey, packStateToken, unpackStateToken } from "../../src/http/token.js";
import { jsonStateSerializer } from "../../src/http/types.js";
import { randomBytes } from "../../src/util/web-crypto.js";

describe("State Token", () => {
  const signingKey = randomBytes(32);

  test("pack and unpack round-trips correctly", async () => {
    const stateBytes = new TextEncoder().encode('{"count":5}');
    const schemaBytes = new Uint8Array([1, 2, 3, 4]);
    const inputSchemaBytes = new Uint8Array([5, 6, 7]);

    const token = await packStateToken(stateBytes, schemaBytes, inputSchemaBytes, signingKey);

    expect(typeof token).toBe("string");

    const unpacked = await unpackStateToken(token, signingKey, 3600);
    expect(new TextDecoder().decode(unpacked.stateBytes)).toBe('{"count":5}');
    expect(Array.from(unpacked.schemaBytes)).toEqual([1, 2, 3, 4]);
    expect(Array.from(unpacked.inputSchemaBytes)).toEqual([5, 6, 7]);
    expect(unpacked.createdAt).toBeGreaterThan(0);
  });

  test("HMAC verification fails with wrong key", async () => {
    const stateBytes = new TextEncoder().encode("{}");
    const schemaBytes = new Uint8Array([1]);
    const inputSchemaBytes = new Uint8Array([2]);

    const token = await packStateToken(stateBytes, schemaBytes, inputSchemaBytes, signingKey);
    const wrongKey = randomBytes(32);

    await expect(unpackStateToken(token, wrongKey, 3600)).rejects.toThrow("HMAC verification failed");
  });

  test("detects tampered token", async () => {
    const stateBytes = new TextEncoder().encode("{}");
    const schemaBytes = new Uint8Array([1]);
    const inputSchemaBytes = new Uint8Array([2]);

    const token = await packStateToken(stateBytes, schemaBytes, inputSchemaBytes, signingKey);

    // Decode, tamper, re-encode (use base64 helpers — Buffer is Node-only).
    const bin = atob(token);
    const buf = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) buf[i] = bin.charCodeAt(i);
    buf[10] ^= 0xff; // flip a byte in the state section
    let tampered = "";
    for (let i = 0; i < buf.length; i += 0x8000) {
      tampered += String.fromCharCode(...buf.subarray(i, i + 0x8000));
    }
    const tamperedToken = btoa(tampered);

    await expect(unpackStateToken(tamperedToken, signingKey, 3600)).rejects.toThrow(
      "HMAC verification failed",
    );
  });

  test("TTL expiration", async () => {
    const stateBytes = new TextEncoder().encode("{}");
    const schemaBytes = new Uint8Array([1]);
    const inputSchemaBytes = new Uint8Array([2]);

    // Created 2 hours ago
    const twoHoursAgo = Math.floor(Date.now() / 1000) - 7200;
    const token = await packStateToken(stateBytes, schemaBytes, inputSchemaBytes, signingKey, twoHoursAgo);

    // 1-hour TTL should reject it
    await expect(unpackStateToken(token, signingKey, 3600)).rejects.toThrow("State token expired");

    // 0 TTL (disabled) should accept it
    const unpacked = await unpackStateToken(token, signingKey, 0);
    expect(unpacked.createdAt).toBe(twoHoursAgo);
  });

  test("rejects too-short token", async () => {
    const shortToken = btoa("too short");
    await expect(unpackStateToken(shortToken, signingKey, 3600)).rejects.toThrow(
      "State token too short",
    );
  });

  test("handles empty state", async () => {
    const stateBytes = new Uint8Array(0);
    const schemaBytes = new Uint8Array([1, 2]);
    const inputSchemaBytes = new Uint8Array([3, 4]);

    const token = await packStateToken(stateBytes, schemaBytes, inputSchemaBytes, signingKey);
    const unpacked = await unpackStateToken(token, signingKey, 3600);
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

  test("derivePrincipalKey returns base key when principal is empty", async () => {
    expect(await derivePrincipalKey(signingKey, null)).toBe(signingKey);
    expect(await derivePrincipalKey(signingKey, undefined)).toBe(signingKey);
    expect(await derivePrincipalKey(signingKey, "")).toBe(signingKey);
  });

  test("derivePrincipalKey produces distinct keys per principal", async () => {
    const a = await derivePrincipalKey(signingKey, "alice");
    const b = await derivePrincipalKey(signingKey, "bob");
    const a2 = await derivePrincipalKey(signingKey, "alice");
    const equal = (x: Uint8Array, y: Uint8Array) =>
      x.length === y.length && x.every((v, i) => v === y[i]);
    expect(equal(a, a2)).toBe(true);
    expect(equal(a, b)).toBe(false);
    expect(equal(a, signingKey)).toBe(false);
  });

  test("token signed for one principal cannot be verified by another", async () => {
    const stateBytes = new TextEncoder().encode("{}");
    const schemaBytes = new Uint8Array([1]);
    const inputSchemaBytes = new Uint8Array([2]);

    const aliceKey = await derivePrincipalKey(signingKey, "alice");
    const bobKey = await derivePrincipalKey(signingKey, "bob");

    const aliceToken = await packStateToken(stateBytes, schemaBytes, inputSchemaBytes, aliceKey);

    // Alice can verify her own token.
    await expect(unpackStateToken(aliceToken, aliceKey, 3600)).resolves.toBeDefined();
    // Bob cannot replay Alice's token.
    await expect(unpackStateToken(aliceToken, bobKey, 3600)).rejects.toThrow(
      "HMAC verification failed",
    );
    // Anonymous (base key) cannot replay it either.
    await expect(unpackStateToken(aliceToken, signingKey, 3600)).rejects.toThrow(
      "HMAC verification failed",
    );
  });

  test("handles large state", async () => {
    const stateBytes = randomBytes(10000);
    const schemaBytes = randomBytes(500);
    const inputSchemaBytes = randomBytes(500);

    const token = await packStateToken(stateBytes, schemaBytes, inputSchemaBytes, signingKey);
    const unpacked = await unpackStateToken(token, signingKey, 3600);
    const equal = (x: Uint8Array, y: Uint8Array) =>
      x.length === y.length && x.every((v, i) => v === y[i]);
    expect(equal(unpacked.stateBytes, stateBytes)).toBe(true);
    expect(equal(unpacked.schemaBytes, schemaBytes)).toBe(true);
    expect(equal(unpacked.inputSchemaBytes, inputSchemaBytes)).toBe(true);
  });
});
