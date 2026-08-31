// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import {
  computeAad,
  computeCallAad,
  packCallToken,
  packStateToken,
  unpackCallToken,
  unpackStateToken,
} from "../../src/http/token.js";
import { jsonStateSerializer } from "../../src/http/types.js";
import { randomBytes } from "../../src/util/web-crypto.js";

describe("State Token", () => {
  test("bound authenticated empty principals remain domain-separated", () => {
    expect(computeAad("", "binding", "domain-a")).not.toEqual(computeAad("", "binding", "domain-b"));
    expect(computeCallAad("", "binding", "domain-a")).not.toEqual(computeCallAad("", "binding", "domain-b"));
    expect(computeAad("", "binding", "domain-a")).not.toEqual(computeAad(null, "binding", "domain-a"));
    expect(computeCallAad("", "binding", "domain-a")).not.toEqual(computeCallAad(null, "binding", "domain-a"));
  });

  test("peer evidence selects v5 AAD and binds domain, principal, and digest", () => {
    const aad = computeAad("alice", "evidence-digest", "oauth");
    expect(new TextDecoder().decode(aad)).toBe("vgi_rpc.state.v5\0\x01oauth\0alice\0evidence-digest");
  });

  test("peer-bound tokens reject a changed evidence digest or auth domain", () => {
    const token = packStateToken(
      new Uint8Array([1]),
      CALL_ID,
      tokenKey,
      "alice",
      undefined,
      "evidence-digest",
      "oauth",
    );
    expect(() => unpackStateToken(token, tokenKey, 3600, "alice", "evidence-digest", "oauth")).not.toThrow();
    expect(() => unpackStateToken(token, tokenKey, 3600, "alice", "other-digest", "oauth")).toThrow(
      "signature verification failed",
    );
    expect(() => unpackStateToken(token, tokenKey, 3600, "alice", "evidence-digest", "other-domain")).toThrow(
      "signature verification failed",
    );
  });

  test("peer-bound call tokens reject a changed digest or empty-principal domain", () => {
    const token = packCallToken(
      CALL_ID,
      new Uint8Array([1]),
      new Uint8Array([2]),
      tokenKey,
      "",
      undefined,
      "binding",
      "domain-a",
    );
    expect(() => unpackCallToken(token, tokenKey, "", 3600, "binding", "domain-a")).not.toThrow();
    expect(() => unpackCallToken(token, tokenKey, "", 3600, "other", "domain-a")).toThrow(
      "signature verification failed",
    );
    expect(() => unpackCallToken(token, tokenKey, "", 3600, "binding", "domain-b")).toThrow(
      "signature verification failed",
    );
    expect(() => unpackCallToken(token, tokenKey, null, 3600, "binding", "domain-a")).toThrow(
      "signature verification failed",
    );
  });
  const tokenKey = randomBytes(32);
  const CALL_ID = new Uint8Array(16).fill(7);
  const ANON = "";

  test("pack and unpack round-trips correctly", () => {
    const stateBytes = new TextEncoder().encode('{"count":5}');
    const schemaBytes = new Uint8Array([1, 2, 3, 4]);
    const inputSchemaBytes = new Uint8Array([5, 6, 7]);

    const token = packStateToken(stateBytes, CALL_ID, tokenKey, ANON);
    expect(typeof token).toBe("string");

    const unpacked = unpackStateToken(token, tokenKey, 3600, ANON);
    expect(new TextDecoder().decode(unpacked.stateBytes)).toBe('{"count":5}');
    expect(Array.from(unpacked.callId)).toEqual(Array.from(CALL_ID));
    expect(unpacked.createdAt).toBeGreaterThan(0);

    // The schemas ride the call token now, not the cursor.
    const callToken = packCallToken(CALL_ID, schemaBytes, inputSchemaBytes, tokenKey, ANON);
    const { callId, call } = unpackCallToken(callToken, tokenKey, ANON, 3600);
    expect(Array.from(callId)).toEqual(Array.from(CALL_ID));
    expect(Array.from(call.schemaBytes)).toEqual([1, 2, 3, 4]);
    expect(Array.from(call.inputSchemaBytes)).toEqual([5, 6, 7]);
  });

  test("a call token cannot be presented as a cursor, or vice versa", () => {
    // The two AADs carry different version-tagged prefixes, so a swap fails
    // the AEAD tag check rather than decoding into a payload the reader
    // would misinterpret.
    const cursor = packStateToken(new Uint8Array([1]), CALL_ID, tokenKey, ANON);
    const call = packCallToken(CALL_ID, new Uint8Array([2]), new Uint8Array([3]), tokenKey, ANON);
    expect(() => unpackStateToken(call, tokenKey, 3600, ANON)).toThrow();
    expect(() => unpackCallToken(cursor, tokenKey, ANON, 3600)).toThrow();
  });

  test("decryption fails with wrong key", () => {
    const token = packStateToken(new Uint8Array([1]), CALL_ID, tokenKey, ANON);
    const wrongKey = randomBytes(32);
    expect(() => unpackStateToken(token, wrongKey, 3600, ANON)).toThrow("signature verification failed");
  });

  test("detects tampered ciphertext", () => {
    const token = packStateToken(new Uint8Array([1]), CALL_ID, tokenKey, ANON);

    const bin = atob(token);
    const buf = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) buf[i] = bin.charCodeAt(i);
    // Flip a byte inside the ciphertext (skip version=1 + nonce=24 = 25-byte header).
    buf[26] ^= 0xff;
    let s = "";
    for (let i = 0; i < buf.length; i += 0x8000) {
      s += String.fromCharCode(...buf.subarray(i, i + 0x8000));
    }
    const tamperedToken = btoa(s);

    expect(() => unpackStateToken(tamperedToken, tokenKey, 3600, ANON)).toThrow("signature verification failed");
  });

  test("detects tampered nonce", () => {
    const token = packStateToken(new Uint8Array([1]), CALL_ID, tokenKey, ANON);
    const bin = atob(token);
    const buf = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) buf[i] = bin.charCodeAt(i);
    buf[1] ^= 0x01; // first nonce byte
    let s = "";
    for (let i = 0; i < buf.length; i += 0x8000) {
      s += String.fromCharCode(...buf.subarray(i, i + 0x8000));
    }
    const tamperedToken = btoa(s);
    expect(() => unpackStateToken(tamperedToken, tokenKey, 3600, ANON)).toThrow("signature verification failed");
  });

  test("rejects unknown token version", () => {
    const token = packStateToken(new Uint8Array([1]), CALL_ID, tokenKey, ANON);
    const bin = atob(token);
    const buf = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) buf[i] = bin.charCodeAt(i);
    buf[0] = 0x99;
    let s = "";
    for (let i = 0; i < buf.length; i += 0x8000) {
      s += String.fromCharCode(...buf.subarray(i, i + 0x8000));
    }
    const tamperedToken = btoa(s);
    expect(() => unpackStateToken(tamperedToken, tokenKey, 3600, ANON)).toThrow("Unsupported state token version");
  });

  test("rejects malformed base64", () => {
    expect(() => unpackStateToken("not!base64!", tokenKey, 3600, ANON)).toThrow();
  });

  test("TTL expiration", () => {
    // Created 2 hours ago
    const twoHoursAgo = Math.floor(Date.now() / 1000) - 7200;
    const token = packStateToken(new Uint8Array([1]), CALL_ID, tokenKey, ANON, twoHoursAgo);

    // 1-hour TTL should reject it
    expect(() => unpackStateToken(token, tokenKey, 3600, ANON)).toThrow("State token expired");

    // 0 TTL (disabled) should accept it
    const unpacked = unpackStateToken(token, tokenKey, 0, ANON);
    expect(unpacked.createdAt).toBe(twoHoursAgo);
  });

  test("rejects too-short token", () => {
    const shortToken = btoa("too short");
    expect(() => unpackStateToken(shortToken, tokenKey, 3600, ANON)).toThrow();
  });

  test("handles empty state", () => {
    const stateBytes = new Uint8Array(0);

    const token = packStateToken(stateBytes, CALL_ID, tokenKey, ANON);
    const unpacked = unpackStateToken(token, tokenKey, 3600, ANON);
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

  test("token sealed for one principal cannot be opened by another", () => {
    const stateBytes = new TextEncoder().encode("{}");

    const aliceToken = packStateToken(stateBytes, CALL_ID, tokenKey, "alice");

    // Alice can open her own token.
    expect(() => unpackStateToken(aliceToken, tokenKey, 3600, "alice")).not.toThrow();
    // Bob cannot replay Alice's token.
    expect(() => unpackStateToken(aliceToken, tokenKey, 3600, "bob")).toThrow("signature verification failed");
    // Anonymous cannot replay it either.
    expect(() => unpackStateToken(aliceToken, tokenKey, 3600, ANON)).toThrow("signature verification failed");
  });

  test("anonymous token cannot be opened by a named principal", () => {
    const token = packStateToken(new Uint8Array([1]), CALL_ID, tokenKey, ANON);
    expect(() => unpackStateToken(token, tokenKey, 3600, "alice")).toThrow("signature verification failed");
  });

  test("handles large state", () => {
    const stateBytes = randomBytes(10000);
    const schemaBytes = randomBytes(500);
    const inputSchemaBytes = randomBytes(500);

    const token = packStateToken(stateBytes, CALL_ID, tokenKey, ANON);
    const unpacked = unpackStateToken(token, tokenKey, 3600, ANON);
    const equal = (x: Uint8Array, y: Uint8Array) => x.length === y.length && x.every((v, i) => v === y[i]);
    expect(equal(unpacked.stateBytes, stateBytes)).toBe(true);

    const callToken = packCallToken(CALL_ID, schemaBytes, inputSchemaBytes, tokenKey, ANON);
    const { call } = unpackCallToken(callToken, tokenKey, ANON, 3600);
    expect(equal(call.schemaBytes, schemaBytes)).toBe(true);
    expect(equal(call.inputSchemaBytes, inputSchemaBytes)).toBe(true);
  });
});
