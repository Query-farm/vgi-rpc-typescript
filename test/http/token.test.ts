// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import { openBytes } from "../../src/crypto.js";
import {
  base64ToBytes,
  computeAad,
  computeCallAad,
  packCallToken,
  packSchemas,
  packStateToken,
  SERVER_SCOPE,
  type TokenScope,
  unpackCallToken,
  unpackSchemas,
  unpackStateToken,
} from "../../src/http/token.js";
import { jsonStateSerializer } from "../../src/http/types.js";
import { inlineStateRefBytes } from "../../src/state-ref.js";
import { unpackToken } from "../../src/token-codec.js";
import { randomBytes } from "../../src/util/web-crypto.js";

/** A call token over two schemas, with `schemas_ref` inline (no storage). */
function packCall(
  callId: Uint8Array,
  schema: Uint8Array,
  inputSchema: Uint8Array,
  key: Uint8Array,
  tokenScope: TokenScope | string,
  createdAt?: number,
  budget?: { responseLimitBytes?: number; preferredResponseBytes?: number },
): string {
  return packCallToken(
    callId,
    inlineStateRefBytes(packSchemas(schema, inputSchema)),
    key,
    tokenScope as TokenScope,
    createdAt,
    budget,
  );
}

/** Open a call token and resolve its inline `schemas_ref`. */
function unpackCall(token: string, key: Uint8Array, tokenScope: TokenScope, ttl = 0) {
  const opened = unpackCallToken(token, key, tokenScope, ttl);
  expect(opened.schemasRef[0]).toBe(0x01);
  const schemas = unpackSchemas(unpackToken(opened.schemasRef.subarray(1)));
  return {
    callId: opened.callId,
    call: {
      ...schemas,
      responseLimitBytes: opened.responseLimitBytes,
      preferredResponseBytes: opened.preferredResponseBytes,
    },
  };
}

/** The protocol every token in this file is scoped to unless a test varies it. */
const PROTO = "test.Svc.v1";

/** Terse scope builder: the fields a test is not varying stay at their
 *  defaults, so what each assertion is actually changing stays visible. */
function scope(over: Partial<TokenScope> = {}): TokenScope {
  return { protocol: PROTO, principal: null, ...over };
}

describe("State Token", () => {
  test("call tokens compress wide schemas and round-trip them exactly", () => {
    const tokenKey = new Uint8Array(32).fill(7);
    const callId = new Uint8Array(16).fill(3);
    // A wide, repetitive schema, the shape of echo over all_types.
    const schema = new TextEncoder().encode("field_name_with_a_long_type_descriptor;".repeat(300));
    const token = packCall(callId, schema, schema, tokenKey, scope());
    expect(token.length).toBeLessThan(schema.length / 4);
    const { callId: back, call } = unpackCall(token, tokenKey, scope());
    expect(back).toEqual(callId);
    expect(call.schemaBytes).toEqual(schema);
    expect(call.inputSchemaBytes).toEqual(schema);
  });
  test("call tokens seal response budgets", () => {
    const tokenKey = new Uint8Array(32).fill(7);
    const callId = new Uint8Array(16).fill(3);
    const token = packCall(callId, new Uint8Array([1]), new Uint8Array([2]), tokenKey, scope(), undefined, {
      responseLimitBytes: 65_536,
      preferredResponseBytes: 65_536,
    });
    const { call } = unpackCall(token, tokenKey, scope());
    expect(call.responseLimitBytes).toBe(65_536);
    expect(call.preferredResponseBytes).toBe(65_536);
  });
  test("bound authenticated empty principals remain domain-separated", () => {
    const a = scope({ principal: "", evidenceBinding: "binding", domain: "domain-a" });
    const b = scope({ principal: "", evidenceBinding: "binding", domain: "domain-b" });
    const anon = scope({ principal: null, evidenceBinding: "binding", domain: "domain-a" });
    expect(computeAad(a)).not.toEqual(computeAad(b));
    expect(computeCallAad(a)).not.toEqual(computeCallAad(b));
    expect(computeAad(a)).not.toEqual(computeAad(anon));
    expect(computeCallAad(a)).not.toEqual(computeCallAad(anon));
  });

  test("peer evidence selects the bound AAD and binds domain, principal, digest, and protocol", () => {
    const aad = computeAad(scope({ principal: "alice", evidenceBinding: "evidence-digest", domain: "oauth" }));
    expect(new TextDecoder().decode(aad)).toBe(`vgi_rpc.state.v7\0\x01oauth\0alice\0evidence-digest\0${PROTO}`);
  });

  test("the protocol is part of the AAD, so a cross-protocol replay fails the tag check", () => {
    const token = packStateToken(new Uint8Array([1]), CALL_ID, tokenKey, scope({ principal: "alice" }));
    expect(() => unpackStateToken(token, tokenKey, 3600, scope({ principal: "alice" }))).not.toThrow();
    expect(() =>
      unpackStateToken(token, tokenKey, 3600, scope({ principal: "alice", protocol: "other.Svc.v1" })),
    ).toThrow("signature verification failed");
  });

  test("a call token is bound to its protocol too, so a cold-cache continuation cannot cross", () => {
    const token = packCall(CALL_ID, new Uint8Array([1]), new Uint8Array([2]), tokenKey, scope());
    expect(() => unpackCall(token, tokenKey, scope(), 3600)).not.toThrow();
    expect(() => unpackCall(token, tokenKey, scope({ protocol: "other.Svc.v1" }), 3600)).toThrow(
      "signature verification failed",
    );
  });

  test("a server-scoped token is not interchangeable with a protocol-scoped one", () => {
    const token = packStateToken(new Uint8Array([1]), CALL_ID, tokenKey, scope({ protocol: SERVER_SCOPE }));
    expect(() => unpackStateToken(token, tokenKey, 3600, scope())).toThrow("signature verification failed");
  });

  test("peer-bound tokens reject a changed evidence digest or auth domain", () => {
    const bound = scope({ principal: "alice", evidenceBinding: "evidence-digest", domain: "oauth" });
    const token = packStateToken(new Uint8Array([1]), CALL_ID, tokenKey, bound);
    expect(() => unpackStateToken(token, tokenKey, 3600, bound)).not.toThrow();
    expect(() => unpackStateToken(token, tokenKey, 3600, { ...bound, evidenceBinding: "other-digest" })).toThrow(
      "signature verification failed",
    );
    expect(() => unpackStateToken(token, tokenKey, 3600, { ...bound, domain: "other-domain" })).toThrow(
      "signature verification failed",
    );
  });

  test("peer-bound call tokens reject a changed digest or empty-principal domain", () => {
    const bound = scope({ principal: "", evidenceBinding: "binding", domain: "domain-a" });
    const token = packCall(CALL_ID, new Uint8Array([1]), new Uint8Array([2]), tokenKey, bound);
    expect(() => unpackCall(token, tokenKey, bound, 3600)).not.toThrow();
    expect(() => unpackCall(token, tokenKey, { ...bound, evidenceBinding: "other" }, 3600)).toThrow(
      "signature verification failed",
    );
    expect(() => unpackCall(token, tokenKey, { ...bound, domain: "domain-b" }, 3600)).toThrow(
      "signature verification failed",
    );
    expect(() => unpackCall(token, tokenKey, { ...bound, principal: null }, 3600)).toThrow(
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

    const token = packStateToken(stateBytes, CALL_ID, tokenKey, scope({ principal: ANON }));
    expect(typeof token).toBe("string");

    const unpacked = unpackStateToken(token, tokenKey, 3600, scope({ principal: ANON }));
    expect(new TextDecoder().decode(unpacked.stateBytes)).toBe('{"count":5}');
    expect(Array.from(unpacked.callId)).toEqual(Array.from(CALL_ID));
    expect(unpacked.createdAt).toBeGreaterThan(0);

    // The schemas ride the call token now, not the cursor.
    const callToken = packCall(CALL_ID, schemaBytes, inputSchemaBytes, tokenKey, scope({ principal: ANON }));
    const { callId, call } = unpackCall(callToken, tokenKey, scope({ principal: ANON }), 3600);
    expect(Array.from(callId)).toEqual(Array.from(CALL_ID));
    expect(Array.from(call.schemaBytes)).toEqual([1, 2, 3, 4]);
    expect(Array.from(call.inputSchemaBytes)).toEqual([5, 6, 7]);
  });

  test("a call token cannot be presented as a cursor, or vice versa", () => {
    // The two AADs carry different version-tagged prefixes, so a swap fails
    // the AEAD tag check rather than decoding into a payload the reader
    // would misinterpret.
    const cursor = packStateToken(new Uint8Array([1]), CALL_ID, tokenKey, scope({ principal: ANON }));
    const call = packCall(CALL_ID, new Uint8Array([2]), new Uint8Array([3]), tokenKey, ANON);
    expect(() => unpackStateToken(call, tokenKey, 3600, scope({ principal: ANON }))).toThrow();
    expect(() => unpackCall(cursor, tokenKey, scope({ principal: ANON }), 3600)).toThrow();
  });

  test("decryption fails with wrong key", () => {
    const token = packStateToken(new Uint8Array([1]), CALL_ID, tokenKey, scope({ principal: ANON }));
    const wrongKey = randomBytes(32);
    expect(() => unpackStateToken(token, wrongKey, 3600, scope({ principal: ANON }))).toThrow(
      "signature verification failed",
    );
  });

  test("detects tampered ciphertext", () => {
    const token = packStateToken(new Uint8Array([1]), CALL_ID, tokenKey, scope({ principal: ANON }));

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

    expect(() => unpackStateToken(tamperedToken, tokenKey, 3600, scope({ principal: ANON }))).toThrow(
      "signature verification failed",
    );
  });

  test("detects tampered nonce", () => {
    const token = packStateToken(new Uint8Array([1]), CALL_ID, tokenKey, scope({ principal: ANON }));
    const bin = atob(token);
    const buf = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) buf[i] = bin.charCodeAt(i);
    buf[1] ^= 0x01; // first nonce byte
    let s = "";
    for (let i = 0; i < buf.length; i += 0x8000) {
      s += String.fromCharCode(...buf.subarray(i, i + 0x8000));
    }
    const tamperedToken = btoa(s);
    expect(() => unpackStateToken(tamperedToken, tokenKey, 3600, scope({ principal: ANON }))).toThrow(
      "signature verification failed",
    );
  });

  test("rejects unknown token version", () => {
    const token = packStateToken(new Uint8Array([1]), CALL_ID, tokenKey, scope({ principal: ANON }));
    const bin = atob(token);
    const buf = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) buf[i] = bin.charCodeAt(i);
    buf[0] = 0x99;
    let s = "";
    for (let i = 0; i < buf.length; i += 0x8000) {
      s += String.fromCharCode(...buf.subarray(i, i + 0x8000));
    }
    const tamperedToken = btoa(s);
    expect(() => unpackStateToken(tamperedToken, tokenKey, 3600, scope({ principal: ANON }))).toThrow(
      "Unsupported state token version",
    );
  });

  test("rejects malformed base64", () => {
    expect(() => unpackStateToken("not!base64!", tokenKey, 3600, scope({ principal: ANON }))).toThrow();
  });

  test("TTL expiration", () => {
    // Created 2 hours ago
    const twoHoursAgo = Math.floor(Date.now() / 1000) - 7200;
    const token = packStateToken(new Uint8Array([1]), CALL_ID, tokenKey, scope({ principal: ANON }), twoHoursAgo);

    // 1-hour TTL should reject it
    expect(() => unpackStateToken(token, tokenKey, 3600, scope({ principal: ANON }))).toThrow("State token expired");

    // 0 TTL (disabled) should accept it
    const unpacked = unpackStateToken(token, tokenKey, 0, scope({ principal: ANON }));
    expect(unpacked.createdAt).toBe(twoHoursAgo);
  });

  test("rejects too-short token", () => {
    const shortToken = btoa("too short");
    expect(() => unpackStateToken(shortToken, tokenKey, 3600, scope({ principal: ANON }))).toThrow();
  });

  test("handles empty state", () => {
    const stateBytes = new Uint8Array(0);

    const token = packStateToken(stateBytes, CALL_ID, tokenKey, scope({ principal: ANON }));
    const unpacked = unpackStateToken(token, tokenKey, 3600, scope({ principal: ANON }));
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

    const aliceToken = packStateToken(stateBytes, CALL_ID, tokenKey, scope({ principal: "alice" }));

    // Alice can open her own token.
    expect(() => unpackStateToken(aliceToken, tokenKey, 3600, scope({ principal: "alice" }))).not.toThrow();
    // Bob cannot replay Alice's token.
    expect(() => unpackStateToken(aliceToken, tokenKey, 3600, scope({ principal: "bob" }))).toThrow(
      "signature verification failed",
    );
    // Anonymous cannot replay it either.
    expect(() => unpackStateToken(aliceToken, tokenKey, 3600, scope({ principal: ANON }))).toThrow(
      "signature verification failed",
    );
  });

  test("anonymous token cannot be opened by a named principal", () => {
    const token = packStateToken(new Uint8Array([1]), CALL_ID, tokenKey, scope({ principal: ANON }));
    expect(() => unpackStateToken(token, tokenKey, 3600, scope({ principal: "alice" }))).toThrow(
      "signature verification failed",
    );
  });

  test("the cursor is compressed before sealing, and a tiny one stays raw (STATE_REF_V1.md §7)", () => {
    const sc = scope({ principal: ANON });
    const open = (token: string) => openBytes(base64ToBytes(token), tokenKey, { aad: computeAad(sc), version: 6 });
    const repetitive = new TextEncoder().encode('{"init":"'.concat("x".repeat(20000), '"}'));
    const big = packStateToken(repetitive, CALL_ID, tokenKey, sc);
    expect(open(big)[8 + 16]).toBe(0x01);
    expect(base64ToBytes(big).length).toBeLessThan(repetitive.length / 10);
    expect(unpackStateToken(big, tokenKey, 3600, sc).stateBytes).toEqual(repetitive);
    const tiny = packStateToken(new Uint8Array([7]), CALL_ID, tokenKey, sc);
    expect(open(tiny)[8 + 16]).toBe(0x00);
    expect(unpackStateToken(tiny, tokenKey, 3600, sc).stateBytes).toEqual(new Uint8Array([7]));
  });

  test("handles large state", () => {
    const stateBytes = randomBytes(10000);
    const schemaBytes = randomBytes(500);
    const inputSchemaBytes = randomBytes(500);

    const token = packStateToken(stateBytes, CALL_ID, tokenKey, scope({ principal: ANON }));
    const unpacked = unpackStateToken(token, tokenKey, 3600, scope({ principal: ANON }));
    const equal = (x: Uint8Array, y: Uint8Array) => x.length === y.length && x.every((v, i) => v === y[i]);
    expect(equal(unpacked.stateBytes, stateBytes)).toBe(true);

    const callToken = packCall(CALL_ID, schemaBytes, inputSchemaBytes, tokenKey, scope({ principal: ANON }));
    const { call } = unpackCall(callToken, tokenKey, scope({ principal: ANON }), 3600);
    expect(equal(call.schemaBytes, schemaBytes)).toBe(true);
    expect(equal(call.inputSchemaBytes, inputSchemaBytes)).toBe(true);
  });
});
