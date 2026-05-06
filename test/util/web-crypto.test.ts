// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import {
  constantTimeEqual,
  hmacSha256,
  hmacSha256Verify,
  randomBytes,
  sha256,
  sha256Hex,
} from "../../src/util/web-crypto.js";

describe("web-crypto helpers", () => {
  test("randomBytes returns the requested length", () => {
    const a = randomBytes(32);
    const b = randomBytes(16);
    expect(a).toBeInstanceOf(Uint8Array);
    expect(a.length).toBe(32);
    expect(b.length).toBe(16);
    // Two consecutive draws should differ.
    expect(constantTimeEqual(a, randomBytes(32))).toBe(false);
  });

  test("constantTimeEqual handles equal, mismatched-length, and one-byte diffs", () => {
    const a = new Uint8Array([1, 2, 3, 4]);
    const b = new Uint8Array([1, 2, 3, 4]);
    const c = new Uint8Array([1, 2, 3, 5]);
    const d = new Uint8Array([1, 2, 3]);
    expect(constantTimeEqual(a, b)).toBe(true);
    expect(constantTimeEqual(a, c)).toBe(false);
    expect(constantTimeEqual(a, d)).toBe(false);
  });

  test("hmacSha256 produces a 32-byte tag and is deterministic", async () => {
    const key = new TextEncoder().encode("secret");
    const data = new TextEncoder().encode("hello world");
    const tag1 = await hmacSha256(key, data);
    const tag2 = await hmacSha256(key, data);
    expect(tag1.length).toBe(32);
    expect(constantTimeEqual(tag1, tag2)).toBe(true);
  });

  test("hmacSha256Verify accepts the right tag and rejects a tampered one", async () => {
    const key = new TextEncoder().encode("secret");
    const data = new TextEncoder().encode("hello world");
    const tag = await hmacSha256(key, data);

    expect(await hmacSha256Verify(key, data, tag)).toBe(true);

    const tampered = new Uint8Array(tag);
    tampered[0] ^= 0xff;
    expect(await hmacSha256Verify(key, data, tampered)).toBe(false);

    const wrongKey = new TextEncoder().encode("other");
    expect(await hmacSha256Verify(wrongKey, data, tag)).toBe(false);
  });

  test("sha256 / sha256Hex match RFC 6234 test vector for the empty string", async () => {
    // Empty string SHA-256 = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    const expected = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    const empty = new Uint8Array(0);
    expect(await sha256Hex(empty)).toBe(expected);
    const bytes = await sha256(empty);
    expect(bytes.length).toBe(32);
  });

  test("sha256Hex matches RFC 6234 test vector for 'abc'", async () => {
    // SHA-256("abc") = ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
    const expected = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    const data = new TextEncoder().encode("abc");
    expect(await sha256Hex(data)).toBe(expected);
  });
});
