// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Language-local coverage of STATE_REF_V1.md (protocol 3.4): what the shared
 * conformance suite cannot observe over the wire -- the codec tag on the seal
 * path, a stored ref re-serializing to identical bytes without a second
 * `putObject`, the tag's own expiry, the peer-evidence binding in the
 * argument-reference AAD -- plus the object cache, the turn budget and fix 1.
 */

import { describe, expect, test } from "bun:test";
import { AuthContext } from "../src/auth.js";
import { ObjectMissingError, type ObjectStore } from "../src/external.js";
import { createHttpHandler, resolveMaxUploadBytes } from "../src/http/index.js";
import { jsonStateSerializer } from "../src/http/types.js";
import { Protocol } from "../src/index.js";
import { ObjectCache } from "../src/object-cache.js";
import { str } from "../src/schema.js";
import {
  ArgumentsRefIntegrityError,
  ArgumentsRefMissingError,
  CursorTooLargeError,
  encodeStateRefValue,
  InvalidArgumentsRefError,
  InvalidStateRefError,
  mintArgumentRef,
  resolveArgumentRef,
  resolveEncodedStateRef,
  StateRef,
  StateRefMissingError,
  StateRefMisuseError,
  StateRefRuntime,
  StateRefScope,
  StateRefTooLargeError,
  StorageUnavailableError,
  serializeWithStateRefs,
  stateRefEncoder,
  stateRefInlineBytesFromEnv,
  turnBudget,
} from "../src/state-ref.js";
import { CODEC_RAW, CODEC_ZSTD, packToken, TokenCodecError, unpackToken } from "../src/token-codec.js";

const KEY = new Uint8Array(32).fill(9);
const PROTO = "test.StateRef.v1";

/** In-memory object store with counters. */
class MemoryStore implements ObjectStore {
  readonly objects = new Map<string, Uint8Array>();
  puts = 0;
  gets = 0;
  failGets = false;
  async putObject(data: Uint8Array): Promise<string> {
    this.puts += 1;
    const id = `obj-${this.puts}-${crypto.randomUUID()}`;
    this.objects.set(id, new Uint8Array(data));
    return id;
  }
  async get(objectId: string): Promise<Uint8Array> {
    this.gets += 1;
    if (this.failGets) throw new Error("HTTP 503");
    const data = this.objects.get(objectId);
    if (!data) throw new ObjectMissingError();
    return new Uint8Array(data);
  }
}

function randomPayload(n: number): Uint8Array {
  const out = new Uint8Array(n);
  for (let i = 0; i < n; i += 65536) crypto.getRandomValues(out.subarray(i, Math.min(n, i + 65536)));
  return out;
}

function runtime(store: MemoryStore | null, over: Partial<ConstructorParameters<typeof StateRefRuntime>[0]> = {}) {
  return new StateRefRuntime({
    tokenKey: KEY,
    storage: () => store,
    cache: new ObjectCache(1 << 24),
    ...over,
  });
}

const alice = new AuthContext("test", true, "alice");
const bob = new AuthContext("test", true, "bob");
const callA = new Uint8Array(16).fill(1);
const callB = new Uint8Array(16).fill(2);

describe("token codec", () => {
  test("compresses when smaller, stays raw otherwise, refuses unknown codecs", () => {
    const big = new TextEncoder().encode("abc".repeat(5000));
    const packed = packToken(big);
    expect(packed[0]).toBe(CODEC_ZSTD);
    expect(packed.length).toBeLessThan(big.length / 10);
    expect(unpackToken(packed)).toEqual(big);
    const tiny = packToken(new Uint8Array([1, 2, 3]));
    expect(Array.from(tiny)).toEqual([CODEC_RAW, 1, 2, 3]);
    expect(() => unpackToken(new Uint8Array([0x7f, 1]))).toThrow(TokenCodecError);
    expect(() => unpackToken(new Uint8Array())).toThrow(TokenCodecError);
  });

  test("bounds the decompressed size by the frame's declared content size", () => {
    const big = new Uint8Array(1 << 20);
    expect(() => unpackToken(packToken(big), 1000)).toThrow("decompression bound");
  });

  test("reads raw DEFLATE (codec 0x02)", async () => {
    const zlib = await import("node:zlib");
    const value = new TextEncoder().encode("deflate ".repeat(100));
    const body = new Uint8Array(zlib.deflateRawSync(value));
    const payload = new Uint8Array(1 + body.length);
    payload[0] = 0x02;
    payload.set(body, 1);
    expect(unpackToken(payload)).toEqual(value);
  });
});

describe("StateRef", () => {
  test("inline when small or without storage; stored once when over the limit", async () => {
    const store = new MemoryStore();
    const rt = runtime(store);
    const enc = stateRefEncoder(rt, { auth: alice, protocol: PROTO, method: "m", callId: callA, callTokenBytes: 0 });
    const small = await encodeStateRefValue(new Uint8Array(100), enc);
    expect(small[0]).toBe(0x01);
    expect(store.puts).toBe(0);
    const large = randomPayload(40000);
    const stored = await encodeStateRefValue(large, enc);
    expect(stored[0]).toBe(0x02);
    expect(store.puts).toBe(1);
    expect(stored.length).toBeLessThan(200);
    const back = await resolveEncodedStateRef(stored, rt, { auth: alice, protocol: PROTO, callId: callA });
    expect(back).toEqual(large);
    // No storage: an over-limit value stays inline.
    const none = runtime(null);
    const encNone = stateRefEncoder(none, {
      auth: alice,
      protocol: PROTO,
      method: "m",
      callId: callA,
      callTokenBytes: 0,
    });
    expect((await encodeStateRefValue(large, encNone))[0]).toBe(0x01);
  });

  test("a stored ref re-serializes to identical bytes without a second putObject", async () => {
    const store = new MemoryStore();
    const rt = runtime(store);
    const scope = new StateRefScope({ runtime: rt, auth: alice, protocol: PROTO, method: "m", init: true });
    const state = { value: scope.stateRef(randomPayload(40000)), n: 1 };
    const enc = stateRefEncoder(rt, { auth: alice, protocol: PROTO, method: "m", callId: callA, callTokenBytes: 0 });
    const serialize = () => jsonStateSerializer.serialize(state);
    const first = await serializeWithStateRefs(serialize, enc);
    const second = await serializeWithStateRefs(serialize, enc);
    expect(store.puts).toBe(1);
    expect(second).toEqual(first);
    expect(state.value.form).toBe("stored");
    // The JSON reviver rebuilds a ref holding the same encoded bytes.
    const revived = jsonStateSerializer.deserialize(first);
    expect(StateRef.isStateRef(revived.value)).toBe(true);
    expect(revived.value.encoded).toEqual(state.value.encoded);
    const turn = new StateRefScope({ runtime: rt, auth: alice, protocol: PROTO, method: "m", callId: callA });
    expect((await turn.resolve(revived.value)).byteLength).toBe(40000);
  });

  test("an unserialized ref is not stored; serialization outside a mint renders inline uncached", async () => {
    const store = new MemoryStore();
    const rt = runtime(store);
    const scope = new StateRefScope({ runtime: rt, auth: alice, protocol: PROTO, method: "m", init: true });
    scope.stateRef(randomPayload(40000)); // never placed in the state
    const kept = scope.stateRef(randomPayload(40000));
    const enc = stateRefEncoder(rt, { auth: alice, protocol: PROTO, method: "m", callId: callA, callTokenBytes: 0 });
    jsonStateSerializer.serialize({ kept }); // outside a mint
    expect(kept.form).toBe("memory");
    await serializeWithStateRefs(() => jsonStateSerializer.serialize({ kept }), enc);
    expect(store.puts).toBe(1);
  });

  test("memoizes identical and byte-equal values within one init", () => {
    const scope = new StateRefScope({ init: true, protocol: PROTO, method: "m" });
    const bytes = new Uint8Array([1, 2, 3]);
    expect(scope.stateRef(bytes)).toBe(scope.stateRef(bytes));
    expect(scope.stateRef(new Uint8Array([1, 2, 3]))).toBe(scope.stateRef(bytes));
    expect(scope.stateRef(new Uint8Array([1, 2, 4]))).not.toBe(scope.stateRef(bytes));
  });

  test("stateRef() outside an init handler is state_ref_misuse", () => {
    const scope = new StateRefScope({ protocol: PROTO, method: "scan" });
    expect(() => scope.stateRef(new Uint8Array([1]))).toThrow(StateRefMisuseError);
    expect(() => scope.stateRef(new Uint8Array([1]))).toThrow(
      `state_ref() may only be called while handling a stream's init request (${PROTO}.scan)`,
    );
  });

  test("a typed resolve returns the original record while in memory", async () => {
    const record = { toBytes: () => new Uint8Array([5, 6]) };
    const scope = new StateRefScope({ init: true });
    const ref = scope.stateRef(record);
    expect(await scope.resolve(ref, () => "decoded")).toBe(record);
    const encoded = StateRef.fromBytes(ref.toBytes());
    expect(await scope.resolve(encoded, (b) => Array.from(b))).toEqual([5, 6]);
  });

  test("tampered, other-stream, other-principal and unknown-form refs are invalid_state_ref", async () => {
    const store = new MemoryStore();
    const rt = runtime(store);
    const enc = stateRefEncoder(rt, { auth: alice, protocol: PROTO, method: "m", callId: callA, callTokenBytes: 0 });
    const stored = await encodeStateRefValue(randomPayload(40000), enc);
    const tampered = new Uint8Array(stored);
    tampered[tampered.length >> 1] ^= 1;
    for (const [bytes, scope] of [
      [tampered, { auth: alice, protocol: PROTO, callId: callA }],
      [stored, { auth: alice, protocol: PROTO, callId: callB }],
      [stored, { auth: bob, protocol: PROTO, callId: callA }],
      [stored, { auth: alice, protocol: "other.v1", callId: callA }],
      [new Uint8Array([0x07, ...stored.subarray(1)]), { auth: alice, protocol: PROTO, callId: callA }],
      [new Uint8Array([0x01, 0x7f, 1, 2]), { auth: alice, protocol: PROTO, callId: callA }],
    ] as const) {
      await expect(resolveEncodedStateRef(bytes, rt, scope)).rejects.toBeInstanceOf(InvalidStateRefError);
    }
    expect(store.gets).toBe(0);
  });

  test("an expired tag is refused before any get", async () => {
    const store = new MemoryStore();
    let now = 1_000_000;
    const rt = runtime(store, { tokenTtl: 60, clock: () => now });
    const enc = stateRefEncoder(rt, { auth: alice, protocol: PROTO, method: "m", callId: callA, callTokenBytes: 0 });
    const stored = await encodeStateRefValue(randomPayload(40000), enc);
    now += 61;
    await expect(resolveEncodedStateRef(stored, rt, { auth: alice, protocol: PROTO, callId: callA })).rejects.toThrow(
      "StateRef expired",
    );
    expect(store.gets).toBe(0);
  });

  test("missing, transient and oversized objects map to their kinds", async () => {
    const store = new MemoryStore();
    const rt = runtime(store);
    const enc = stateRefEncoder(rt, { auth: alice, protocol: PROTO, method: "m", callId: callA, callTokenBytes: 0 });
    const stored = await encodeStateRefValue(randomPayload(40000), enc);
    const scope = { auth: alice, protocol: PROTO, callId: callA };
    store.failGets = true;
    await expect(resolveEncodedStateRef(stored, rt, scope)).rejects.toBeInstanceOf(StorageUnavailableError);
    store.failGets = false;
    store.objects.clear();
    await expect(resolveEncodedStateRef(stored, rt, scope)).rejects.toBeInstanceOf(StateRefMissingError);
    const tight = runtime(store, { maxFetchBytes: 1000 });
    const encTight = stateRefEncoder(tight, {
      auth: alice,
      protocol: PROTO,
      method: "m",
      callId: callA,
      callTokenBytes: 0,
    });
    await expect(encodeStateRefValue(randomPayload(40000), encTight)).rejects.toBeInstanceOf(StateRefTooLargeError);
  });

  test("off HTTP a ref stays in memory and resolves without storage", async () => {
    const scope = new StateRefScope({ init: true });
    const ref = scope.stateRef(new Uint8Array([9, 9]));
    expect(ref.form).toBe("memory");
    expect(await new StateRefScope().resolve(ref)).toEqual(new Uint8Array([9, 9]));
    await expect(
      new StateRefScope().resolveArgumentRef(new Uint8Array(60), { size: 1, sha256: "a".repeat(64) }),
    ).rejects.toBeInstanceOf(InvalidArgumentsRefError);
  });
});

describe("argument references", () => {
  async function upload(store: MemoryStore, data: Uint8Array) {
    const id = await store.putObject(data);
    const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", data));
    const sha256 = Array.from(digest, (b) => b.toString(16).padStart(2, "0")).join("");
    return { id, sha256 };
  }

  test("resolve for the granted principal; refused for others, wrong claims, or a missing object", async () => {
    const store = new MemoryStore();
    const cache = new ObjectCache(1 << 20);
    const data = randomPayload(5000);
    const { id, sha256 } = await upload(store, data);
    const ref = mintArgumentRef(KEY, alice, { size: data.length, sha256, objectId: id });
    const opts = { tokenKey: KEY, storage: store, maxFetchBytes: 1 << 20, cache };
    expect(await resolveArgumentRef(ref, { ...opts, auth: alice, size: data.length, sha256 })).toEqual(data);
    await expect(resolveArgumentRef(ref, { ...opts, auth: bob, size: data.length, sha256 })).rejects.toBeInstanceOf(
      InvalidArgumentsRefError,
    );
    await expect(
      resolveArgumentRef(ref, { ...opts, auth: alice, size: data.length + 1, sha256 }),
    ).rejects.toBeInstanceOf(InvalidArgumentsRefError);
    store.objects.clear();
    cache.clear();
    await expect(resolveArgumentRef(ref, { ...opts, auth: alice, size: data.length, sha256 })).rejects.toBeInstanceOf(
      ArgumentsRefMissingError,
    );
  });

  test("bytes that differ from the sealed claim are arguments_ref_integrity", async () => {
    const store = new MemoryStore();
    const data = randomPayload(100);
    const { id, sha256 } = await upload(store, data);
    store.objects.set(id, randomPayload(100));
    const ref = mintArgumentRef(KEY, alice, { size: 100, sha256, objectId: id });
    await expect(
      resolveArgumentRef(ref, {
        tokenKey: KEY,
        auth: alice,
        storage: store,
        maxFetchBytes: 1 << 20,
        size: 100,
        sha256,
        cache: new ObjectCache(1 << 20),
      }),
    ).rejects.toBeInstanceOf(ArgumentsRefIntegrityError);
  });

  test("the AAD includes the peer-evidence binding: refs do not cross bindings", async () => {
    const store = new MemoryStore();
    const data = randomPayload(64);
    const { id, sha256 } = await upload(store, data);
    const bound = (binding: string) => new AuthContext("test", true, "alice", { peer_evidence_binding: binding });
    const ref = mintArgumentRef(KEY, bound("digest-a"), { size: 64, sha256, objectId: id });
    const opts = {
      tokenKey: KEY,
      storage: store,
      maxFetchBytes: 1 << 20,
      size: 64,
      sha256,
      cache: new ObjectCache(1 << 20),
    };
    expect(await resolveArgumentRef(ref, { ...opts, auth: bound("digest-a") })).toEqual(data);
    await expect(resolveArgumentRef(ref, { ...opts, auth: bound("digest-b") })).rejects.toBeInstanceOf(
      InvalidArgumentsRefError,
    );
    await expect(resolveArgumentRef(ref, { ...opts, auth: alice })).rejects.toBeInstanceOf(InvalidArgumentsRefError);
  });
});

describe("object cache", () => {
  test("single-flight: concurrent misses share one fetch", async () => {
    const cache = new ObjectCache(1 << 20);
    let fetches = 0;
    const fetch = async () => {
      fetches += 1;
      await new Promise((r) => setTimeout(r, 20));
      return new Uint8Array(10);
    };
    await Promise.all(Array.from({ length: 8 }, () => cache.getOrFetch("x", fetch)));
    expect(fetches).toBe(1);
    expect((await cache.getOrFetch("x", fetch)).hit).toBe(true);
  });

  test("byte bound evicts LRU, oversized values are not retained, errors are not cached", async () => {
    const cache = new ObjectCache(150);
    await cache.getOrFetch("a", async () => new Uint8Array(100));
    await cache.getOrFetch("b", async () => new Uint8Array(100));
    expect(cache.stats().residentBytes).toBe(100);
    expect(cache.contains("a")).toBe(false);
    expect(cache.stats().evictions).toBe(1);
    await cache.getOrFetch("big", async () => new Uint8Array(200));
    expect(cache.contains("big")).toBe(false);
    await expect(cache.getOrFetch("e", async () => Promise.reject(new Error("boom")))).rejects.toThrow("boom");
    expect((await cache.getOrFetch("e", async () => new Uint8Array(1))).hit).toBe(false);
  });

  test("entries fetched over a day ago are misses", async () => {
    const cache = new ObjectCache(1000);
    await cache.getOrFetch("a", async () => new Uint8Array(1));
    expect(cache.ageEntries(86401)).toBe(1);
    expect((await cache.getOrFetch("a", async () => new Uint8Array(1))).hit).toBe(false);
  });
});

describe("turn budget and the cursor rule", () => {
  test("budget formula and inline limit", () => {
    expect(turnBudget(undefined, 100)).toBeNull();
    expect(turnBudget(0, 100)).toBeNull();
    expect(turnBudget(32768, 1000)).toBe(Math.floor(((32768 - 4096) * 100) / 134) - 1000);
    const rt = runtime(null, { maxRequestBytes: 32768 });
    expect(rt.inlineBytes(0)).toBe(Math.floor(turnBudget(32768, 0)! / 4));
    expect(runtime(null).inlineBytes(0)).toBe(16384);
  });

  test("warn returns a cursor_large message; error throws cursor_too_large; unbounded never fires", () => {
    const args = { cursorBytes: 40000, callTokenBytes: 500, protocol: PROTO, method: "m" };
    expect(runtime(null, { maxRequestBytes: 32768 }).checkCursor(args)).toStartWith("cursor_large:");
    expect(() => runtime(null, { maxRequestBytes: 32768, cursorRule: "error" }).checkCursor(args)).toThrow(
      CursorTooLargeError,
    );
    expect(runtime(null, { cursorRule: "error" }).checkCursor(args)).toBeNull();
    expect(
      runtime(null, { maxRequestBytes: 32768, cursorRule: "error" }).checkCursor({ ...args, cursorBytes: 100 }),
    ).toBeNull();
  });

  test("below the 16 KiB floor the rule is disabled (not refused)", () => {
    const rt = runtime(null, { maxRequestBytes: 9000, cursorRule: "error" });
    expect(rt.cursorRuleEnabled).toBe(false);
    expect(rt.checkCursor({ cursorBytes: 1 << 20, callTokenBytes: 0, protocol: PROTO, method: "m" })).toBeNull();
  });

  test("VGI_RPC_STATE_REF_INLINE_BYTES must be a positive integer", () => {
    expect(stateRefInlineBytesFromEnv({})).toBe(16384);
    expect(stateRefInlineBytesFromEnv({ VGI_RPC_STATE_REF_INLINE_BYTES: "4096" })).toBe(4096);
    expect(() => stateRefInlineBytesFromEnv({ VGI_RPC_STATE_REF_INLINE_BYTES: "0" })).toThrow();
    expect(() => stateRefInlineBytesFromEnv({ VGI_RPC_STATE_REF_INLINE_BYTES: "x" })).toThrow();
  });
});

describe("fix 1: the advertised upload cap never exceeds the fetch cap", () => {
  const provider = {
    generateUploadUrl: () => ({ uploadUrl: "https://x/put", downloadUrl: "https://x/get", expiresAt: new Date() }),
  };
  const storage = { upload: async () => "" };

  test("defaults to the fetch cap, clamps to the default cap, refuses only an explicit lower fetch cap", () => {
    expect(resolveMaxUploadBytes(undefined, 5, undefined)).toBe(0);
    expect(resolveMaxUploadBytes(provider, undefined, undefined)).toBe(256 * 1024 * 1024);
    expect(resolveMaxUploadBytes(provider, undefined, { storage, maxFetchBytes: 1000 })).toBe(1000);
    expect(resolveMaxUploadBytes(provider, 500, { storage, maxFetchBytes: 1000 })).toBe(500);
    expect(resolveMaxUploadBytes(provider, 2 ** 40, undefined)).toBe(256 * 1024 * 1024);
    expect(() => resolveMaxUploadBytes(provider, 2000, { storage, maxFetchBytes: 1000 })).toThrow(
      "exceeds the server's inbound fetch cap",
    );
  });

  test("VGI-Max-Upload-Bytes is always advertised alongside upload URLs", async () => {
    const protocol = new Protocol("Fix1").unary("echo", {
      params: { value: str },
      result: { result: str },
      handler: (p) => ({ result: p.value }),
    });
    const handler = createHttpHandler(protocol, { uploadUrlProvider: provider });
    const res = await handler(new Request("http://h/health"));
    expect(res.headers.get("VGI-Upload-URL-Support")).toBe("2");
    expect(res.headers.get("VGI-Max-Upload-Bytes")).toBe(String(256 * 1024 * 1024));
  });
});
