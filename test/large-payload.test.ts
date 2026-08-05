// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Neither transport clamp can be judged by the platform CI runs on. Linux
// caps a single pipe or socket transfer at 0x7ffff000 and returns a short
// count that the existing write loop absorbs, so a Linux runner passes
// whether or not the per-call sizes are bounded. macOS does not: `read(n)`
// above 1 GiB throws `ERR_OUT_OF_RANGE` (Node and Bun alike), and `send(2)`
// above 2 GiB fails with `EINVAL`. The conformance suite's `large_payload`
// group catches both, but only on a Mac — the >2 GiB test runs everywhere
// now, yet only a Mac reaches the sizes where the syscalls actually refuse.
//
// So these assert the invariant itself — no single call is ever handed more
// than the clamp — on payloads small enough to run anywhere in milliseconds.

import { describe, expect, it } from "bun:test";
import { batchFromColumns, binary, field, schema, serializeBatches } from "../src/arrow/index.js";
import { MAX_ENCODABLE_BYTES, requireEncodable } from "../src/arrow/limits.js";
import { clampReads, MAX_READ_CHUNK } from "../src/wire/reader.js";
import { IpcStreamWriter, MAX_STREAM_CHUNK } from "../src/wire/writer.js";

describe("read clamp", () => {
  it("never asks a Node readable for more than MAX_READ_CHUNK", () => {
    const asked: (number | undefined)[] = [];
    const stream = {
      readable: true,
      read(n?: number) {
        asked.push(n);
        return null;
      },
      pipe() {},
    };
    const wrapped = clampReads(stream);
    // arrow-js's Node adapter asks for a whole IPC message body in one call,
    // so a >2 GiB Arrow body becomes a >2 GiB request.
    wrapped.read(2 ** 31 + 1);
    wrapped.read(1024);
    wrapped.read();
    expect(asked).toEqual([MAX_READ_CHUNK, 1024, undefined]);
  });

  it("passes everything else through to the underlying stream", () => {
    const events: string[] = [];
    const stream = {
      readable: true,
      bytesRead: 7,
      read() {
        return null;
      },
      pipe() {},
      once(event: string) {
        events.push(event);
      },
    };
    const wrapped = clampReads(stream);
    wrapped.once("end");
    expect(events).toEqual(["end"]);
    expect(wrapped.readable).toBe(true);
    expect(wrapped.bytesRead).toBe(7);
    // The trio arrow-js sniffs to choose its Node adapter has to survive the
    // wrapper, or the stream silently takes a different code path.
    expect(typeof wrapped.read).toBe("function");
    expect(typeof wrapped.pipe).toBe("function");
  });
});

describe("socket write clamp", () => {
  it("hands the socket no single write larger than MAX_STREAM_CHUNK", async () => {
    const chunks: Uint8Array[] = [];
    // Anything carrying a `writable` property is treated as a Socket by
    // IpcStreamWriter rather than as a plain byte sink.
    const socket = {
      destroyed: false,
      writableEnded: false,
      writable: true,
      write(data: Uint8Array) {
        chunks.push(data.slice());
        return true;
      },
      once() {},
      off() {},
    };

    const payload = new Uint8Array(3 * MAX_STREAM_CHUNK + 17);
    for (let i = 0; i < payload.length; i++) payload[i] = i & 0xff;
    const s = schema([field("v", binary())]);
    const batch = batchFromColumns(s, { v: [payload] });

    const writer = new IpcStreamWriter(socket as never);
    await writer.writeStream(s, [batch]);

    expect(chunks.length).toBeGreaterThan(3);
    for (const chunk of chunks) {
      expect(chunk.length).toBeLessThanOrEqual(MAX_STREAM_CHUNK);
    }
    // Same bytes, only split differently: a chunking bug that dropped or
    // reordered a piece would show up here rather than as a peer deadlock.
    const expected = serializeBatches(s, [batch]);
    const joined = new Uint8Array(chunks.reduce((n, c) => n + c.length, 0));
    let offset = 0;
    for (const chunk of chunks) {
      joined.set(chunk, offset);
      offset += chunk.length;
    }
    expect(joined).toEqual(expected);
  });
});

describe("encodable ceiling", () => {
  it("accepts the largest payload the Arrow encoders can represent", () => {
    // 2 GiB - 8 is the last size whose 8-byte-aligned length still fits an
    // int32. Refusing it would give up a payload that demonstrably works.
    expect(() => requireEncodable(fakeBuffers(MAX_ENCODABLE_BYTES), "value")).not.toThrow();
  });

  it("refuses one byte past it, naming the field and the real size", () => {
    expect(() => requireEncodable(new Uint8Array(0), "value")).not.toThrow();
    try {
      requireEncodable(fakeBuffers(MAX_ENCODABLE_BYTES + 1), "result");
      throw new Error("expected a refusal");
    } catch (e) {
      const msg = (e as Error).message;
      expect(msg).toContain("result");
      expect(msg).toContain(String(MAX_ENCODABLE_BYTES + 1));
      // The limit belongs to the JS Arrow encoders, and the message has to
      // say so — a reader who thinks it is a protocol limit goes looking in
      // the wrong repository.
      expect(msg).toContain("not vgi-rpc");
    }
  });

  it("measures an Arrow Data by its largest buffer, not their sum", () => {
    // An echo handler returns the decoded `Data` untouched, so the guard sees
    // a buffer container rather than a JS value. Summing would refuse a
    // payload of exactly the ceiling on account of its own validity bytes.
    const atCeiling = {
      buffers: {
        0: { byteLength: 64 },
        1: { byteLength: MAX_ENCODABLE_BYTES },
      },
    };
    expect(() => requireEncodable(atCeiling, "value")).not.toThrow();
  });
});

/** A buffer container of a given size, without allocating one. */
function fakeBuffers(byteLength: number): unknown {
  return { buffers: { 1: { byteLength } } };
}
