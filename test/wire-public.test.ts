// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// The public intermediary surface (`vgi_rpc.wire` in Python).

import { describe, expect, it } from "bun:test";
import { binary, deserializeBatch, field, int64, schema as makeSchema, utf8 } from "../src/arrow/index.js";
import { LOG_LEVEL_KEY, LOG_MESSAGE_KEY, PROTOCOL_VERSION_KEY, STATE_KEY } from "../src/constants.js";
import { serializeIpcStream } from "../src/http/common.js";
import {
  buildErrorStream,
  findProtocolVersion,
  findStateToken,
  readRequest,
  readUnaryResult,
  writeRequest,
  writeUnaryResult,
} from "../src/wire/public.js";
import { buildEmptyBatch, buildLogBatch } from "../src/wire/response.js";

const PARAMS_SCHEMA = makeSchema([field("count", int64(), true), field("name", utf8(), true)]);
const ENVELOPE_SCHEMA = makeSchema([field("result", binary(), true)]);

describe("wire.readRequest / writeRequest", () => {
  it("round-trips a request through the framing codec", async () => {
    const body = writeRequest("bind", PARAMS_SCHEMA, { count: 7n, name: "sequence" });
    const parsed = await readRequest(body);
    expect(parsed.methodName).toBe("bind");
    expect(Number(parsed.params.count)).toBe(7);
    expect(parsed.params.name).toBe("sequence");
  });

  it("stamps the protocol version when one is supplied", async () => {
    const stamped = writeRequest("bind", PARAMS_SCHEMA, { count: 1n, name: "x" }, "2.1.0");
    expect(await findProtocolVersion(stamped)).toBe("2.1.0");
  });

  it("omits the protocol version key when none is supplied", async () => {
    const bare = writeRequest("bind", PARAMS_SCHEMA, { count: 1n, name: "x" });
    expect(await findProtocolVersion(bare)).toBeNull();
  });
});

describe("wire.buildErrorStream", () => {
  it("frames an error the client can decode", async () => {
    const err = new TypeError("denied by the proxy");
    const body = buildErrorStream(err, makeSchema([]), "srv1");
    // An error stream carries no protocol version and no state token.
    expect(await findProtocolVersion(body)).toBeNull();
    expect(await findStateToken(body)).toBeNull();
    expect(body.byteLength).toBeGreaterThan(0);
  });

  it("defaults to an empty schema when none is supplied", () => {
    const body = buildErrorStream(new Error("boom"));
    const batch = deserializeBatch(body);
    expect(batch.schema.fields.length).toBe(0);
    expect(batch.metadata?.get(LOG_LEVEL_KEY)).toBe("EXCEPTION");
    expect(batch.metadata?.get(LOG_MESSAGE_KEY)).toContain("boom");
  });
});

describe("wire.findStateToken", () => {
  it("finds the token on a single-stream exchange request", async () => {
    const batch = buildEmptyBatch(makeSchema([]), new Map([[STATE_KEY, "tok-abc"]]));
    const body = serializeIpcStream(makeSchema([]), [batch]);
    expect(await findStateToken(body)).toBe("tok-abc");
  });

  it("finds a token living in a later concatenated stream", async () => {
    // Producer init responses are a header stream followed by the data stream;
    // the token rides on a batch of the second stream.
    const headerSchema = makeSchema([field("h", int64(), true)]);
    const header = serializeIpcStream(headerSchema, [buildEmptyBatch(headerSchema)]);
    const dataSchema = makeSchema([field("n", int64(), true)]);
    const data = serializeIpcStream(dataSchema, [buildEmptyBatch(dataSchema, new Map([[STATE_KEY, "tok-later"]]))]);
    const body = new Uint8Array(header.byteLength + data.byteLength);
    body.set(header, 0);
    body.set(data, header.byteLength);
    expect(await findStateToken(body)).toBe("tok-later");
  });

  it("returns null when absent", async () => {
    const schema = makeSchema([field("n", int64(), true)]);
    const body = serializeIpcStream(schema, [buildEmptyBatch(schema)]);
    expect(await findStateToken(body)).toBeNull();
  });

  it("returns null on an unparseable body rather than throwing", async () => {
    expect(await findStateToken(new Uint8Array([1, 2, 3, 4, 5]))).toBeNull();
  });
});

describe("wire.readUnaryResult / writeUnaryResult", () => {
  it("round-trips the raw result bytes", async () => {
    const payload = new Uint8Array([9, 8, 7, 6]);
    const body = writeUnaryResult(ENVELOPE_SCHEMA, payload);
    const unwrapped = await readUnaryResult(body);
    expect(unwrapped).not.toBeNull();
    expect(Array.from(unwrapped!.resultBytes)).toEqual([9, 8, 7, 6]);
    expect(unwrapped!.envelopeSchema.fields.map((f) => f.name)).toEqual(["result"]);
  });

  it("skips leading log batches", async () => {
    const payload = new Uint8Array([42]);
    const log = buildLogBatch(ENVELOPE_SCHEMA, "INFO", "warming up");
    expect(log.metadata?.get(LOG_LEVEL_KEY)).toBe("INFO");
    const dataBody = writeUnaryResult(ENVELOPE_SCHEMA, payload);
    // Re-frame as one stream: [log batch, data batch].
    const dataBatch = (await import("../src/arrow/index.js")).deserializeBatch(dataBody);
    const body = serializeIpcStream(ENVELOPE_SCHEMA, [log, dataBatch]);
    const unwrapped = await readUnaryResult(body);
    expect(unwrapped).not.toBeNull();
    expect(Array.from(unwrapped!.resultBytes)).toEqual([42]);
  });

  it("returns null for a stream with no `result` column", async () => {
    const schema = makeSchema([field("other", int64(), true)]);
    const body = serializeIpcStream(schema, [buildEmptyBatch(schema)]);
    expect(await readUnaryResult(body)).toBeNull();
  });
});

describe("wire constants stay aligned with the codec", () => {
  it("uses the canonical metadata keys", () => {
    expect(STATE_KEY).toBe("vgi_rpc.stream_state#b64");
    expect(PROTOCOL_VERSION_KEY).toBe("vgi_rpc.protocol_version");
  });
});
