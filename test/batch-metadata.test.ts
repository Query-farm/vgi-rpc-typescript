// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Per-batch `custom_metadata` must survive serialization on BOTH Arrow
// backends, and — critically — through `createIncrementalEncoder`, the encoder
// the lockstep stdio/launcher transport uses.
//
// Regression guard. flechette only puts batch metadata on the wire when the
// `batchMetadata` encode option is passed; without it `serializeBatch` dropped
// the map silently. Because `createIncrementalEncoder` builds every stdio frame
// by slicing `serializeBatch` output, everything riding batch metadata —
// result-cache directives, state tokens, row provenance — vanished on stdio
// while the one-shot HTTP path kept working. That asymmetry made it look like a
// transport bug rather than an encoder one.

import { describe, expect, it } from "bun:test";
import {
  batchFromColumns,
  createIncrementalEncoder,
  deserializeBatch,
  field,
  int64,
  schema,
  serializeBatch,
  withBatchMetadata,
} from "../src/arrow/index.js";

const SCHEMA = schema([field("n", int64())]);

function stamped() {
  const batch = batchFromColumns(SCHEMA, { n: [1n, 2n, 3n] });
  return withBatchMetadata(batch, new Map([["vgi.cache.ttl", "300"]]));
}

describe("per-batch custom_metadata", () => {
  it("survives a one-shot serializeBatch round trip", () => {
    const round = deserializeBatch(serializeBatch(stamped()));
    expect(round.metadata?.get("vgi.cache.ttl")).toBe("300");
  });

  it("survives the incremental encoder used by stdio / launcher", () => {
    // Mirror the lockstep framing: writeBatch() emits [schema][dict…][batch]
    // on the first call, then finish() appends the EOS marker.
    const enc = createIncrementalEncoder(SCHEMA);
    const parts: Uint8Array[] = [enc.start(), enc.writeBatch(stamped()), enc.finish()];
    const total = parts.reduce((n, p) => n + p.length, 0);
    const stream = new Uint8Array(total);
    let at = 0;
    for (const p of parts) {
      stream.set(p, at);
      at += p.length;
    }

    const round = deserializeBatch(stream);
    expect(round.numRows).toBe(3);
    expect(round.metadata?.get("vgi.cache.ttl")).toBe("300");
  });

  it("leaves a batch without metadata unchanged", () => {
    const round = deserializeBatch(serializeBatch(batchFromColumns(SCHEMA, { n: [7n] })));
    expect(round.numRows).toBe(1);
    expect(round.metadata?.get("vgi.cache.ttl")).toBeUndefined();
  });
});
