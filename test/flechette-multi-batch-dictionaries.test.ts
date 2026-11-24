// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * `serializeBatches` over batches that each carry their own dictionary.
 *
 * The HTTP stream path writes one IPC stream per reply: zero or more zero-row
 * metadata batches (logs, state tokens) followed by result batches, each built
 * on its own — so a dictionary column's values differ batch to batch. The
 * flechette backend crashed ("Cannot convert undefined to a BigInt") when a
 * zero-row metadata batch led the stream, and silently decoded later batches
 * against the first batch's dictionary (['a'], ['b'] read back as 'a', 'a').
 * Both were in the flechette fork's `tablesToIPC`; arrow-js's writer was always
 * right. Both backends run here, decoded by arrow-js as the independent reader.
 */

import { describe, expect, test } from "bun:test";
import { tableFromIPC } from "@query-farm/apache-arrow";
import * as arrowjs from "../src/arrow/impl-arrowjs/index.js";
import * as flechette from "../src/arrow/impl-flechette/index.js";

type Impl = typeof arrowjs;

function suite(name: string, impl: Impl) {
  const SCHEMA = impl.schema([
    impl.field("n", impl.int64(), true),
    impl.field("kind", impl.dictionary(impl.int8(), impl.utf8()), true),
  ]);
  const batch = (n: bigint[], kind: (string | null)[]) => impl.batchFromColumns(SCHEMA, { n, kind });
  const logBatch = () => impl.emptyBatchWithMetadata(SCHEMA, new Map([["vgi_rpc.log_level", "WARN"]]));

  /** Decode with arrow-js into [n, kind] rows. */
  const rows = (bytes: Uint8Array): [number, string | null][] => {
    const t = tableFromIPC(bytes);
    const n = t.getChild("n")!;
    const kind = t.getChild("kind")!;
    return Array.from({ length: t.numRows }, (_, i) => [Number(n.get(i)), kind.get(i) as string | null]);
  };

  describe(`serializeBatches with per-batch dictionaries (${name})`, () => {
    test("a zero-row metadata batch before an all-NULL dictionary batch", () => {
      expect(rows(impl.serializeBatches(SCHEMA, [logBatch(), batch([1n], [null])]))).toEqual([[1, null]]);
    });

    test("a zero-row metadata batch before data", () => {
      expect(rows(impl.serializeBatches(SCHEMA, [logBatch(), batch([1n, 2n], ["a", null])]))).toEqual([
        [1, "a"],
        [2, null],
      ]);
    });

    test("each batch decodes against its own dictionary", () => {
      const bytes = impl.serializeBatches(SCHEMA, [
        batch([1n], ["a"]),
        batch([2n, 3n], ["b", "c"]),
        batch([4n], [null]),
      ]);
      expect(rows(bytes)).toEqual([
        [1, "a"],
        [2, "b"],
        [3, "c"],
        [4, null],
      ]);
    });
  });
}

suite("arrow-js", arrowjs);
suite("flechette", flechette as unknown as Impl);
