// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * The HTTP client's streaming and raw paths work on both Arrow backends.
 *
 * The unary path builds its request through the `#vgi-rpc-arrow` facade, so
 * each backend constructs its own batch type. The streaming client did not:
 * `HttpStreamSession.exchange`, the continuation tick, the cancel signal,
 * `rawInputBatch` and the raw call paths built arrow-js `RecordBatch`es
 * directly and handed them to the facade's writer. Under flechette -- the
 * backend workerd and `--conditions=flechette` select -- that writer cannot
 * read an arrow-js batch (`undefined is not an object (evaluating
 * 'columns[0]')`), and wrapping a flechette batch in an arrow-js
 * `RecordBatch` throws (`RecordBatch constructor expects a [Schema, Data]
 * pair`). So an in-process client on flechette could open a stream but not
 * take its second turn.
 *
 * The child (test/fixtures/stream-client-child.ts) serves a protocol over HTTP
 * in-process and drives exchange (rows, a facade batch, an arrow-js batch,
 * zero rows, raw), producer continuations (iteration, a tick with metadata),
 * a cancel and a raw unary call. It runs once per backend; both must give the
 * same answers.
 */

import { describe, expect, test } from "bun:test";
import { join } from "node:path";

const CHILD = join(import.meta.dir, "fixtures", "stream-client-child.ts");

function run(conditions: string[]): any {
  const proc = Bun.spawnSync(["bun", ...conditions, "run", CHILD], {
    cwd: join(import.meta.dir, ".."),
    stdout: "pipe",
    stderr: "pipe",
    timeout: 20_000,
  });
  const out = proc.stdout.toString().trim();
  if (proc.exitCode !== 0) {
    throw new Error(`child (${conditions.join(" ") || "default"}) exited ${proc.exitCode}: ${proc.stderr.toString()}`);
  }
  return JSON.parse(out.split("\n").pop()!);
}

const EXPECTED = {
  rows: [
    [3, ""],
    [4, ""],
  ],
  facadeBatch: [[10, ""]],
  arrowBatch: [[14, ""]],
  zeroRows: [],
  rawExchange: [20, "raw"],
  produced: [0, 1, 2],
  tickWithMetadata: [[1, "tick"]],
  cancelled: true,
  rawCall: "echo:hi",
};

describe("the HTTP stream client on both backends", () => {
  for (const [name, conditions] of [
    ["arrow-js", []],
    ["flechette", ["--conditions=flechette"]],
  ] as const) {
    test(`${name}: exchange, continuations, cancel and raw calls against a strict server`, () => {
      expect(run([...conditions])).toEqual({ backend: name, ...EXPECTED });
    }, 30_000);
  }
});
