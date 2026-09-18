// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Both Arrow backends put the DECLARED schema on the wire.
 *
 * A peer checks the schema it receives against the one it declared, exactly:
 * a vgi-rpc server refuses a request whose parameter schema differs in
 * nullability ("Parameter schema mismatch ... expected nullable=false, got
 * nullable=true"), and the DuckDB extension refuses a response batch the same
 * way. The flechette backend lost the declared schema in several places:
 *
 * - `singleRowBatchWithMetadata` and `emptyBatchWithMetadata` built their
 *   batches with flechette's `tableFromColumns`, which makes every field
 *   nullable with no metadata, drops the schema's metadata, and orders fields
 *   by object-key order (integer-like names first). The first of those is
 *   every request `buildRequestIpc` sends, so a flechette client could not get
 *   past its first `describe` against a strict server.
 * - `serializeBatches` and the incremental encoder ignored the schema they were
 *   given and wrote the first batch's -- arrow-js writes the declared one.
 * - Type normalization folded LargeUtf8 / LargeBinary into Utf8 / Binary.
 *
 * The first block compares the two backends' bytes for the same declared
 * schema directly (both implementations imported side by side). The second
 * runs whole processes, because `#vgi-rpc-arrow` picks one backend per
 * process: the bytes of the request `reflectionRequest("describe", ...)`
 * builds, and a flechette HTTP client describing and then calling a strict
 * in-process server.
 */

import { describe, expect, test } from "bun:test";
import { join } from "node:path";
import { type Schema, tableFromIPC } from "@query-farm/apache-arrow";
import {
  columnFromArray as f_columnFromArray,
  int64 as f_int64,
  tableFromColumns as f_tableFromColumns,
  utf8 as f_utf8,
} from "@query-farm/flechette";
import * as arrowjs from "../src/arrow/impl-arrowjs/index.js";
import * as flechette from "../src/arrow/impl-flechette/index.js";
import { REFLECTION_DESCRIBE_PARAMS } from "../src/reflection.js";

type Impl = typeof arrowjs;

/** A schema, as a peer that decodes it sees it (read back with arrow-js). */
function wireSchema(bytes: Uint8Array): unknown {
  return describeSchema(tableFromIPC(bytes).schema);
}

function describeSchema(schema: Schema): unknown {
  const fieldOf = (f: any): unknown => ({
    name: f.name,
    type: String(f.type),
    nullable: f.nullable,
    metadata: [...(f.metadata ?? new Map()).entries()],
    children: (f.type.children ?? []).map(fieldOf),
  });
  return { fields: schema.fields.map(fieldOf), metadata: [...(schema.metadata ?? new Map()).entries()] };
}

/** The same declared schema, built with one backend's own factories. */
function declared(impl: Impl) {
  return impl.schema(
    [
      impl.field("protocol", impl.utf8(), false, new Map([["doc", "the protocol to describe"]])),
      // Integer-like names: object-key order would move these to the front.
      impl.field("1", impl.int64(), true),
      impl.field("0", impl.float64(), false),
      impl.field("big", impl.largeUtf8(), false),
      impl.field("blob", impl.largeBinary(), true),
      impl.field("tags", impl.list(impl.field("item", impl.utf8(), false)), false),
    ],
    new Map([["vgi_rpc.schema_note", "declared"]]),
  );
}

const VALUES = {
  protocol: "demo.v1",
  "1": 7n,
  "0": 1.5,
  big: "large string",
  blob: new Uint8Array([1, 2, 3]),
  tags: ["a", "b"],
};
const COLUMNS = {
  protocol: ["x", "y"],
  "1": [1n, null],
  "0": [0.5, 2.5],
  big: ["p", "q"],
  blob: [null, new Uint8Array([9])],
  tags: [["t"], []],
};
const MD = new Map([["vgi_rpc.method", "describe"]]);
const LOG_MD = new Map([
  ["vgi_rpc.log_level", "INFO"],
  ["vgi_rpc.log_message", "hello"],
]);

function concat(parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}

/** Every writer path, fed batches from every builder, on one backend. */
function writes(impl: Impl): Record<string, Uint8Array> {
  const s = declared(impl);
  const encoder = impl.createIncrementalEncoder(s);
  return {
    // What buildRequestIpc sends: one row, request metadata.
    request: impl.serializeBatches(s, [impl.singleRowBatchWithMetadata(s, VALUES, MD)]),
    // A response stream whose first batch is a log batch.
    "log-then-data": impl.serializeBatches(s, [
      impl.emptyBatchWithMetadata(s, LOG_MD),
      impl.batchFromColumns(s, COLUMNS),
    ]),
    // A zero-row result carrying only metadata (a state-token carrier).
    "metadata-only": impl.serializeBatch(impl.emptyBatchWithMetadata(s, MD)),
    // The stdio/launcher framing, log batch first.
    incremental: concat([
      encoder.start(),
      encoder.writeBatch(impl.emptyBatchWithMetadata(s, LOG_MD)),
      encoder.writeBatch(impl.batchFromColumns(s, COLUMNS)),
      encoder.finish(),
    ]),
    "schema-only": impl.serializeSchema(s),
  };
}

describe("both backends write the declared schema", () => {
  const expected = describeSchema(declared(arrowjs) as unknown as Schema);
  const fromArrowjs = writes(arrowjs);
  const fromFlechette = writes(flechette);

  for (const path of Object.keys(fromArrowjs)) {
    test(path, () => {
      expect(wireSchema(fromArrowjs[path])).toEqual(expected);
      expect(wireSchema(fromFlechette[path])).toEqual(expected);
    });
  }

  test("the data rides unchanged under the declared schema", () => {
    for (const path of ["request", "log-then-data", "incremental"]) {
      const a = tableFromIPC(fromArrowjs[path])
        .toArray()
        .map((r) => JSON.stringify(r.toJSON(), jsonable));
      const f = tableFromIPC(fromFlechette[path])
        .toArray()
        .map((r) => JSON.stringify(r.toJSON(), jsonable));
      expect(f).toEqual(a);
      expect(f.length).toBeGreaterThan(0);
    }
  });

  test("a writer re-homes a batch built outside the facade onto the declared schema", () => {
    // flechette's own tableFromColumns: every field nullable, no metadata.
    const s = flechette.schema([
      flechette.field("n", flechette.int64(), false),
      flechette.field("s", flechette.utf8(), false),
    ]);
    const foreign = f_tableFromColumns({
      n: f_columnFromArray([1n, 2n], f_int64()),
      s: f_columnFromArray(["a", "b"], f_utf8()),
    }) as any;
    const bytes = flechette.serializeBatches(s, [foreign]);
    const fields = (wireSchema(bytes) as any).fields;
    expect(fields.map((f: any) => [f.name, f.nullable])).toEqual([
      ["n", false],
      ["s", false],
    ]);
    expect(tableFromIPC(bytes).getChild("s")?.toArray()).toEqual(["a", "b"]);
  });
});

function jsonable(_key: string, value: unknown): unknown {
  if (typeof value === "bigint") return `${value}n`;
  if (value instanceof Uint8Array) return [...value];
  if (value && typeof (value as any).toArray === "function") return [...(value as any).toArray()];
  return value;
}

const CHILD = join(import.meta.dir, "fixtures", "declared-schema-child.ts");

/** Run the child under one backend; returns its JSON line. */
function child(mode: "request" | "http", conditions: string[]): any {
  const proc = Bun.spawnSync(["bun", ...conditions, "run", CHILD, mode], {
    cwd: join(import.meta.dir, ".."),
    stdout: "pipe",
    stderr: "pipe",
    timeout: 20_000,
  });
  const out = proc.stdout.toString().trim();
  if (proc.exitCode !== 0) {
    throw new Error(
      `child (${conditions.join(" ") || "default"}) ${mode} exited ${proc.exitCode}: ${proc.stderr.toString()}`,
    );
  }
  return JSON.parse(out.split("\n").pop()!);
}

describe("per process: flechette against arrow-js", () => {
  test("reflectionRequest('describe', ...) declares the same schema on both backends", () => {
    const arrow = child("request", []);
    const flech = child("request", ["--conditions=flechette"]);
    expect(arrow.backend).not.toBe(flech.backend);
    const a = Buffer.from(arrow.bytes, "base64");
    const f = Buffer.from(flech.bytes, "base64");
    expect(wireSchema(f)).toEqual(wireSchema(a));
    // ...which is the schema the server holds the request to: one non-null
    // utf8 `protocol` (read off the declaration, so this holds on whichever
    // backend runs the test).
    const declaredParams = REFLECTION_DESCRIBE_PARAMS.fields.map((field) => [field.name, field.nullable]);
    expect(declaredParams).toEqual([["protocol", false]]);
    expect((wireSchema(f) as any).fields.map((field: any) => [field.name, field.type, field.nullable])).toEqual([
      ["protocol", "Utf8", false],
    ]);
    expect(tableFromIPC(f).getChild("protocol")?.get(0)).toBe("demo.v1");
  }, 30_000);

  test("a flechette HTTP client describes, then calls, a strict server", () => {
    const result = child("http", ["--conditions=flechette"]);
    expect(result).toEqual({
      backend: "flechette",
      protocol: "demo.v1",
      methods: ["greet"],
      greeting: "Hello, flechette!",
    });
  }, 30_000);
});
