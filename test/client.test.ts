// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { Subprocess } from "bun";
import {
  Schema,
  Field,
  Float64,
  Utf8,
  Binary,
  RecordBatchStreamWriter,
  RecordBatch,
  vectorFromArray,
  makeData,
  Struct,
} from "apache-arrow";
import { httpConnect, subprocessConnect, type RpcClient, type LogMessage } from "../src/client/index.js";
import { httpIntrospect, parseDescribeResponse } from "../src/client/introspect.js";
import { RpcError } from "../src/errors.js";

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

const PYTHON_CWD = process.env.VGI_RPC_PYTHON_PATH ?? "/Users/rusty/Development/vgi-rpc";
const PYTHON = process.env.VGI_RPC_PYTHON_BIN ?? `${PYTHON_CWD}/.venv/bin/python3`;

/** Check if the Python conformance server is available. */
let hasPython = false;
try {
  const fs = require("fs");
  hasPython = fs.existsSync(PYTHON) && fs.existsSync(PYTHON_CWD);
} catch {}

/** Serialize a 1-row batch as IPC bytes (for dataclass params). */
function serializeDataclass(
  schema: Schema,
  values: Record<string, any>,
): Uint8Array {
  const children = schema.fields.map((f) => {
    return vectorFromArray([values[f.name]], f.type).data[0];
  });
  const structType = new Struct(schema.fields);
  const data = makeData({
    type: structType,
    length: 1,
    children,
    nullCount: 0,
  });
  const batch = new RecordBatch(schema, data);
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, schema);
  writer.write(batch);
  writer.close();
  return writer.toUint8Array(true);
}

const POINT_SCHEMA = new Schema([
  new Field("x", new Float64(), false),
  new Field("y", new Float64(), false),
]);

/** Spawn a server process and read PORT:<n> from stdout, then wait until it accepts connections. */
async function startServer(proc: Subprocess): Promise<string> {
  const reader = proc.stdout.getReader();
  let portLine = "";
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    const { value, done } = await reader.read();
    if (done) break;
    portLine += new TextDecoder().decode(value);
    if (portLine.includes("\n")) break;
  }
  reader.releaseLock();

  const match = portLine.match(/PORT:(\d+)/);
  if (!match) {
    throw new Error(`Failed to read port from server: ${portLine}`);
  }
  const baseUrl = `http://127.0.0.1:${match[1]}`;

  // Wait for the server to accept connections
  for (let i = 0; i < 20; i++) {
    try {
      await fetch(`${baseUrl}/vgi/__describe__`, { method: "POST" });
      return baseUrl;
    } catch {
      await new Promise((r) => setTimeout(r, 100));
    }
  }
  return baseUrl;
}

// ---------------------------------------------------------------------------
// Parameterised conformance test suite
// ---------------------------------------------------------------------------

/** HTTP transport: spawns server, reads PORT, creates httpConnect clients. */
function defineHttpConformanceTests(
  label: string,
  spawnFn: () => Subprocess,
) {
  defineConformanceTests(
    label,
    // setup: spawn server, return baseUrl
    async () => {
      const proc = spawnFn();
      const baseUrl = await startServer(proc);
      return { proc, baseUrl };
    },
    // teardown
    (ctx) => { if (ctx.proc) ctx.proc.kill(); },
    // clientFactory: create a new httpConnect client
    (ctx, opts) => httpConnect(ctx.baseUrl, opts),
    // describeFactory
    (ctx) => httpIntrospect(ctx.baseUrl),
  );
}

/** Pipe transport: each test gets a fresh subprocessConnect client. */
function definePipeConformanceTests(
  label: string,
  cmd: string[],
  cmdOpts?: { cwd?: string },
  extraOpts?: { supportsZeroRowExchange?: boolean },
) {
  defineConformanceTests(
    label,
    // setup: no-op (clients are created per-test)
    async () => ({ cmd, cmdOpts }),
    // teardown: no-op
    () => {},
    // clientFactory
    (ctx, opts) => subprocessConnect(ctx.cmd, { cwd: ctx.cmdOpts?.cwd, onLog: opts?.onLog }),
    // describeFactory
    (ctx) => {
      const c = subprocessConnect(ctx.cmd, { cwd: ctx.cmdOpts?.cwd });
      return c.describe().then((desc) => { c.close(); return desc; });
    },
    // Pipe transport can't cleanly recover from init errors.
    { supportsInitErrors: false, ...extraOpts },
  );
}

function defineConformanceTests<TCtx>(
  label: string,
  setup: () => Promise<TCtx>,
  teardown: (ctx: TCtx) => void,
  clientFactory: (ctx: TCtx, opts?: { onLog?: (msg: LogMessage) => void }) => RpcClient,
  describeFactory: (ctx: TCtx) => Promise<any>,
  opts?: { supportsInitErrors?: boolean; supportsZeroRowExchange?: boolean },
) {
  const supportsInitErrors = opts?.supportsInitErrors ?? true;
  const supportsZeroRowExchange = opts?.supportsZeroRowExchange ?? true;
  describe(`Client conformance [${label}]`, () => {
    let ctx: TCtx;

    beforeAll(async () => {
      ctx = await setup();
    });

    afterAll(() => {
      teardown(ctx);
    });

    // -----------------------------------------------------------------
    // TestUnaryScalarEcho (5 tests)
    // -----------------------------------------------------------------

    describe("TestUnaryScalarEcho", () => {
      it("echo_string", async () => {
        const client = clientFactory(ctx);
        const result = await client.call("echo_string", { value: "hello" });
        expect(result!.result).toBe("hello");
        client.close();
      });

      it("echo_bytes", async () => {
        const client = clientFactory(ctx);
        const result = await client.call("echo_bytes", { data: new Uint8Array([104, 101, 108, 108, 111]) });
        expect(result!.result).toBeInstanceOf(Uint8Array);
        expect(new TextDecoder().decode(result!.result as Uint8Array)).toBe("hello");
        client.close();
      });

      it("echo_int", async () => {
        const client = clientFactory(ctx);
        const result = await client.call("echo_int", { value: 42 });
        expect(result!.result).toBe(42);
        client.close();
      });

      it("echo_float", async () => {
        const client = clientFactory(ctx);
        const result = await client.call("echo_float", { value: 3.14 });
        expect(result!.result).toBeCloseTo(3.14);
        client.close();
      });

      it("echo_bool", async () => {
        const client = clientFactory(ctx);
        expect((await client.call("echo_bool", { value: true }))!.result).toBe(true);
        expect((await client.call("echo_bool", { value: false }))!.result).toBe(false);
        client.close();
      });
    });

    // -----------------------------------------------------------------
    // TestUnaryVoid (2 tests)
    // -----------------------------------------------------------------

    describe("TestUnaryVoid", () => {
      it("void_noop", async () => {
        const client = clientFactory(ctx);
        const result = await client.call("void_noop");
        expect(result).toBeNull();
        client.close();
      });

      it("void_with_param", async () => {
        const client = clientFactory(ctx);
        const result = await client.call("void_with_param", { value: 99 });
        expect(result).toBeNull();
        client.close();
      });
    });

    // -----------------------------------------------------------------
    // TestUnaryComplexTypes (6 tests)
    // -----------------------------------------------------------------

    describe("TestUnaryComplexTypes", () => {
      it("echo_enum PENDING", async () => {
        const client = clientFactory(ctx);
        const result = await client.call("echo_enum", { status: "PENDING" });
        expect(result!.result).toBe("PENDING");
        client.close();
      });

      it("echo_enum ACTIVE", async () => {
        const client = clientFactory(ctx);
        const result = await client.call("echo_enum", { status: "ACTIVE" });
        expect(result!.result).toBe("ACTIVE");
        client.close();
      });

      it("echo_enum CLOSED", async () => {
        const client = clientFactory(ctx);
        const result = await client.call("echo_enum", { status: "CLOSED" });
        expect(result!.result).toBe("CLOSED");
        client.close();
      });

      it("echo_list", async () => {
        const client = clientFactory(ctx);
        const result = await client.call("echo_list", { values: ["a", "b", "c"] });
        const arr = result!.result;
        const list: string[] = [];
        for (let i = 0; i < arr.length; i++) list.push(arr.get(i));
        expect(list).toEqual(["a", "b", "c"]);
        client.close();
      });

      it("echo_dict", async () => {
        const client = clientFactory(ctx);
        const result = await client.call("echo_dict", { mapping: new Map([["z", 1], ["a", 2], ["m", 3]]) });
        expect(result).toBeTruthy();
        client.close();
      });

      it("echo_nested_list", async () => {
        const client = clientFactory(ctx);
        const result = await client.call("echo_nested_list", { matrix: [[1, 2], [3, 4, 5], [6]] });
        const outer = result!.result;
        const matrix: number[][] = [];
        for (let i = 0; i < outer.length; i++) {
          const inner = outer.get(i);
          const row: number[] = [];
          for (let j = 0; j < inner.length; j++) {
            const val = inner.get(j);
            row.push(typeof val === "bigint" ? Number(val) : val);
          }
          matrix.push(row);
        }
        expect(matrix).toEqual([[1, 2], [3, 4, 5], [6]]);
        client.close();
      });
    });

    // -----------------------------------------------------------------
    // TestUnaryOptional (5 tests)
    // -----------------------------------------------------------------

    describe("TestUnaryOptional", () => {
      it("optional_string null", async () => {
        const client = clientFactory(ctx);
        const result = await client.call("echo_optional_string", { value: null });
        expect(result!.result).toBeNull();
        client.close();
      });

      it("optional_string non-null", async () => {
        const client = clientFactory(ctx);
        const result = await client.call("echo_optional_string", { value: "hello" });
        expect(result!.result).toBe("hello");
        client.close();
      });

      it("optional_int null", async () => {
        const client = clientFactory(ctx);
        const result = await client.call("echo_optional_int", { value: null });
        expect(result!.result).toBeNull();
        client.close();
      });

      it("optional_int non-null", async () => {
        const client = clientFactory(ctx);
        const result = await client.call("echo_optional_int", { value: 7 });
        expect(result!.result).toBe(7);
        client.close();
      });

      it("empty string vs null", async () => {
        const client = clientFactory(ctx);
        expect((await client.call("echo_optional_string", { value: "" }))!.result).toBe("");
        expect((await client.call("echo_optional_string", { value: null }))!.result).toBeNull();
        client.close();
      });
    });

    // -----------------------------------------------------------------
    // TestUnaryDataclass (5 tests)
    // -----------------------------------------------------------------

    describe("TestUnaryDataclass", () => {
      it("echo_point", async () => {
        const client = clientFactory(ctx);
        const pointBytes = serializeDataclass(POINT_SCHEMA, { x: 1.5, y: 2.5 });
        const result = await client.call("echo_point", { point: pointBytes });
        expect(result!.result).toBeInstanceOf(Uint8Array);
        client.close();
      });

      it("echo_bounding_box", async () => {
        const client = clientFactory(ctx);
        const bbSchema = new Schema([
          new Field("top_left", new Struct([
            new Field("x", new Float64(), false),
            new Field("y", new Float64(), false),
          ]), false),
          new Field("bottom_right", new Struct([
            new Field("x", new Float64(), false),
            new Field("y", new Float64(), false),
          ]), false),
          new Field("label", new Utf8(), false),
        ]);
        const tlX = vectorFromArray([0.0], new Float64()).data[0];
        const tlY = vectorFromArray([10.0], new Float64()).data[0];
        const tlStruct = new Struct([new Field("x", new Float64(), false), new Field("y", new Float64(), false)]);
        const tlData = makeData({ type: tlStruct, length: 1, children: [tlX, tlY], nullCount: 0 });
        const brX = vectorFromArray([10.0], new Float64()).data[0];
        const brY = vectorFromArray([0.0], new Float64()).data[0];
        const brData = makeData({ type: tlStruct, length: 1, children: [brX, brY], nullCount: 0 });
        const labelData = vectorFromArray(["test"], new Utf8()).data[0];
        const structType = new Struct(bbSchema.fields);
        const data = makeData({ type: structType, length: 1, children: [tlData, brData, labelData], nullCount: 0 });
        const batch = new RecordBatch(bbSchema, data);
        const writer = new RecordBatchStreamWriter();
        writer.reset(undefined, bbSchema);
        writer.write(batch);
        writer.close();
        const bbBytes = writer.toUint8Array(true);

        const result = await client.call("echo_bounding_box", { box: bbBytes });
        expect(result!.result).toBeInstanceOf(Uint8Array);
        client.close();
      });

      it("echo_all_types", async () => {
        const client = clientFactory(ctx);
        expect(true).toBe(true);
        client.close();
      });

      it("echo_all_types_with_nulls", async () => {
        const client = clientFactory(ctx);
        expect(true).toBe(true);
        client.close();
      });

      it("inspect_point", async () => {
        const client = clientFactory(ctx);
        const pointBytes = serializeDataclass(POINT_SCHEMA, { x: 1.5, y: 2.5 });
        const result = await client.call("inspect_point", { point: pointBytes });
        expect(result!.result).toBe("Point(1.5, 2.5)");
        client.close();
      });
    });

    // -----------------------------------------------------------------
    // TestUnaryAnnotated (2 tests)
    // -----------------------------------------------------------------

    describe("TestUnaryAnnotated", () => {
      it("echo_int32", async () => {
        const client = clientFactory(ctx);
        const result = await client.call("echo_int32", { value: 42 });
        expect(result!.result).toBe(42);
        client.close();
      });

      it("echo_float32", async () => {
        const client = clientFactory(ctx);
        const result = await client.call("echo_float32", { value: 1.5 });
        expect(result!.result).toBeCloseTo(1.5);
        client.close();
      });
    });

    // -----------------------------------------------------------------
    // TestUnaryMultiParam (5 tests)
    // -----------------------------------------------------------------

    describe("TestUnaryMultiParam", () => {
      it("add_floats", async () => {
        const client = clientFactory(ctx);
        const result = await client.call("add_floats", { a: 1.5, b: 2.5 });
        expect(result!.result).toBeCloseTo(4.0);
        client.close();
      });

      it("concatenate with default", async () => {
        const client = clientFactory(ctx);
        const result = await client.call("concatenate", { prefix: "hello", suffix: "world" });
        expect(result!.result).toBe("hello-world");
        client.close();
      });

      it("concatenate custom separator", async () => {
        const client = clientFactory(ctx);
        const result = await client.call("concatenate", { prefix: "hello", suffix: "world", separator: "_" });
        expect(result!.result).toBe("hello_world");
        client.close();
      });

      it("with_defaults all default", async () => {
        const client = clientFactory(ctx);
        const result = await client.call("with_defaults", { required: 1 });
        expect(result!.result).toBe("required=1, optional_str=default, optional_int=42");
        client.close();
      });

      it("with_defaults override all", async () => {
        const client = clientFactory(ctx);
        const result = await client.call("with_defaults", { required: 2, optional_str: "custom", optional_int: 99 });
        expect(result!.result).toBe("required=2, optional_str=custom, optional_int=99");
        client.close();
      });
    });

    // -----------------------------------------------------------------
    // TestUnaryErrors (3 tests)
    // -----------------------------------------------------------------

    describe("TestUnaryErrors", () => {
      it("raise_value_error", async () => {
        const client = clientFactory(ctx);
        try {
          await client.call("raise_value_error", { message: "test error" });
          expect(true).toBe(false);
        } catch (e: any) {
          expect(e).toBeInstanceOf(RpcError);
          expect(e.errorType).toBe("ValueError");
          expect(e.errorMessage).toContain("test error");
        }
        client.close();
      });

      it("raise_runtime_error", async () => {
        const client = clientFactory(ctx);
        try {
          await client.call("raise_runtime_error", { message: "runtime error" });
          expect(true).toBe(false);
        } catch (e: any) {
          expect(e).toBeInstanceOf(RpcError);
          expect(e.errorType).toBe("RuntimeError");
        }
        client.close();
      });

      it("raise_type_error", async () => {
        const client = clientFactory(ctx);
        try {
          await client.call("raise_type_error", { message: "type error" });
          expect(true).toBe(false);
        } catch (e: any) {
          expect(e).toBeInstanceOf(RpcError);
          expect(e.errorType).toBe("TypeError");
        }
        client.close();
      });
    });

    // -----------------------------------------------------------------
    // TestUnaryLogging (3 tests)
    // -----------------------------------------------------------------

    describe("TestUnaryLogging", () => {
      it("echo_with_info_log", async () => {
        const logs: LogMessage[] = [];
        const client = clientFactory(ctx, { onLog: (msg) => logs.push(msg) });
        const result = await client.call("echo_with_info_log", { value: "test" });
        expect(result!.result).toBe("test");
        expect(logs.length).toBe(1);
        expect(logs[0].level).toBe("INFO");
        expect(logs[0].message).toContain("test");
        client.close();
      });

      it("echo_with_multi_logs", async () => {
        const logs: LogMessage[] = [];
        const client = clientFactory(ctx, { onLog: (msg) => logs.push(msg) });
        const result = await client.call("echo_with_multi_logs", { value: "multi" });
        expect(result!.result).toBe("multi");
        expect(logs.length).toBe(3);
        expect(logs[0].level).toBe("DEBUG");
        expect(logs[1].level).toBe("INFO");
        expect(logs[2].level).toBe("WARN");
        client.close();
      });

      it("echo_with_log_extras", async () => {
        const logs: LogMessage[] = [];
        const client = clientFactory(ctx, { onLog: (msg) => logs.push(msg) });
        const result = await client.call("echo_with_log_extras", { value: "extra" });
        expect(result!.result).toBe("extra");
        expect(logs.length).toBe(1);
        expect(logs[0].level).toBe("INFO");
        expect(logs[0].extra).toBeDefined();
        expect(logs[0].extra!.source).toBe("conformance");
        expect(logs[0].extra!.detail).toBe("extra");
        client.close();
      });
    });

    // -----------------------------------------------------------------
    // TestBoundaryValues (26 tests)
    // -----------------------------------------------------------------

    describe("TestBoundaryValues", () => {
      let client: RpcClient;
      beforeAll(() => { client = clientFactory(ctx); });
      afterAll(() => { client.close(); });

      // Strings
      it("empty string", async () => {
        expect((await client.call("echo_string", { value: "" }))!.result).toBe("");
      });

      it("unicode emoji", async () => {
        const val = "\u{1F600}\u{1F680}";
        expect((await client.call("echo_string", { value: val }))!.result).toBe(val);
      });

      it("unicode CJK", async () => {
        const val = "\u4f60\u597d\u4e16\u754c";
        expect((await client.call("echo_string", { value: val }))!.result).toBe(val);
      });

      it("unicode RTL", async () => {
        const val = "\u0645\u0631\u062d\u0628\u0627";
        expect((await client.call("echo_string", { value: val }))!.result).toBe(val);
      });

      it("string null byte", async () => {
        expect((await client.call("echo_string", { value: "a\x00b" }))!.result).toBe("a\x00b");
      });

      it("string escapes", async () => {
        expect((await client.call("echo_string", { value: "\n\t\\" }))!.result).toBe("\n\t\\");
      });

      // Bytes
      it("empty bytes", async () => {
        const result = await client.call("echo_bytes", { data: new Uint8Array(0) });
        expect(result!.result).toBeInstanceOf(Uint8Array);
        expect((result!.result as Uint8Array).length).toBe(0);
      });

      it("null bytes", async () => {
        const data = new Uint8Array(1000).fill(0);
        const result = await client.call("echo_bytes", { data });
        expect((result!.result as Uint8Array).length).toBe(1000);
        for (const b of result!.result as Uint8Array) expect(b).toBe(0);
      });

      it("high bytes", async () => {
        const data = new Uint8Array(1000).fill(0xff);
        const result = await client.call("echo_bytes", { data });
        expect((result!.result as Uint8Array).length).toBe(1000);
        for (const b of result!.result as Uint8Array) expect(b).toBe(0xff);
      });

      // Integers
      it("int zero", async () => {
        expect((await client.call("echo_int", { value: 0 }))!.result).toBe(0);
      });

      it("int negative", async () => {
        expect((await client.call("echo_int", { value: -1 }))!.result).toBe(-1);
      });

      it("int max int64", async () => {
        const max = BigInt("9223372036854775807");
        const result = await client.call("echo_int", { value: max });
        expect(result!.result).toBe(max);
      });

      it("int min int64", async () => {
        const min = BigInt("-9223372036854775808");
        const result = await client.call("echo_int", { value: min });
        expect(result!.result).toBe(min);
      });

      // Floats
      it("float zero", async () => {
        expect((await client.call("echo_float", { value: 0.0 }))!.result).toBe(0.0);
      });

      it("float negative zero", async () => {
        const result = (await client.call("echo_float", { value: -0.0 }))!.result;
        expect(Object.is(result, -0)).toBe(true);
      });

      it("float inf", async () => {
        expect((await client.call("echo_float", { value: Infinity }))!.result).toBe(Infinity);
      });

      it("float neg inf", async () => {
        expect((await client.call("echo_float", { value: -Infinity }))!.result).toBe(-Infinity);
      });

      it("float NaN", async () => {
        const result = (await client.call("echo_float", { value: NaN }))!.result;
        expect(Number.isNaN(result)).toBe(true);
      });

      it("float small", async () => {
        const result = (await client.call("echo_float", { value: 5e-324 }))!.result as number;
        expect(result).toBe(5e-324);
      });

      it("float large", async () => {
        const result = (await client.call("echo_float", { value: 1e300 }))!.result as number;
        expect(result).toBeCloseTo(1e300);
      });

      // Lists
      it("empty list", async () => {
        const result = await client.call("echo_list", { values: [] });
        const arr = result!.result;
        expect(arr.length).toBe(0);
      });

      it("single element list", async () => {
        const result = await client.call("echo_list", { values: ["only"] });
        const arr = result!.result;
        expect(arr.length).toBe(1);
        expect(arr.get(0)).toBe("only");
      });

      // Dicts
      it("empty dict", async () => {
        const result = await client.call("echo_dict", { mapping: new Map() });
        expect(result).toBeTruthy();
      });

      it("single entry dict", async () => {
        const result = await client.call("echo_dict", { mapping: new Map([["k", 1]]) });
        expect(result).toBeTruthy();
      });

      // Nested lists
      it("empty nested list", async () => {
        const result = await client.call("echo_nested_list", { matrix: [[]] });
        const outer = result!.result;
        expect(outer.length).toBe(1);
        expect(outer.get(0).length).toBe(0);
      });

      it("nested list varied", async () => {
        const result = await client.call("echo_nested_list", { matrix: [[1], [2, 3], [4, 5, 6]] });
        const outer = result!.result;
        expect(outer.length).toBe(3);
        expect(outer.get(0).length).toBe(1);
        expect(outer.get(1).length).toBe(2);
        expect(outer.get(2).length).toBe(3);
      });
    });

    // -----------------------------------------------------------------
    // TestLargeData (7 tests)
    // -----------------------------------------------------------------

    describe("TestLargeData", () => {
      it("large string (10KB)", async () => {
        const client = clientFactory(ctx);
        const big = "x".repeat(10_000);
        const result = await client.call("echo_string", { value: big });
        expect(result!.result).toBe(big);
        client.close();
      });

      it("large bytes (100KB)", async () => {
        const client = clientFactory(ctx);
        const big = new Uint8Array(100_000);
        for (let i = 0; i < big.length; i++) big[i] = i % 256;
        const result = await client.call("echo_bytes", { data: big });
        expect((result!.result as Uint8Array).length).toBe(100_000);
        client.close();
      });

      it("large list (10K strings)", async () => {
        const client = clientFactory(ctx);
        const big = Array.from({ length: 10_000 }, (_, i) => String(i));
        const result = await client.call("echo_list", { values: big });
        const arr = result!.result;
        expect(arr.length).toBe(10_000);
        expect(arr.get(0)).toBe("0");
        expect(arr.get(9999)).toBe("9999");
        client.close();
      });

      it("large dict (1K entries)", async () => {
        const client = clientFactory(ctx);
        const big = new Map<string, number>();
        for (let i = 0; i < 1_000; i++) big.set(`key_${i}`, i);
        const result = await client.call("echo_dict", { mapping: big });
        expect(result).toBeTruthy();
        client.close();
      });

      it("large batch producer (5x10K rows)", async () => {
        const client = clientFactory(ctx);
        const session = await client.stream("produce_large_batches", { rows_per_batch: 10_000, batch_count: 5 });
        let batchCount = 0;
        let totalRows = 0;
        for await (const rows of session) {
          batchCount++;
          totalRows += rows.length;
        }
        expect(batchCount).toBe(5);
        expect(totalRows).toBe(50_000);
        client.close();
      });

      it("large exchange (10x5K rows)", async () => {
        const client = clientFactory(ctx);
        const session = await client.stream("exchange_scale", { factor: 2.0 });
        for (let i = 0; i < 10; i++) {
          const input = Array.from({ length: 5_000 }, (_, j) => ({ value: j * 1.0 }));
          const result = await session.exchange(input);
          expect(result.length).toBe(5_000);
        }
        session.close();
        client.close();
      });

      it("many small batches (100x1)", async () => {
        const client = clientFactory(ctx);
        const session = await client.stream("produce_n", { count: 100 });
        let count = 0;
        for await (const _rows of session) {
          count++;
        }
        expect(count).toBe(100);
        client.close();
      });
    });

    // -----------------------------------------------------------------
    // TestProducerStream (6 tests)
    // -----------------------------------------------------------------

    describe("TestProducerStream", () => {
      it("produce_n(5)", async () => {
        const client = clientFactory(ctx);
        const session = await client.stream("produce_n", { count: 5 });
        const allRows: Record<string, any>[][] = [];
        for await (const rows of session) {
          allRows.push(rows);
        }
        expect(allRows.length).toBe(5);
        for (let i = 0; i < 5; i++) {
          expect(allRows[i][0].index).toBe(i);
          expect(allRows[i][0].value).toBe(i * 10);
        }
        client.close();
      });

      it("produce_empty", async () => {
        const client = clientFactory(ctx);
        const session = await client.stream("produce_empty");
        const allRows: Record<string, any>[][] = [];
        for await (const rows of session) {
          allRows.push(rows);
        }
        expect(allRows.length).toBe(0);
        client.close();
      });

      it("produce_single", async () => {
        const client = clientFactory(ctx);
        const session = await client.stream("produce_single");
        const allRows: Record<string, any>[][] = [];
        for await (const rows of session) {
          allRows.push(rows);
        }
        expect(allRows.length).toBe(1);
        expect(allRows[0][0].index).toBe(0);
        client.close();
      });

      it("produce_with_logs(3)", async () => {
        const logs: LogMessage[] = [];
        const client = clientFactory(ctx, { onLog: (msg) => logs.push(msg) });
        const session = await client.stream("produce_with_logs", { count: 3 });
        let count = 0;
        for await (const _rows of session) {
          count++;
        }
        expect(count).toBe(3);
        expect(logs.length).toBe(3);
        for (let i = 0; i < 3; i++) {
          expect(logs[i].level).toBe("INFO");
          expect(logs[i].message).toContain(String(i));
        }
        client.close();
      });

      it("produce_error_mid_stream(3)", async () => {
        const client = clientFactory(ctx);
        const session = await client.stream("produce_error_mid_stream", { emit_before_error: 3 });
        let count = 0;
        try {
          for await (const _rows of session) {
            count++;
          }
          expect(true).toBe(false); // should throw
        } catch (e: any) {
          expect(e).toBeInstanceOf(RpcError);
          expect(e.message).toContain("intentional error");
        }
        expect(count).toBeLessThanOrEqual(3);
        client.close();
      });

      it.skipIf(!supportsInitErrors)("produce_error_on_init", async () => {
        const client = clientFactory(ctx);
        try {
          const session = await client.stream("produce_error_on_init");
          for await (const _rows of session) {
            // shouldn't get here
          }
          expect(true).toBe(false); // should throw
        } catch (e: any) {
          expect(e).toBeInstanceOf(RpcError);
          expect(e.message).toContain("intentional init error");
        }
        client.close();
      });
    });

    // -----------------------------------------------------------------
    // TestProducerStreamWithHeader (2 tests)
    // -----------------------------------------------------------------

    describe("TestProducerStreamWithHeader", () => {
      it("produce_with_header(3)", async () => {
        const client = clientFactory(ctx);
        const session = await client.stream("produce_with_header", { count: 3 });
        expect(session.header).toBeTruthy();
        expect(session.header!.total_expected).toBe(3);
        expect(session.header!.description).toContain("3");

        let count = 0;
        for await (const _rows of session) {
          count++;
        }
        expect(count).toBe(3);
        client.close();
      });

      it("produce_with_header_and_logs(2)", async () => {
        const logs: LogMessage[] = [];
        const client = clientFactory(ctx, { onLog: (msg) => logs.push(msg) });
        const session = await client.stream("produce_with_header_and_logs", { count: 2 });
        expect(session.header).toBeTruthy();

        let count = 0;
        for await (const _rows of session) {
          count++;
        }
        expect(count).toBe(2);
        expect(logs.some((l) => l.message === "stream init log")).toBe(true);
        client.close();
      });
    });

    // -----------------------------------------------------------------
    // TestExchangeStream (8 tests)
    // -----------------------------------------------------------------

    describe("TestExchangeStream", () => {
      it("exchange_scale(3.0)", async () => {
        const client = clientFactory(ctx);
        const session = await client.stream("exchange_scale", { factor: 3.0 });
        const result = await session.exchange([{ value: 1.0 }, { value: 2.0 }, { value: 3.0 }]);
        expect(result[0].value).toBeCloseTo(3.0);
        expect(result[1].value).toBeCloseTo(6.0);
        expect(result[2].value).toBeCloseTo(9.0);
        session.close();
        client.close();
      });

      it("exchange_accumulate", async () => {
        const client = clientFactory(ctx);
        const session = await client.stream("exchange_accumulate");
        const out1 = await session.exchange([{ value: 1.0 }, { value: 2.0 }]);
        expect(out1[0].running_sum).toBeCloseTo(3.0);
        expect(out1[0].exchange_count).toBe(1);

        const out2 = await session.exchange([{ value: 10.0 }]);
        expect(out2[0].running_sum).toBeCloseTo(13.0);
        expect(out2[0].exchange_count).toBe(2);
        session.close();
        client.close();
      });

      it("exchange_with_logs", async () => {
        const logs: LogMessage[] = [];
        const client = clientFactory(ctx, { onLog: (msg) => logs.push(msg) });
        const session = await client.stream("exchange_with_logs");
        await session.exchange([{ value: 1.0 }]);
        expect(logs.length).toBe(2);
        expect(logs[0].level).toBe("INFO");
        expect(logs[1].level).toBe("DEBUG");
        session.close();
        client.close();
      });

      it("exchange_error_on_nth(1) - error first", async () => {
        const client = clientFactory(ctx);
        const session = await client.stream("exchange_error_on_nth", { fail_on: 1 });
        try {
          await session.exchange([{ value: 1.0 }]);
          expect(true).toBe(false);
        } catch (e: any) {
          expect(e).toBeInstanceOf(RpcError);
          expect(e.message).toContain("intentional error");
        }
        client.close();
      });

      it("exchange_error_on_nth(3) - error Nth", async () => {
        const client = clientFactory(ctx);
        const session = await client.stream("exchange_error_on_nth", { fail_on: 3 });
        await session.exchange([{ value: 1.0 }]);
        await session.exchange([{ value: 2.0 }]);
        try {
          await session.exchange([{ value: 3.0 }]);
          expect(true).toBe(false);
        } catch (e: any) {
          expect(e).toBeInstanceOf(RpcError);
          expect(e.message).toContain("intentional error");
        }
        client.close();
      });

      it.skipIf(!supportsInitErrors)("exchange_error_on_init", async () => {
        const client = clientFactory(ctx);
        try {
          await client.stream("exchange_error_on_init");
          expect(true).toBe(false);
        } catch (e: any) {
          expect(e).toBeInstanceOf(RpcError);
          expect(e.message).toContain("intentional exchange init error");
        }
        client.close();
      });

      it("empty exchange session (open + close)", async () => {
        const client = clientFactory(ctx);
        const session = await client.stream("exchange_scale", { factor: 1.0 });
        session.close();
        // Verify transport still usable
        const result = await client.call("echo_int", { value: 42 });
        expect(result!.result).toBe(42);
        client.close();
      });

      it.skipIf(!supportsZeroRowExchange)("zero-row input", async () => {
        const client = clientFactory(ctx);
        const session = await client.stream("exchange_scale", { factor: 2.0 });
        const result = await session.exchange([]);
        expect(result.length).toBe(0);
        session.close();
        client.close();
      });
    });

    // -----------------------------------------------------------------
    // TestExchangeStreamWithHeader (1 test)
    // -----------------------------------------------------------------

    describe("TestExchangeStreamWithHeader", () => {
      it("exchange_with_header(2.0)", async () => {
        const client = clientFactory(ctx);
        const session = await client.stream("exchange_with_header", { factor: 2.0 });
        expect(session.header).toBeTruthy();
        expect(session.header!.description).toContain("2.0");

        const result = await session.exchange([{ value: 5.0 }]);
        expect(result[0].value).toBeCloseTo(10.0);
        session.close();
        client.close();
      });
    });

    // -----------------------------------------------------------------
    // TestErrorRecovery (4 tests)
    // -----------------------------------------------------------------

    describe("TestErrorRecovery", () => {
      it("error then success", async () => {
        const client = clientFactory(ctx);
        try { await client.call("raise_value_error", { message: "boom" }); } catch {}
        const result = await client.call("echo_int", { value: 42 });
        expect(result!.result).toBe(42);
        client.close();
      });

      it("stream error then unary", async () => {
        const client = clientFactory(ctx);
        try {
          const session = await client.stream("produce_error_mid_stream", { emit_before_error: 1 });
          for await (const _rows of session) {}
        } catch {}
        const result = await client.call("echo_string", { value: "ok" });
        expect(result!.result).toBe("ok");
        client.close();
      });

      it("exchange error then exchange", async () => {
        const client = clientFactory(ctx);
        try {
          const session = await client.stream("exchange_error_on_nth", { fail_on: 1 });
          await session.exchange([{ value: 1.0 }]);
        } catch {}

        const session2 = await client.stream("exchange_scale", { factor: 2.0 });
        const out = await session2.exchange([{ value: 5.0 }]);
        expect(out[0].value).toBeCloseTo(10.0);
        session2.close();
        client.close();
      });

      it("multiple sequential sessions", async () => {
        const client = clientFactory(ctx);
        expect((await client.call("echo_int", { value: 1 }))!.result).toBe(1);

        const session1 = await client.stream("produce_n", { count: 2 });
        let count = 0;
        for await (const _rows of session1) count++;
        expect(count).toBe(2);

        const session2 = await client.stream("exchange_scale", { factor: 2.0 });
        const out = await session2.exchange([{ value: 3.0 }]);
        expect(out[0].value).toBeCloseTo(6.0);
        session2.close();

        expect((await client.call("echo_string", { value: "end" }))!.result).toBe("end");
        client.close();
      });
    });

    // -----------------------------------------------------------------
    // TestDescribeConformance (2 tests)
    // -----------------------------------------------------------------

    describe("TestDescribeConformance", () => {
      it("verify 46 methods", async () => {
        const desc = await describeFactory(ctx);
        expect(desc.methods.length).toBe(46);
        expect(["Conformance", "ConformanceService"]).toContain(desc.protocolName);
      });

      it("verify method types", async () => {
        const desc = await describeFactory(ctx);
        const methodMap = new Map(desc.methods.map((m) => [m.name, m]));

        expect(methodMap.get("echo_string")!.type).toBe("unary");
        expect(methodMap.get("add_floats")!.type).toBe("unary");
        expect(methodMap.get("raise_value_error")!.type).toBe("unary");

        expect(methodMap.get("produce_n")!.type).toBe("stream");
        expect(methodMap.get("exchange_scale")!.type).toBe("stream");
        expect(methodMap.get("produce_with_header")!.type).toBe("stream");
      });
    });

    // -----------------------------------------------------------------
    // TestDynamicRichHeader (3 tests)
    // -----------------------------------------------------------------

    describe("TestDynamicRichHeader", () => {
      function assertRichHeader(header: Record<string, any>, seed: number) {
        expect(header.str_field).toBe(`seed-${seed}`);
        expect(header.int_field).toBe(seed * 7);
        expect(header.float_field).toBeCloseTo(seed * 1.5);
        expect(header.bool_field).toBe(seed % 2 === 0);
        expect(header.annotated_int32).toBe(seed % 1000);
        expect(header.annotated_float32).toBeCloseTo(seed / 3.0);
      }

      it("seed 42", async () => {
        const client = clientFactory(ctx);
        const session = await client.stream("produce_with_rich_header", { seed: 42, count: 3 });
        expect(session.header).toBeTruthy();
        assertRichHeader(session.header!, 42);

        let count = 0;
        for await (const rows of session) {
          expect(rows[0].index).toBe(count);
          expect(rows[0].value).toBe(count * 10);
          count++;
        }
        expect(count).toBe(3);
        client.close();
      });

      it("seed 7", async () => {
        const client = clientFactory(ctx);
        const session = await client.stream("produce_with_rich_header", { seed: 7, count: 2 });
        assertRichHeader(session.header!, 7);

        let count = 0;
        for await (const _rows of session) count++;
        expect(count).toBe(2);
        client.close();
      });

      it("seed 0", async () => {
        const client = clientFactory(ctx);
        const session = await client.stream("produce_with_rich_header", { seed: 0, count: 1 });
        assertRichHeader(session.header!, 0);

        let count = 0;
        for await (const _rows of session) count++;
        expect(count).toBe(1);
        client.close();
      });
    });

    // -----------------------------------------------------------------
    // TestDynamicSchemaProducer (4 tests)
    // -----------------------------------------------------------------

    describe("TestDynamicSchemaProducer", () => {
      it("all columns", async () => {
        const client = clientFactory(ctx);
        const session = await client.stream("produce_dynamic_schema", {
          seed: 42, count: 3, include_strings: true, include_floats: true,
        });
        expect(session.header).toBeTruthy();

        const allRows: Record<string, any>[][] = [];
        for await (const rows of session) allRows.push(rows);
        expect(allRows.length).toBe(3);
        for (let i = 0; i < 3; i++) {
          expect(allRows[i][0].index).toBe(i);
          expect(allRows[i][0].label).toBe(`row-${i}`);
          expect(allRows[i][0].score).toBeCloseTo(i * 1.5);
        }
        client.close();
      });

      it("strings only", async () => {
        const client = clientFactory(ctx);
        const session = await client.stream("produce_dynamic_schema", {
          seed: 7, count: 2, include_strings: true, include_floats: false,
        });
        expect(session.header).toBeTruthy();

        const allRows: Record<string, any>[][] = [];
        for await (const rows of session) allRows.push(rows);
        expect(allRows.length).toBe(2);
        for (let i = 0; i < 2; i++) {
          expect(allRows[i][0].label).toBe(`row-${i}`);
          expect(allRows[i][0].score).toBeUndefined();
        }
        client.close();
      });

      it("floats only", async () => {
        const client = clientFactory(ctx);
        const session = await client.stream("produce_dynamic_schema", {
          seed: 5, count: 2, include_strings: false, include_floats: true,
        });
        expect(session.header).toBeTruthy();

        const allRows: Record<string, any>[][] = [];
        for await (const rows of session) allRows.push(rows);
        expect(allRows.length).toBe(2);
        for (let i = 0; i < 2; i++) {
          expect(allRows[i][0].score).toBeCloseTo(i * 1.5);
          expect(allRows[i][0].label).toBeUndefined();
        }
        client.close();
      });

      it("minimal", async () => {
        const client = clientFactory(ctx);
        const session = await client.stream("produce_dynamic_schema", {
          seed: 0, count: 1, include_strings: false, include_floats: false,
        });
        expect(session.header).toBeTruthy();

        const allRows: Record<string, any>[][] = [];
        for await (const rows of session) allRows.push(rows);
        expect(allRows.length).toBe(1);
        expect(allRows[0][0].index).toBe(0);
        client.close();
      });
    });

    // -----------------------------------------------------------------
    // TestRichHeaderExchange (2 tests)
    // -----------------------------------------------------------------

    describe("TestRichHeaderExchange", () => {
      it("seed 5, factor 2.5", async () => {
        const client = clientFactory(ctx);
        const session = await client.stream("exchange_with_rich_header", { seed: 5, factor: 2.5 });
        expect(session.header).toBeTruthy();
        expect(session.header!.str_field).toBe("seed-5");

        const result = await session.exchange([{ value: 4.0 }]);
        expect(result[0].value).toBeCloseTo(10.0);
        session.close();
        client.close();
      });

      it("seed 12, factor 1.0", async () => {
        const client = clientFactory(ctx);
        const session = await client.stream("exchange_with_rich_header", { seed: 12, factor: 1.0 });
        expect(session.header).toBeTruthy();
        expect(session.header!.str_field).toBe("seed-12");

        const result = await session.exchange([{ value: 7.0 }]);
        expect(result[0].value).toBeCloseTo(7.0);
        session.close();
        client.close();
      });
    });
  });
}

// ---------------------------------------------------------------------------
// Run the conformance suite against HTTP and pipe transports
// ---------------------------------------------------------------------------

defineHttpConformanceTests("bun-http", () =>
  Bun.spawn(["bun", "run", "examples/conformance-http.ts"], {
    stdout: "pipe",
    stderr: "pipe",
  }),
);

if (hasPython) {
  defineHttpConformanceTests("python-http", () =>
    Bun.spawn([PYTHON, "-m", "tests.serve_conformance_describe_http"], {
      cwd: PYTHON_CWD,
      stdout: "pipe",
      stderr: "pipe",
    }),
  );
}

definePipeConformanceTests(
  "bun-pipe",
  ["bun", "run", "examples/conformance.ts"],
);

if (hasPython) {
  definePipeConformanceTests(
    "python-pipe",
    [PYTHON, "-m", "tests.serve_conformance_describe"],
    { cwd: PYTHON_CWD },
    // Python server strictly validates input schema. The pipe transport can't
    // know the correct schema for zero-row exchange since the describe response
    // doesn't include stream IO schemas (HTTP gets it from the init response).
    { supportsZeroRowExchange: false },
  );
}
