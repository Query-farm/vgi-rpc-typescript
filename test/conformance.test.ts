import { describe, it, expect, afterEach } from "bun:test";
import {
  RecordBatchReader,
  RecordBatchStreamWriter,
  Table,
  Schema,
  Field,
  Float64,
  makeData,
  Struct,
  vectorFromArray,
  RecordBatch,
} from "apache-arrow";
import { unlinkSync } from "node:fs";

const VGI_CLI = "/Users/rusty/Development/vgi-rpc/.venv/bin/vgi-rpc";
const TS_DIR = "/Users/rusty/Development/vgi-rpc-typescript";

const tmpFiles: string[] = [];

function tmpFile(name: string, ext = "arrow"): string {
  const path = `/tmp/vgi-conf-${name}-${Date.now()}.${ext}`;
  tmpFiles.push(path);
  return path;
}

afterEach(() => {
  for (const f of tmpFiles) {
    try { unlinkSync(f); } catch {}
  }
  tmpFiles.length = 0;
});

async function run(
  args: string[],
  timeoutMs = 5000,
): Promise<{ stdout: string; stderr: string; exitCode: number }> {
  const proc = Bun.spawn(args, { stdout: "pipe", stderr: "pipe" });
  const timer = setTimeout(() => proc.kill(), timeoutMs);
  const exitCode = await proc.exited;
  clearTimeout(timer);
  const stdout = await new Response(proc.stdout).text();
  const stderr = await new Response(proc.stderr).text();
  return { stdout: stdout.trim(), stderr: stderr.trim(), exitCode };
}

function cli(...extra: string[]): string[] {
  const cmd = `bun run ${TS_DIR}/examples/conformance.ts`;
  return [VGI_CLI, "--cmd", cmd, ...extra];
}

/** Call a unary method and return parsed JSON. */
async function callJson(method: string, ...params: string[]) {
  const { stdout, stderr, exitCode } = await run(cli("call", method, ...params));
  if (exitCode !== 0) throw new Error(`exit ${exitCode}: ${stdout} ${stderr}`);
  return JSON.parse(stdout);
}

/** Call with --format arrow and return the table. */
async function callArrow(method: string, params: string[]): Promise<Table> {
  const outFile = tmpFile(method);
  const { exitCode, stdout, stderr } = await run(
    cli("--format", "arrow", "-o", outFile, "call", method, ...params),
  );
  if (exitCode !== 0) throw new Error(`exit ${exitCode}: ${stdout} ${stderr}`);
  const bytes = await Bun.file(outFile).arrayBuffer();
  const reader = await RecordBatchReader.from(new Uint8Array(bytes));
  return new Table(reader.readAll());
}

/** Call expecting an error; return the JSON error response. */
async function callError(method: string, ...params: string[]) {
  const { stdout, stderr, exitCode } = await run(cli("call", method, ...params));
  expect(exitCode).not.toBe(0);
  const combined = stdout + stderr;
  return JSON.parse(combined.split("\n").find((l) => l.startsWith("{"))!);
}

/** Create an Arrow IPC file with rows of {value: float64} for exchange input. */
async function writeExchangeInput(values: number[]): Promise<string> {
  const schema = new Schema([new Field("value", new Float64(), false)]);
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, schema);
  for (const v of values) {
    const arr = vectorFromArray([v], new Float64());
    const data = makeData({
      type: new Struct(schema.fields),
      length: 1,
      children: [arr.data[0]],
      nullCount: 0,
    });
    writer.write(new RecordBatch(schema, data));
  }
  writer.close();
  const bytes = writer.toUint8Array(true);
  const path = tmpFile("exchange-input");
  await Bun.write(path, bytes);
  return path;
}

// =========================================================================
// Describe
// =========================================================================

describe("conformance: describe", () => {
  it("lists all 43 methods", async () => {
    const { stdout, exitCode } = await run(cli("describe"));
    expect(exitCode).toBe(0);
    const data = JSON.parse(stdout);
    expect(data.protocol_name).toBe("Conformance");
    expect(Object.keys(data.methods).length).toBe(43);
  });

  it("has header methods in describe", async () => {
    // The CLI describe output shows headers are present via the header stream
    // (has_header is in the raw batch but not in CLI JSON output)
    // Verify header methods respond correctly by calling them
    const { stdout, exitCode } = await run(
      cli("call", "produce_with_header", "count=1"),
    );
    expect(exitCode).toBe(0);
    expect(stdout).toContain("__header__");
  });
});

// =========================================================================
// Scalar Echo
// =========================================================================

describe("conformance: scalar echo", () => {
  it("echo_string", async () => {
    const r = await callJson("echo_string", "value=hello world");
    expect(r.result).toBe("hello world");
  });

  it("echo_int", async () => {
    const r = await callJson("echo_int", "value=42");
    expect(r.result).toBe(42);
  });

  it("echo_float", async () => {
    const r = await callJson("echo_float", "value=3.14");
    expect(r.result).toBeCloseTo(3.14);
  });

  it("echo_bool", async () => {
    const r = await callJson("echo_bool", "value=true");
    expect(r.result).toBe(true);
  });

  it("echo_bytes round-trip", async () => {
    // Pass base64-encoded bytes via --json; CLI renders binary as b'...'
    const r = await callJson("echo_bytes", "--json", '{"data": "aGVsbG8="}');
    expect(r.result).toContain("aGVsbG8=");
  });
});

// =========================================================================
// Void Returns
// =========================================================================

describe("conformance: void", () => {
  it("void_noop", async () => {
    const { stdout, exitCode } = await run(cli("call", "void_noop"));
    expect(exitCode).toBe(0);
    expect(stdout).toBe("null");
  });

  it("void_with_param", async () => {
    const { stdout, exitCode } = await run(cli("call", "void_with_param", "value=99"));
    expect(exitCode).toBe(0);
    expect(stdout).toBe("null");
  });
});

// =========================================================================
// Complex Types
// =========================================================================

describe("conformance: complex types", () => {
  it("echo_enum", async () => {
    const r = await callJson("echo_enum", "status=ACTIVE");
    expect(r.result).toBe("ACTIVE");
  });

  it("echo_list", async () => {
    const r = await callJson("echo_list", '--json', '{"values": ["a", "b", "c"]}');
    expect(r.result).toEqual(["a", "b", "c"]);
  });

  it("echo_dict", async () => {
    const r = await callJson("echo_dict", '--json', '{"mapping": {"x": 1, "y": 2}}');
    // CLI renders map as dict in JSON
    expect(r.result).toEqual({ x: 1, y: 2 });
  });

  it("echo_nested_list", async () => {
    const r = await callJson("echo_nested_list", '--json', '{"matrix": [[1, 2], [3]]}');
    expect(r.result).toEqual([[1, 2], [3]]);
  });
});

// =========================================================================
// Nullable
// =========================================================================

describe("conformance: nullable", () => {
  it("echo_optional_string with value", async () => {
    const r = await callJson("echo_optional_string", "value=test");
    expect(r.result).toBe("test");
  });

  it("echo_optional_string with null", async () => {
    const r = await callJson("echo_optional_string", '--json', '{"value": null}');
    expect(r.result).toBeNull();
  });

  it("echo_optional_int with value", async () => {
    const r = await callJson("echo_optional_int", "value=42");
    expect(r.result).toBe(42);
  });

  it("echo_optional_int with null", async () => {
    const r = await callJson("echo_optional_int", '--json', '{"value": null}');
    expect(r.result).toBeNull();
  });
});

// =========================================================================
// Annotated Types
// =========================================================================

describe("conformance: annotated types", () => {
  it("echo_int32", async () => {
    const r = await callJson("echo_int32", "value=42");
    expect(r.result).toBe(42);
  });

  it("echo_float32", async () => {
    const r = await callJson("echo_float32", "value=3.14");
    expect(r.result).toBeCloseTo(3.14, 2);
  });
});

// =========================================================================
// Multi-Param & Defaults
// =========================================================================

describe("conformance: multi-param", () => {
  it("add_floats", async () => {
    const r = await callJson("add_floats", "a=3.0", "b=4.0");
    expect(r.result).toBe(7.0);
  });

  it("concatenate with default separator", async () => {
    const r = await callJson("concatenate", "prefix=hello", "suffix=world");
    expect(r.result).toBe("hello-world");
  });

  it("concatenate with custom separator", async () => {
    const r = await callJson("concatenate", "prefix=hello", "suffix=world", "separator=_");
    expect(r.result).toBe("hello_world");
  });

  it("with_defaults using defaults", async () => {
    const r = await callJson("with_defaults", "required=7");
    expect(r.result).toBe("required=7, optional_str=default, optional_int=42");
  });

  it("with_defaults overriding", async () => {
    const r = await callJson("with_defaults", "required=1", "optional_str=custom", "optional_int=99");
    expect(r.result).toBe("required=1, optional_str=custom, optional_int=99");
  });
});

// =========================================================================
// Error Propagation
// =========================================================================

describe("conformance: errors", () => {
  it("raise_value_error", async () => {
    const r = await callError("raise_value_error", "message=boom");
    expect(r.error.type).toBe("ValueError");
    expect(r.error.message).toContain("boom");
  });

  it("raise_runtime_error", async () => {
    const r = await callError("raise_runtime_error", "message=oops");
    expect(r.error.type).toBe("RuntimeError");
    expect(r.error.message).toContain("oops");
  });

  it("raise_type_error", async () => {
    const r = await callError("raise_type_error", "message=bad type");
    expect(r.error.type).toBe("TypeError");
    expect(r.error.message).toContain("bad type");
  });
});

// =========================================================================
// Client-Directed Logging
// =========================================================================

describe("conformance: logging", () => {
  it("echo_with_info_log", async () => {
    const { stdout, stderr, exitCode } = await run(
      cli("-v", "call", "echo_with_info_log", "value=test"),
    );
    expect(exitCode).toBe(0);
    expect(stderr).toContain("info: test");
    const data = JSON.parse(stdout);
    expect(data.result).toBe("test");
  });

  it("echo_with_multi_logs", async () => {
    const { stdout, stderr, exitCode } = await run(
      cli("-v", "--log-level", "DEBUG", "call", "echo_with_multi_logs", "value=hi"),
    );
    expect(exitCode).toBe(0);
    expect(stderr).toContain("debug: hi");
    expect(stderr).toContain("info: hi");
    expect(stderr).toContain("warn: hi");
    const data = JSON.parse(stdout);
    expect(data.result).toBe("hi");
  });

  it("echo_with_log_extras", async () => {
    const { stdout, stderr, exitCode } = await run(
      cli("-v", "call", "echo_with_log_extras", "value=x"),
    );
    expect(exitCode).toBe(0);
    expect(stderr).toContain("info: x");
    const data = JSON.parse(stdout);
    expect(data.result).toBe("x");
  });
});

// =========================================================================
// Producer Streams
// =========================================================================

describe("conformance: producer streams", () => {
  it("produce_n with 5 batches", async () => {
    const table = await callArrow("produce_n", ["count=5"]);
    expect(table.numRows).toBe(5);
    const idx = table.getChildAt(0)!;
    const val = table.getChildAt(1)!;
    for (let i = 0; i < 5; i++) {
      expect(Number(idx.get(i))).toBe(i);
      expect(Number(val.get(i))).toBe(i * 10);
    }
  });

  it("produce_empty", async () => {
    const table = await callArrow("produce_empty", []);
    expect(table.numRows).toBe(0);
  });

  it("produce_single", async () => {
    const table = await callArrow("produce_single", []);
    expect(table.numRows).toBe(1);
    expect(Number(table.getChildAt(0)!.get(0))).toBe(0);
    expect(Number(table.getChildAt(1)!.get(0))).toBe(0);
  });

  it("produce_large_batches", async () => {
    const table = await callArrow("produce_large_batches", [
      "rows_per_batch=100",
      "batch_count=3",
    ]);
    expect(table.numRows).toBe(300);
    // First row of second batch
    expect(Number(table.getChildAt(0)!.get(100))).toBe(100);
    expect(Number(table.getChildAt(1)!.get(100))).toBe(1000);
  });

  it("produce_with_logs", async () => {
    const { stdout, stderr, exitCode } = await run(
      cli("-v", "--format", "arrow", "call", "produce_with_logs", "count=2"),
    );
    expect(exitCode).toBe(0);
    expect(stderr).toContain("producing batch 0");
    expect(stderr).toContain("producing batch 1");
  });

  it("produce_error_mid_stream", async () => {
    const { stderr, exitCode } = await run(
      cli("call", "produce_error_mid_stream", "emit_before_error=2"),
    );
    expect(exitCode).toBe(1);
    const combined = stderr;
    expect(combined).toContain("intentional error after 2 batches");
  });

  it("produce_error_on_init", async () => {
    const { stderr, exitCode } = await run(
      cli("call", "produce_error_on_init"),
    );
    expect(exitCode).toBe(1);
    expect(stderr).toContain("intentional init error");
  });
});

// =========================================================================
// Producer Streams With Headers
// =========================================================================

describe("conformance: producer headers", () => {
  it("produce_with_header", async () => {
    const { stdout, exitCode } = await run(
      cli("call", "produce_with_header", "count=3"),
    );
    expect(exitCode).toBe(0);
    // The CLI outputs the header as __header__
    expect(stdout).toContain('"total_expected": 3');
    expect(stdout).toContain('"producing 3 batches"');
  });

  it("produce_with_header_and_logs", async () => {
    const { stdout, stderr, exitCode } = await run(
      cli("-v", "call", "produce_with_header_and_logs", "count=2"),
    );
    expect(exitCode).toBe(0);
    expect(stderr).toContain("stream init log");
    expect(stdout).toContain('"total_expected": 2');
  });
});

// =========================================================================
// Exchange Streams
// =========================================================================

describe("conformance: exchange streams", () => {
  it("exchange_scale", async () => {
    const inputFile = await writeExchangeInput([5.0, 10.0]);
    const outFile = tmpFile("exchange-out");
    const { exitCode, stdout, stderr } = await run(
      cli("--format", "arrow", "-o", outFile, "call", "exchange_scale", "factor=2.0", "--input", inputFile),
    );
    if (exitCode !== 0) throw new Error(`exit ${exitCode}: ${stdout} ${stderr}`);
    const bytes = await Bun.file(outFile).arrayBuffer();
    const reader = await RecordBatchReader.from(new Uint8Array(bytes));
    const table = new Table(reader.readAll());
    expect(table.numRows).toBe(2);
    expect(table.getChildAt(0)!.get(0)).toBe(10.0);
    expect(table.getChildAt(0)!.get(1)).toBe(20.0);
  });

  it("exchange_accumulate", async () => {
    const inputFile = await writeExchangeInput([1.0, 2.0, 3.0]);
    const outFile = tmpFile("accum-out");
    const { exitCode, stdout, stderr } = await run(
      cli("--format", "arrow", "-o", outFile, "call", "exchange_accumulate", "--input", inputFile),
    );
    if (exitCode !== 0) throw new Error(`exit ${exitCode}: ${stdout} ${stderr}`);
    const bytes = await Bun.file(outFile).arrayBuffer();
    const reader = await RecordBatchReader.from(new Uint8Array(bytes));
    const table = new Table(reader.readAll());
    expect(table.numRows).toBe(3);
    const sums = table.getChildAt(0)!;
    const counts = table.getChildAt(1)!;
    expect(sums.get(0)).toBe(1.0);
    expect(sums.get(1)).toBe(3.0);
    expect(sums.get(2)).toBe(6.0);
    expect(Number(counts.get(0))).toBe(1);
    expect(Number(counts.get(1))).toBe(2);
    expect(Number(counts.get(2))).toBe(3);
  });

  it("exchange_with_logs", async () => {
    const inputFile = await writeExchangeInput([1.0]);
    const { stderr, exitCode } = await run(
      cli("-v", "--log-level", "DEBUG", "call", "exchange_with_logs", "--input", inputFile),
    );
    expect(exitCode).toBe(0);
    expect(stderr).toContain("exchange processing");
    expect(stderr).toContain("exchange debug");
  });

  it("exchange_error_on_nth", async () => {
    const inputFile = await writeExchangeInput([1.0, 2.0, 3.0]);
    const { stderr, exitCode } = await run(
      cli("call", "exchange_error_on_nth", "fail_on=2", "--input", inputFile),
    );
    expect(exitCode).toBe(1);
    expect(stderr).toContain("intentional error on exchange 2");
  });

  it("exchange_error_on_init", async () => {
    const inputFile = await writeExchangeInput([1.0]);
    const { stderr, exitCode } = await run(
      cli("call", "exchange_error_on_init", "--input", inputFile),
    );
    expect(exitCode).toBe(1);
    expect(stderr).toContain("intentional exchange init error");
  });
});

// =========================================================================
// Exchange Streams With Headers
// =========================================================================

describe("conformance: exchange headers", () => {
  it("exchange_with_header", async () => {
    const inputFile = await writeExchangeInput([5.0]);
    const { stdout, exitCode } = await run(
      cli("call", "exchange_with_header", "factor=3.0", "--input", inputFile),
    );
    expect(exitCode).toBe(0);
    expect(stdout).toContain("scale by 3.0");
  });
});

// =========================================================================
// Dataclass Round-trip (via Python vgi_rpc.connect)
// =========================================================================

describe("conformance: dataclass", () => {
  /** Use Python programmatic client to call methods requiring binary dataclass params. */
  async function callViaPython(script: string): Promise<string> {
    const pythonBin = "/Users/rusty/Development/vgi-rpc/.venv/bin/python3";
    const cmd = ["bun", "run", `${TS_DIR}/examples/conformance.ts`];
    const fullScript = `
from vgi_rpc import connect
from vgi_rpc.conformance import ConformanceService, Point, BoundingBox
with connect(ConformanceService, cmd=${JSON.stringify(cmd)}) as client:
    ${script}
`;
    const { stdout, stderr, exitCode } = await run(
      [pythonBin, "-c", fullScript],
    );
    if (exitCode !== 0) throw new Error(`Python exit ${exitCode}: ${stderr}`);
    return stdout.trim();
  }

  it("echo_point", async () => {
    const out = await callViaPython(`
    result = client.echo_point(point=Point(x=1.5, y=2.5))
    print(f"{result.x},{result.y}")
    `);
    const [x, y] = out.split(",").map(Number);
    expect(x).toBeCloseTo(1.5);
    expect(y).toBeCloseTo(2.5);
  });

  it("inspect_point", async () => {
    const out = await callViaPython(`
    result = client.inspect_point(point=Point(x=3.0, y=4.0))
    print(result)
    `);
    expect(out).toBe("Point(3.0, 4.0)");
  });

  it("echo_all_types", async () => {
    const out = await callViaPython(`
    from vgi_rpc.conformance import AllTypes, Status, Point
    data = AllTypes(
        str_field="hello",
        bytes_field=b"\\x01\\x02\\x03",
        int_field=42,
        float_field=3.14,
        bool_field=True,
        list_of_int=[1, 2, 3],
        list_of_str=["a", "b"],
        dict_field={"k": 1},
        enum_field=Status.ACTIVE,
        nested_point=Point(x=1.0, y=2.0),
        optional_str="present",
        optional_int=7,
        optional_nested=Point(x=3.0, y=4.0),
        list_of_nested=[Point(x=5.0, y=6.0)],
        annotated_int32=100,
        annotated_float32=1.5,
        nested_list=[[1, 2], [3]],
        dict_str_str={"key": "val"},
    )
    result = client.echo_all_types(data=data)
    assert result.str_field == "hello"
    assert result.int_field == 42
    assert result.bool_field is True
    assert result.optional_str == "present"
    assert result.optional_int == 7
    print("ok")
    `);
    expect(out).toBe("ok");
  });

  it("echo_bounding_box", async () => {
    const out = await callViaPython(`
    box = BoundingBox(top_left=Point(x=0.0, y=10.0), bottom_right=Point(x=10.0, y=0.0), label="test")
    result = client.echo_bounding_box(box=box)
    print(f"{result.label},{result.top_left.x},{result.top_left.y}")
    `);
    const [label, x, y] = out.split(",");
    expect(label).toBe("test");
    expect(Number(x)).toBe(0.0);
    expect(Number(y)).toBe(10.0);
  });
});
