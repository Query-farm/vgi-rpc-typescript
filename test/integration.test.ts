import { describe, it, expect, afterEach } from "bun:test";
import { RecordBatchReader, Table } from "apache-arrow";
import { unlinkSync } from "node:fs";

const VGI_CLI = "/Users/rusty/Development/vgi-rpc/.venv/bin/vgi-rpc";
const TS_DIR = "/Users/rusty/Development/vgi-rpc-typescript";

const tmpFiles: string[] = [];

function tmpArrow(name: string): string {
  const path = `/tmp/vgi-rpc-test-${name}-${Date.now()}.arrow`;
  tmpFiles.push(path);
  return path;
}

afterEach(() => {
  for (const f of tmpFiles) {
    try {
      unlinkSync(f);
    } catch {}
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

function cliArgs(example: string, ...extra: string[]): string[] {
  const cmd = `bun run ${TS_DIR}/examples/${example}.ts`;
  return [VGI_CLI, "--cmd", cmd, ...extra];
}

/** Run a CLI call with --format arrow -o, read back the Arrow IPC file. */
async function callArrow(
  example: string,
  method: string,
  params: string[],
): Promise<{ table: import("apache-arrow").Table; exitCode: number }> {
  const outFile = tmpArrow(`${example}-${method}`);
  const { exitCode, stdout, stderr } = await run(
    cliArgs(example, "--format", "arrow", "-o", outFile, "call", method, ...params),
  );
  if (exitCode !== 0) {
    throw new Error(
      `CLI exited ${exitCode}: ${stdout} ${stderr}`,
    );
  }
  const bytes = await Bun.file(outFile).arrayBuffer();
  const reader = await RecordBatchReader.from(new Uint8Array(bytes));
  const batches = reader.readAll();
  const table = new Table(batches);
  return { table, exitCode };
}

describe("integration: describe", () => {
  it("describes the calculator service", async () => {
    const { stdout, exitCode } = await run(
      cliArgs("calculator", "describe"),
    );
    expect(exitCode).toBe(0);
    const data = JSON.parse(stdout);
    expect(data.protocol_name).toBe("Calculator");
    expect(data.describe_version).toBe("2");
    expect(data.methods.add).toBeTruthy();
    expect(data.methods.multiply).toBeTruthy();
    expect(data.methods.divide).toBeTruthy();
    expect(data.methods.add.method_type).toBe("unary");
  });

  it("describes the greeter service", async () => {
    const { stdout, exitCode } = await run(
      cliArgs("greeter", "describe"),
    );
    expect(exitCode).toBe(0);
    const data = JSON.parse(stdout);
    expect(data.protocol_name).toBe("Greeter");
    expect(data.methods.greet).toBeTruthy();
    expect(data.methods.add).toBeTruthy();
  });

  it("describes the streaming service", async () => {
    const { stdout, exitCode } = await run(
      cliArgs("streaming", "describe"),
    );
    expect(exitCode).toBe(0);
    const data = JSON.parse(stdout);
    expect(data.protocol_name).toBe("Streaming");
    expect(data.methods.count).toBeTruthy();
    expect(data.methods.count.method_type).toBe("stream");
    expect(data.methods.scale).toBeTruthy();
    expect(data.methods.scale.method_type).toBe("stream");
  });
});

describe("integration: unary calls", () => {
  it("calls add(3, 4) = 7", async () => {
    const { table } = await callArrow("calculator", "add", ["a=3.0", "b=4.0"]);
    expect(table.numRows).toBe(1);
    expect(table.getChildAt(0)?.get(0)).toBe(7.0);
  });

  it("calls multiply(3, 7) = 21", async () => {
    const { table } = await callArrow("calculator", "multiply", ["a=3.0", "b=7.0"]);
    expect(table.numRows).toBe(1);
    expect(table.getChildAt(0)?.get(0)).toBe(21.0);
  });

  it("calls greet(World) = Hello, World!", async () => {
    const { table } = await callArrow("greeter", "greet", ["name=World"]);
    expect(table.numRows).toBe(1);
    expect(table.getChildAt(0)?.get(0)).toBe("Hello, World!");
  });

  it("propagates division by zero error", async () => {
    const { stdout, stderr, exitCode } = await run(
      cliArgs("calculator", "--format", "arrow", "call", "divide", "a=1.0", "b=0.0"),
    );
    expect(exitCode).toBe(1);
    const combined = stdout + stderr;
    expect(combined).toContain("Division by zero");
  });
});

describe("integration: streaming calls", () => {
  it("counts from 0 to 2", async () => {
    const { table } = await callArrow("streaming", "count", ["limit=3"]);
    expect(table.numRows).toBe(3);
    const n = table.getChildAt(0)!;
    const sq = table.getChildAt(1)!;
    expect(n.get(0)).toBe(0);
    expect(n.get(1)).toBe(1);
    expect(n.get(2)).toBe(2);
    expect(sq.get(0)).toBe(0);
    expect(sq.get(1)).toBe(1);
    expect(sq.get(2)).toBe(4);
  });

  it("counts from 0 to 4", async () => {
    const { table } = await callArrow("streaming", "count", ["limit=5"]);
    expect(table.numRows).toBe(5);
    const n = table.getChildAt(0)!;
    const sq = table.getChildAt(1)!;
    expect(n.get(4)).toBe(4);
    expect(sq.get(4)).toBe(16);
  });

  it("streams 100000 rows in batches of 1000", async () => {
    const { table } = await callArrow("streaming", "count", [
      "limit=100000",
      "batch_size=1000",
    ]);
    expect(table.numRows).toBe(100000);

    const n = table.getChildAt(0)!;
    const sq = table.getChildAt(1)!;

    // First row
    expect(n.get(0)).toBe(0);
    expect(sq.get(0)).toBe(0);

    // Batch boundary (row 999/1000)
    expect(n.get(999)).toBe(999);
    expect(sq.get(999)).toBe(999 * 999);
    expect(n.get(1000)).toBe(1000);
    expect(sq.get(1000)).toBe(1000000);

    // Mid-stream
    expect(n.get(10000)).toBe(10000);
    expect(sq.get(10000)).toBe(100000000);

    // Last row
    expect(n.get(99999)).toBe(99999);

    // Spot-check sequential n values
    for (let i = 0; i < 100000; i += 10000) {
      expect(n.get(i)).toBe(i);
    }
  });
});
