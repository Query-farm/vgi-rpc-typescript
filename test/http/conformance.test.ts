// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * HTTP conformance tests — verify wire compatibility with the Python CLI
 * via `vgi-rpc --url`.
 *
 * These tests start a Bun HTTP server with the conformance protocol,
 * then use the Python CLI to interact with it.
 */
import { describe, it, expect, beforeAll, afterAll, afterEach } from "bun:test";
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
import { protocol } from "../../examples/conformance-protocol.js";
import { createHttpHandler } from "../../src/http/index.js";

const VGI_CLI = "/Users/rusty/Development/vgi-rpc/.venv/bin/vgi-rpc";
const TS_DIR = "/Users/rusty/Development/vgi-rpc-typescript";
const PORT = 19876; // high port to avoid conflicts
const BASE_URL = `http://localhost:${PORT}`;

const tmpFiles: string[] = [];

function tmpFile(name: string, ext = "arrow"): string {
  const path = `/tmp/vgi-http-conf-${name}-${Date.now()}.${ext}`;
  tmpFiles.push(path);
  return path;
}

afterEach(() => {
  for (const f of tmpFiles) {
    try { unlinkSync(f); } catch {}
  }
  tmpFiles.length = 0;
});

beforeAll(async () => {
  // Write a minimal server script that imports the shared protocol
  const scriptPath = `${TS_DIR}/test/http/_conformance_server.ts`;
  await Bun.write(scriptPath, `
import { protocol } from "../../examples/conformance-protocol.js";
import { createHttpHandler } from "../../src/http/index.js";

const httpHandler = createHttpHandler(protocol, {
  prefix: "/vgi",
  serverId: "conformance-http",
});

const srv = Bun.serve({
  port: ${PORT},
  fetch: httpHandler,
});

console.log(\`HTTP conformance server listening on http://localhost:\${srv.port}\`);
`);

  // Start the subprocess server
  const proc = Bun.spawn(["bun", "run", scriptPath], {
    stdout: "pipe",
    stderr: "pipe",
  });

  // Wait for server to be ready
  const reader = proc.stdout.getReader();
  const { value } = await reader.read();
  const text = new TextDecoder().decode(value);
  if (!text.includes("listening")) {
    throw new Error(`Server didn't start: ${text}`);
  }
  reader.releaseLock();

  // Store proc for cleanup
  (globalThis as any).__conformanceProc = proc;
});

afterAll(() => {
  const proc = (globalThis as any).__conformanceProc;
  if (proc) {
    proc.kill();
  }
  // Clean up the server script
  try { unlinkSync(`${TS_DIR}/test/http/_conformance_server.ts`); } catch {}
});

// Helpers
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

function cliHttp(...extra: string[]): string[] {
  return [VGI_CLI, "--url", BASE_URL, ...extra];
}

async function callJson(method: string, ...params: string[]) {
  const { stdout, stderr, exitCode } = await run(cliHttp("call", method, ...params));
  if (exitCode !== 0) throw new Error(`exit ${exitCode}: ${stdout}\n${stderr}`);
  return JSON.parse(stdout);
}

async function callArrow(method: string, params: string[]): Promise<Table> {
  const outFile = tmpFile(method);
  const { exitCode, stdout, stderr } = await run(
    cliHttp("--format", "arrow", "-o", outFile, "call", method, ...params),
  );
  if (exitCode !== 0) throw new Error(`exit ${exitCode}: ${stdout}\n${stderr}`);
  const buf = await Bun.file(outFile).arrayBuffer();
  const reader = await RecordBatchReader.from(new Uint8Array(buf));
  return new Table(reader.readAll());
}

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
  const buf = writer.toUint8Array(true);
  const path = tmpFile("exchange-input");
  await Bun.write(path, buf);
  return path;
}

// ==========================================================================
// Tests
// ==========================================================================

describe("HTTP conformance: describe", () => {
  it("lists all 46 methods via HTTP", async () => {
    const { stdout, exitCode, stderr } = await run(cliHttp("describe"));
    if (exitCode !== 0) throw new Error(`exit ${exitCode}: ${stdout}\n${stderr}`);
    const data = JSON.parse(stdout);
    expect(data.protocol_name).toBe("Conformance");
    expect(Object.keys(data.methods).length).toBe(46);
  });
});

describe("HTTP conformance: unary", () => {
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

  it("add_floats", async () => {
    const r = await callJson("add_floats", "a=3.0", "b=4.0");
    expect(r.result).toBe(7.0);
  });

  it("concatenate with default separator", async () => {
    const r = await callJson("concatenate", "prefix=hello", "suffix=world");
    expect(r.result).toBe("hello-world");
  });

  it("void_noop", async () => {
    const { stdout, exitCode, stderr } = await run(cliHttp("call", "void_noop"));
    if (exitCode !== 0) throw new Error(`exit ${exitCode}: ${stdout}\n${stderr}`);
    expect(stdout).toBe("null");
  });

  it("raise_value_error", async () => {
    const { stdout, stderr, exitCode } = await run(
      cliHttp("call", "raise_value_error", "message=boom"),
    );
    expect(exitCode).not.toBe(0);
    const combined = stdout + stderr;
    expect(combined).toContain("boom");
  });
});

describe("HTTP conformance: producer streams", () => {
  it("produce_n with 5 batches", async () => {
    const table = await callArrow("produce_n", ["count=5"]);
    expect(table.numRows).toBe(5);
    for (let i = 0; i < 5; i++) {
      expect(Number(table.getChildAt(0)!.get(i))).toBe(i);
      expect(Number(table.getChildAt(1)!.get(i))).toBe(i * 10);
    }
  });

  it("produce_empty", async () => {
    const table = await callArrow("produce_empty", []);
    expect(table.numRows).toBe(0);
  });

  it("produce_single", async () => {
    const table = await callArrow("produce_single", []);
    expect(table.numRows).toBe(1);
  });

  it("produce_error_on_init", async () => {
    const { stderr, exitCode } = await run(
      cliHttp("call", "produce_error_on_init"),
    );
    expect(exitCode).toBe(1);
    expect(stderr).toContain("intentional init error");
  });
});

describe("HTTP conformance: exchange streams", () => {
  it("exchange_scale", async () => {
    const inputFile = await writeExchangeInput([5.0, 10.0]);
    const outFile = tmpFile("exchange-out");
    const { exitCode, stdout, stderr } = await run(
      cliHttp("--format", "arrow", "-o", outFile, "call", "exchange_scale", "factor=2.0", "--input", inputFile),
    );
    if (exitCode !== 0) throw new Error(`exit ${exitCode}: ${stdout}\n${stderr}`);
    const buf = await Bun.file(outFile).arrayBuffer();
    const reader = await RecordBatchReader.from(new Uint8Array(buf));
    const table = new Table(reader.readAll());
    expect(table.numRows).toBe(2);
    expect(table.getChildAt(0)!.get(0)).toBe(10.0);
    expect(table.getChildAt(0)!.get(1)).toBe(20.0);
  });

  it("exchange_accumulate", async () => {
    const inputFile = await writeExchangeInput([1.0, 2.0, 3.0]);
    const outFile = tmpFile("accum-out");
    const { exitCode, stdout, stderr } = await run(
      cliHttp("--format", "arrow", "-o", outFile, "call", "exchange_accumulate", "--input", inputFile),
    );
    if (exitCode !== 0) throw new Error(`exit ${exitCode}: ${stdout}\n${stderr}`);
    const buf = await Bun.file(outFile).arrayBuffer();
    const reader = await RecordBatchReader.from(new Uint8Array(buf));
    const table = new Table(reader.readAll());
    expect(table.numRows).toBe(3);
    const sums = table.getChildAt(0)!;
    expect(sums.get(0)).toBe(1.0);
    expect(sums.get(1)).toBe(3.0);
    expect(sums.get(2)).toBe(6.0);
  });

  it("exchange_error_on_init", async () => {
    const inputFile = await writeExchangeInput([1.0]);
    const { stderr, exitCode } = await run(
      cliHttp("call", "exchange_error_on_init", "--input", inputFile),
    );
    expect(exitCode).toBe(1);
    expect(stderr).toContain("intentional exchange init error");
  });
});
