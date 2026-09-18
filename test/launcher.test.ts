// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * End-to-end launcher tests:  spawn the echo worker via launch(), connect
 * to the returned socket, exercise the contract.
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import * as path from "node:path";

import {
  acquireLock,
  defaultStateDir,
  launch,
  probeSocket,
  statusRows,
  tryAcquireLock,
} from "../src/launcher/index.js";

const ECHO_WORKER = path.resolve(import.meta.dir, "launcher.fixtures/echo-worker.ts");
const FULL_ACCEPT_QUEUE = path.resolve(import.meta.dir, "launcher.fixtures/full-accept-queue.py");
const NODE_LAUNCH = path.resolve(import.meta.dir, "launcher.fixtures/node-launch.ts");
const PYTHON = Bun.which("python3");
const NODE = Bun.which("node");

describe("launcher", () => {
  let stateDir: string;

  beforeEach(() => {
    stateDir = mkdtempSync(path.join(tmpdir(), "vgi-rpc-launch-test-"));
  });

  afterEach(() => {
    try {
      rmSync(stateDir, { recursive: true, force: true });
    } catch {
      // best-effort
    }
  });

  test("launch spawns worker, returns socket path that probes alive", async () => {
    const sock = await launch({
      workerArgv: ["bun", "run", ECHO_WORKER],
      idleTimeout: 2,
      stateDir,
      workerStartupTimeout: 30,
      connectTimeout: 10,
    });
    expect(sock).toMatch(/\.sock$/);
    expect(await probeSocket(sock)).toBe(true);

    // Worker self-shuts after idle timer; wait it out + a small grace.
    await new Promise((r) => setTimeout(r, 4500));
    expect(await probeSocket(sock)).toBe(false);
  }, 30000);

  test("statusRows lists the spawned worker", async () => {
    const sock = await launch({
      workerArgv: ["bun", "run", ECHO_WORKER],
      idleTimeout: 2,
      stateDir,
      workerStartupTimeout: 30,
    });
    const rows = await statusRows(stateDir);
    const ours = rows.find((r) => r.socket === sock);
    expect(ours).toBeDefined();
    expect(ours?.alive).toBe(true);
    expect(ours?.cmd).toEqual(["bun", "run", ECHO_WORKER]);
    await new Promise((r) => setTimeout(r, 4500));
  }, 30000);
});

/** A listener at a socket path that never accepts on its own, with its accept
 *  queue full -- a worker too busy to take another connection. Held by a
 *  Python helper, because Node and Bun accept every connection as it arrives. */
interface FullListener {
  /** Accept (and drop) one queued connection after `delayMs`, freeing a slot. */
  acceptOneAfter(delayMs: number): Promise<void>;
  /** Close the listener and every queued connection. */
  close(): Promise<void>;
}

async function fullListener(sock: string, options: { stale?: boolean } = {}): Promise<FullListener> {
  const proc = Bun.spawn([PYTHON as string, FULL_ACCEPT_QUEUE, sock, ...(options.stale ? ["--stale"] : [])], {
    stdin: "pipe",
    stdout: "pipe",
    stderr: "inherit",
  });
  const reader = proc.stdout.getReader();
  const decoder = new TextDecoder();
  let buffered = "";
  const nextLine = async (): Promise<string> => {
    for (;;) {
      const newline = buffered.indexOf("\n");
      if (newline >= 0) {
        const line = buffered.slice(0, newline);
        buffered = buffered.slice(newline + 1);
        return line;
      }
      const { value, done } = await reader.read();
      if (done) throw new Error(`full-accept-queue helper exited (${await proc.exited}) before answering`);
      buffered += decoder.decode(value, { stream: true });
    }
  };
  const ready = await nextLine();
  if (!ready.startsWith("READY ")) throw new Error(`full-accept-queue helper said ${JSON.stringify(ready)}`);
  return {
    async acceptOneAfter(delayMs) {
      await new Promise((r) => setTimeout(r, delayMs));
      proc.stdin.write("accept\n");
      await proc.stdin.flush();
      expect(await nextLine()).toBe("ACCEPTED");
    },
    async close() {
      try {
        proc.stdin.end();
      } catch {
        // already closed: the --stale helper exits on its own
      }
      await proc.exited;
    },
  };
}

describe.skipIf(process.platform === "win32" || !PYTHON)("a busy worker is not a dead one", () => {
  // Under a burst of connections a launched worker's accept queue fills, and a
  // probe that read the failed connect as "nothing listening" unlinked the live
  // worker's socket and spawned a duplicate -- a 32-process run of the Python
  // reference produced 64 workers for 2 commands. Mirrors the reference's
  // `test_launcher.py` cases for the same fix.
  let dir: string;
  let sock: string;

  beforeEach(() => {
    dir = mkdtempSync(path.join(tmpdir(), "vgi-busy-"));
    sock = path.join(dir, "busy.sock");
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  test("the probe counts a momentarily full accept queue as alive", async () => {
    // However the runtime reports it: Node on Linux says EAGAIN (alive at
    // once), while Bun -- and every runtime on macOS -- says ECONNREFUSED, the
    // same as no listener, which the probe re-tries before believing. The slot
    // frees before the first re-probe, so both must see a live worker.
    const busy = await fullListener(sock);
    try {
      const drained = busy.acceptOneAfter(30);
      expect(await probeSocket(sock)).toBe(true);
      await drained;
    } finally {
      await busy.close();
    }
  }, 15000);

  test("a refused socket is believed only after it is re-probed", async () => {
    // A socket file left by a dead worker refuses every connect. It is still
    // dead -- replacing it is the whole point of the probe -- but only after
    // the 50/100/200 ms re-probes, since a refusal alone may be a busy worker.
    const stale = await fullListener(sock, { stale: true });
    await stale.close();
    const started = performance.now();
    expect(await probeSocket(sock)).toBe(false);
    expect(performance.now() - started).toBeGreaterThanOrEqual(300);
  }, 15000);

  // Only a runtime that surfaces EAGAIN can tell a full queue that *stays* full
  // from no listener at all. Measured on Linux: Node does, Bun (1.4.2) reports
  // ECONNREFUSED -- so this runs `launch()` under Node, bundled from source.
  test.skipIf(process.platform !== "linux" || !NODE)(
    "launch leaves a worker with a full accept queue alone (Linux, under Node)",
    async () => {
      const busy = await fullListener(sock);
      const inode = statSync(sock).ino;
      try {
        const outdir = path.join(dir, "node-launch");
        const build = await Bun.build({
          entrypoints: [NODE_LAUNCH],
          target: "node",
          format: "esm",
          outdir,
          naming: "[name].mjs",
        });
        expect(build.success).toBe(true);
        const proc = Bun.spawn([NODE as string, path.join(outdir, "node-launch.mjs"), sock], {
          stdout: "pipe",
          stderr: "pipe",
        });
        const [exitCode, stdout, stderr] = await Promise.all([
          proc.exited,
          new Response(proc.stdout).text(),
          new Response(proc.stderr).text(),
        ]);
        expect({ exitCode, stderr }).toEqual({ exitCode: 0, stderr: "" });
        expect(JSON.parse(stdout).path).toBe(sock);
        // Not unlinked and re-bound: the same socket, the same worker.
        expect(statSync(sock).ino).toBe(inode);
      } finally {
        await busy.close();
      }
    },
    30000,
  );
});

describe("file lock", () => {
  test("acquire / release cycle", async () => {
    const tmp = mkdtempSync(path.join(tmpdir(), "vgi-lock-"));
    const lockPath = path.join(tmp, "test.lock");
    const handle = await acquireLock(lockPath, 1000);
    expect(handle.path).toBe(lockPath);
    // While held, a second non-blocking attempt fails.
    expect(tryAcquireLock(lockPath)).toBeNull();
    handle.release();
    // After release, available again.
    const second = tryAcquireLock(lockPath);
    expect(second).not.toBeNull();
    second?.release();
    rmSync(tmp, { recursive: true, force: true });
  });
});

describe("defaultStateDir", () => {
  test("returns a writable directory", () => {
    const dir = defaultStateDir();
    expect(typeof dir).toBe("string");
    expect(dir.length).toBeGreaterThan(0);
  });
});
