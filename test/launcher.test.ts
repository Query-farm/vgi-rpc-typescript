// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * End-to-end launcher tests:  spawn the echo worker via launch(), connect
 * to the returned socket, exercise the contract.
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
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
