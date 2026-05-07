// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Cross-language parity vectors for the launcher hash.
 *
 * Each entry pins the byte-identical hash that
 * `vgi_rpc.launcher.compute_hash()` produces for the given (argv, cwd,
 * env) tuple.  Re-run
 * `~/Development/vgi-rpc/scripts/regenerate_launcher_parity_vectors.py`
 * to refresh both this file and the C++ launcher's vectors when the
 * canonical form changes.
 */

import { describe, expect, test } from "bun:test";
import { computeHash } from "../src/launcher/hash.js";

interface Vector {
  name: string;
  argv: string[];
  cwd: string;
  env: Record<string, string>;
  expected: string;
}

const VECTORS: Vector[] = [
  { name: "empty_argv_empty_env", argv: [], cwd: "/tmp", env: {}, expected: "21499d847854c192" },
  { name: "single_arg", argv: ["python"], cwd: "/tmp", env: {}, expected: "13ddf92fa852a381" },
  {
    name: "many_args",
    argv: ["python", "-m", "foo", "--bar", "baz"],
    cwd: "/tmp",
    env: {},
    expected: "1d95f2117bce8c2d",
  },
  {
    name: "argv_with_spaces",
    argv: ["python", "/path with spaces/foo.py"],
    cwd: "/tmp",
    env: {},
    expected: "23664770f5414889",
  },
  {
    name: "cwd_with_special_chars",
    argv: ["python"],
    cwd: '/tmp/has spaces and "quotes"',
    env: {},
    expected: "e87a8168b8665401",
  },
  { name: "env_single", argv: ["python"], cwd: "/tmp", env: { VGI_RPC_FOO: "bar" }, expected: "70118f0ad5ea8bf3" },
  {
    name: "env_multiple_sorted_by_python",
    argv: ["python"],
    cwd: "/tmp",
    env: { VGI_RPC_Z: "z", VGI_RPC_A: "a", VGI_RPC_M: "m" },
    expected: "1000503273c593e4",
  },
  {
    name: "env_with_quotes_and_backslash",
    argv: ["python"],
    cwd: "/tmp",
    env: { VGI_RPC_FOO: 'a"b\\c' },
    expected: "f688dc41e1a4416d",
  },
  {
    name: "env_value_with_spaces",
    argv: ["python"],
    cwd: "/tmp",
    env: { VGI_RPC_FLAG: "value with spaces" },
    expected: "48522da323b1a55d",
  },
  {
    name: "argv_with_quotes_and_backslash",
    argv: ["echo", 'a"b\\c'],
    cwd: "/tmp",
    env: {},
    expected: "cfcf140ab2f01b74",
  },
  {
    name: "long_path",
    argv: ["/usr/local/bin/very/long/path/to/the/worker/executable"],
    cwd: "/tmp",
    env: {},
    expected: "b6f2736f279afd0b",
  },
  {
    name: "deep_cwd",
    argv: ["python"],
    cwd: "/var/folders/5z/abcdefghijklmnop/T/working/directory/deep/nesting",
    env: {},
    expected: "a37badbdf41d0559",
  },
  {
    name: "many_args_many_env",
    argv: ["java", "-jar", "/opt/foo.jar", "-Dlog.level=INFO"],
    cwd: "/var/folders/work",
    env: { VGI_RPC_TOKEN: "secret", VGI_RPC_REGION: "us-west-2", VGI_RPC_BUCKET: "my-bucket" },
    expected: "8abb635d646af180",
  },
];

describe("launcher computeHash parity with Python", () => {
  for (const v of VECTORS) {
    test(v.name, async () => {
      const got = await computeHash(v.argv, v.cwd, v.env);
      expect(got).toBe(v.expected);
    });
  }

  test("uses process.cwd() when cwd omitted", async () => {
    const cwd = process.cwd();
    const a = await computeHash(["python"], cwd);
    const b = await computeHash(["python"]);
    expect(b).toBe(a);
  });

  test("filters env to VGI_RPC_* keys only", async () => {
    const baseEnv = { VGI_RPC_FOO: "bar" };
    const noisyEnv = { ...baseEnv, PATH: "/usr/bin", HOME: "/home/whoever", LC_ALL: "C" };
    const a = await computeHash(["python"], "/tmp", baseEnv);
    const b = await computeHash(["python"], "/tmp", noisyEnv);
    expect(b).toBe(a);
  });
});
