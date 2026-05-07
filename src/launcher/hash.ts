// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Hash a worker tuple (argv + cwd + filtered env) into a deterministic
 * 16-hex-character identifier.
 *
 * Cross-language contract — must match Python's `vgi_rpc.launcher.compute_hash`
 * byte-for-byte so the same worker tuple resolves to the same socket path
 * regardless of which language's launcher discovered it first.  The
 * canonical form is:
 *
 * ```python
 * canonical = {
 *     "cmd": list(worker_argv),
 *     "cwd": cwd if cwd is not None else os.getcwd(),
 *     "env": {k: v for k, v in sorted(os.environ.items()) if k.startswith("VGI_RPC_")},
 * }
 * payload = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
 * sha256(payload).hexdigest()[:16]
 * ```
 *
 * `scripts/regenerate_launcher_parity_vectors.py` in vgi-rpc-python emits a
 * golden vector table; the parity test in `test/launcher.hash.test.ts`
 * asserts byte equality against it.
 */

const HASH_LEN = 16;

/** Recursively stringify with sorted object keys and `,`/`:` separators —
 *  the JS equivalent of `json.dumps(..., sort_keys=True, separators=(",",":"))`.
 *  We can't reuse `JSON.stringify` because the V8 implementation preserves
 *  insertion order rather than sorting. */
function canonicalJson(value: unknown): string {
  if (value === null) return "null";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "number") {
    // Python json emits integers without a trailing `.0` and floats with the
    // shortest round-trippable form. JS `JSON.stringify` matches this for
    // both integer-valued and finite floats; `Infinity`/`NaN` would diverge
    // (Python raises) but they shouldn't occur in launcher payloads.
    return JSON.stringify(value);
  }
  if (typeof value === "string") return JSON.stringify(value);
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJson).join(",")}]`;
  }
  if (typeof value === "object") {
    const keys = Object.keys(value as Record<string, unknown>).sort();
    const parts = keys.map((k) => `${JSON.stringify(k)}:${canonicalJson((value as Record<string, unknown>)[k])}`);
    return `{${parts.join(",")}}`;
  }
  throw new TypeError(`canonicalJson: unsupported type ${typeof value}`);
}

async function sha256Hex(data: Uint8Array): Promise<string> {
  // Web Crypto digest accepts BufferSource; copy into a fresh ArrayBuffer
  // to dodge SharedArrayBuffer constraints in some runtimes.
  const buf = new ArrayBuffer(data.byteLength);
  new Uint8Array(buf).set(data);
  const digest = await crypto.subtle.digest("SHA-256", buf);
  return Array.from(new Uint8Array(digest))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

/**
 * Compute the 16-hex-char tuple hash for a worker.
 *
 * @param workerArgv The worker command and its arguments.
 * @param cwd        Working directory; defaults to `process.cwd()`.
 * @param env        Process environment; defaults to `process.env`.  Only
 *                   keys starting with `VGI_RPC_` participate in the hash —
 *                   workers that differ only in unrelated env (PATH,
 *                   HOME, …) intentionally share a worker.
 */
export async function computeHash(
  workerArgv: readonly string[],
  cwd?: string,
  env?: Record<string, string | undefined>,
): Promise<string> {
  const cwdValue = cwd !== undefined ? cwd : process.cwd();
  const sourceEnv = env ?? (process.env as Record<string, string | undefined>);

  const filteredEnv: Record<string, string> = {};
  for (const key of Object.keys(sourceEnv)) {
    if (key.startsWith("VGI_RPC_")) {
      const v = sourceEnv[key];
      if (v !== undefined) filteredEnv[key] = v;
    }
  }

  const canonical = {
    cmd: [...workerArgv],
    cwd: cwdValue,
    env: filteredEnv,
  };
  const payload = new TextEncoder().encode(canonicalJson(canonical));
  const hex = await sha256Hex(payload);
  return hex.slice(0, HASH_LEN);
}

/** Exposed for parity-test fixtures that need to inspect the canonical form. */
export const _internal = { canonicalJson };
