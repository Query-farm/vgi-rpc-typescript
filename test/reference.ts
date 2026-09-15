// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Where the Python reference implementation lives, for the tests that need to
 * talk to one.
 *
 * There are two Python checkouts on a typical development machine and they are
 * not the same implementation:
 *
 * - `vgi-rpc` is `main`. Flat routes, no routing key, `__describe__` still
 *   live — none of the multiservice work.
 * - `vgi-rpc-python` (branch `multiservice/pr1-internal`) is the canonical
 *   reference for that work.
 *
 * The version numbers actively mislead: the stale tree is the numerically
 * higher one. Pointing a harness at it means testing against a server that
 * 404s every namespaced path this client knows how to build, so the failures
 * it produces say "wrong reference", not "wrong client" — and they are easy to
 * misread as pre-existing breakage, which is what happened across several
 * ports.
 *
 * This resolves in three steps so no committed file has to carry a
 * machine-specific absolute path (the reason the previous pin went unnoticed
 * for so long — nothing about `/Users/rusty/Development/vgi-rpc/...` sitting
 * in a test file announces that it has gone stale):
 *
 *   1. the explicit environment override, which always wins;
 *   2. the reference checkout's virtualenv, located relative to `$HOME`;
 *   3. whatever is on `PATH`, so CI — which installs the package rather than
 *      checking it out — needs no configuration at all.
 */

import { existsSync } from "node:fs";
import { join } from "node:path";

/** Root of the canonical reference checkout. Override with
 *  `VGI_RPC_PYTHON_HOME` to test against a checkout somewhere else. */
export const REFERENCE_HOME: string =
  process.env.VGI_RPC_PYTHON_HOME ?? join(process.env.HOME ?? "", "Development", "vgi-rpc-python");

/** A binary from the reference checkout's venv, or `undefined` if absent. */
function referenceBin(name: string): string | undefined {
  if (!REFERENCE_HOME) return undefined;
  const path = join(REFERENCE_HOME, ".venv", "bin", name);
  return existsSync(path) ? path : undefined;
}

/** Python interpreter with `vgi_rpc` importable.
 *  Override: `VGI_RPC_PYTHON_BIN`. */
export const PYTHON_BIN: string = process.env.VGI_RPC_PYTHON_BIN ?? referenceBin("python") ?? "python3";

/** The `vgi-rpc` CLI. Override: `VGI_RPC_CLI`. */
export const VGI_CLI: string = process.env.VGI_RPC_CLI ?? referenceBin("vgi-rpc") ?? "vgi-rpc";
