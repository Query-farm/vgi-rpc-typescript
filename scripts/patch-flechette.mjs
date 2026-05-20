#!/usr/bin/env bun
// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Apply local patches to `node_modules/@uwdata/flechette`. The
 * `scripts/flechette-patches/src/...` tree holds the modified source
 * files; this script copies them over after every `bun install` so
 * the changes survive package updates. Also rewrites flechette's
 * `package.json` to point `main` at the patched `src/index.js` (it
 * otherwise prefers the pre-built `dist/flechette.cjs` bundle, which
 * our patches don't touch).
 *
 * What the patches add:
 *
 * * Per-record-batch `custom_metadata` support (Arrow IPC Message
 *   field 4) for both encoder + decoder. Upstream flechette doesn't
 *   encode or decode this field, so vgi-rpc's per-call wire metadata
 *   (`vgi_rpc.log_level`, `server_id`, `request_id`, etc.) is
 *   silently dropped without these patches.
 *
 * * Multi-batch positional metadata ordering. The naive
 *   `concatTables → tableToIPC` path drops empty-data tables (their
 *   Columns contribute zero batches), so when a vgi-rpc log batch
 *   precedes a result batch, batch positions and metadata indices
 *   scramble. We replace the multi-table path with an explicit
 *   per-table walk that preserves ordering.
 *
 * * Empty-record-batch synthesis for the
 *   metadata-only-with-non-empty-schema shape (vgi-rpc EXCEPTION
 *   batches on a typed result schema).
 *
 * These changes belong upstream in the Query-farm/flechette fork
 * eventually; this directory + script is the in-tree transition.
 *
 * Idempotent: running twice copies the same files over again, no-op.
 */

import { copyFileSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";

// The patches in `flechette-patches/` are hand-merged against this exact
// upstream version. flechette is pinned to a github fork branch, so a
// dependency bump can change the source these patches overwrite without
// changing how `bun install` resolves it. If the installed version drifts,
// the patched files may no longer line up with the rest of the package
// (e.g. a renamed internal export) and the flechette backend breaks in
// subtle, hard-to-trace ways. Bump this constant whenever the patches are
// re-merged against a new flechette release.
const EXPECTED_FLECHETTE_VERSION = "2.4.0";

const ROOT = resolve(import.meta.dirname, "..", "node_modules", "@uwdata", "flechette");
if (!existsSync(ROOT)) {
  // Flechette not installed (fresh clone before `bun install`). Skip
  // silently — postinstall fires again after the install completes.
  process.exit(0);
}

const installedPkg = JSON.parse(readFileSync(resolve(ROOT, "package.json"), "utf8"));
if (installedPkg.version !== EXPECTED_FLECHETTE_VERSION) {
  console.error(
    `patch-flechette: WARNING — installed @uwdata/flechette is ` +
      `${installedPkg.version}, but the patches in scripts/flechette-patches/ ` +
      `were merged against ${EXPECTED_FLECHETTE_VERSION}. They may not apply ` +
      `correctly. Re-merge the patches and update EXPECTED_FLECHETTE_VERSION ` +
      `in scripts/patch-flechette.mjs.`,
  );
}

const PATCHED_FILES = [
  "src/encode/message.js",
  "src/encode/encode-ipc.js",
  "src/encode/record-batch.js",
  "src/encode/table-to-ipc.js",
  "src/encode/tables-to-ipc.js",
  "src/decode/message.js",
  "src/decode/table-from-ipc.js",
];

const PATCH_SRC = resolve(import.meta.dirname, "flechette-patches");

let copied = 0;
for (const file of PATCHED_FILES) {
  const from = resolve(PATCH_SRC, file);
  const to = resolve(ROOT, file);
  if (!existsSync(from)) {
    console.error(`patch-flechette: missing source ${from}`);
    continue;
  }
  if (!existsSync(dirname(to))) {
    console.error(`patch-flechette: missing target dir ${dirname(to)}`);
    continue;
  }
  copyFileSync(from, to);
  copied++;
}

// Rewrite package.json `main` to point at the patched src tree. Bun's
// default ESM resolution otherwise picks `module: src/index.js` but
// some `require`-style or CJS paths still pick `main: dist/flechette.cjs`,
// which doesn't carry our edits.
const pkgPath = resolve(ROOT, "package.json");
const pkg = JSON.parse(readFileSync(pkgPath, "utf8"));
if (pkg.main !== "./src/index.js") {
  pkg.main = "./src/index.js";
  writeFileSync(pkgPath, `${JSON.stringify(pkg, null, 2)}\n`);
}

if (copied > 0) {
  console.log(`patch-flechette: applied ${copied} source patches`);
}
