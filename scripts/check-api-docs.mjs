#!/usr/bin/env node
// API documentation coverage ratchet.
//
// Runs TypeDoc over the public entry point (src/index.ts) with notDocumented
// validation and counts exported symbols that lack a JSDoc comment. Fails if
// the count exceeds BASELINE — so coverage can only hold steady or improve,
// never regress. As JSDoc is backfilled, lower BASELINE to lock in the gain
// (the script tells you the new number to set).
//
// Run via `make docs-api-coverage`. Requires root node_modules (TypeDoc
// resolves the source graph through it) and docs/node_modules (the TypeDoc
// CLI + markdown plugin live in the docs workspace).

import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

// Every public export is currently documented, so the baseline is 0: any new
// undocumented public export fails this gate. If you must add an exception,
// raise this number deliberately (and ideally open a follow-up to document it).
const BASELINE = Number(process.env.API_DOC_BASELINE ?? 0);

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const docsDir = resolve(repoRoot, "docs");

const result = spawnSync(
  "npx",
  [
    "typedoc",
    "--plugin",
    "typedoc-plugin-markdown",
    "--tsconfig",
    "../tsconfig.json",
    "--entryPoints",
    "../src/index.ts",
    "--excludeInternal",
    "--emit",
    "none",
    "--validation.notDocumented",
    "true",
    // Public types legitimately reference internal helper types we do not
    // publish as standalone pages; that is not a coverage problem.
    "--validation.notExported",
    "false",
    "--validation.invalidLink",
    "true",
  ],
  { cwd: docsDir, encoding: "utf8" },
);

const output = `${result.stdout ?? ""}${result.stderr ?? ""}`;
if (result.status !== 0 && !output.includes("does not have any documentation")) {
  // TypeDoc itself failed (resolution/config error), not a coverage warning.
  process.stderr.write(output);
  process.stderr.write("\nTypeDoc failed to run — see output above.\n");
  process.exit(2);
}

const undocumented = output
  .split("\n")
  .filter((line) => line.includes("does not have any documentation"));
const count = undocumented.length;

process.stdout.write(
  `Undocumented public API symbols: ${count} (baseline: ${BASELINE})\n`,
);

if (count > BASELINE) {
  process.stderr.write(
    `\nERROR: API documentation coverage regressed by ${count - BASELINE}. ` +
      "Add a JSDoc comment to each new public export below, or raise the " +
      "baseline only if intentional:\n\n",
  );
  for (const line of undocumented) {
    process.stderr.write(`${line.replace(/\x1b\[[0-9;]*m/g, "")}\n`);
  }
  process.exit(1);
}

if (count < BASELINE) {
  process.stdout.write(
    `\nCoverage improved by ${BASELINE - count}. Set API_DOC_BASELINE ` +
      `(scripts/check-api-docs.mjs) to ${count} to lock it in.\n`,
  );
}

process.exit(0);
