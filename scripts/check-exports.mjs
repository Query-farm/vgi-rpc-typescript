// Fails if any file named by package.json "exports" is missing after a build.
// Bun resolves the "bun" condition to src/, so a missing dist/ target is
// invisible to every Bun-based test and only surfaces on Node (issue #54).
import { existsSync, readFileSync } from "node:fs";

const pkg = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
const missing = [];
for (const [subpath, conditions] of Object.entries(pkg.exports)) {
  for (const [condition, target] of Object.entries(conditions)) {
    if (!existsSync(new URL(`../${target}`, import.meta.url))) missing.push(`${subpath} [${condition}] -> ${target}`);
  }
}
if (missing.length) {
  console.error(`package.json exports point at files that do not exist:\n  ${missing.join("\n  ")}`);
  process.exit(1);
}
console.log(`all ${Object.keys(pkg.exports).length} export subpaths resolve`);
