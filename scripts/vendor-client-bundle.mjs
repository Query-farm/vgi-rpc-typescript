// Regenerates src/http/client-bundle.ts from the shared VGI JS client build.
// Usage: node scripts/vendor-client-bundle.mjs [path-to-vgi-client.js]
//
// Pair this with scripts/vendor-landing-html.mjs: the page imports the bundle,
// so the two are vendored and released together.
import { readFileSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const src = process.argv[2] ?? join(homedir(), "Development/vgi-web-frontend/public/vgi-client.js");
const bytes = readFileSync(src);
const b64 = bytes.toString("base64");
const header =
  "// © Copyright 2025-2026, Query.Farm LLC - https://query.farm\n" +
  "// SPDX-License-Identifier: Apache-2.0\n//\n" +
  "// VENDORED ASSET — DO NOT EDIT BY HAND.\n" +
  "// Browser build of the @query-farm/vgi client, authored in\n" +
  "// vgi-web-frontend (`bun run build:landing-client`). The shared landing\n" +
  "// page imports it from {prefix}/vgi-client.js to read catalog metadata\n" +
  "// over the VGI protocol. Base64-embedded so it survives single-file\n" +
  "// bundling (bun build) unchanged and is served as raw bytes.\n" +
  "// Regenerate with scripts/vendor-client-bundle.mjs.\n\n";
const body =
  `const CLIENT_BUNDLE_B64 =\n  "${b64}";\n\n` +
  "function decodeBase64(b64: string): Uint8Array {\n" +
  "  // atob is available on Node >=16, Bun, Deno, and workerd.\n" +
  "  const bin = atob(b64);\n" +
  "  const out = new Uint8Array(bin.length);\n" +
  "  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);\n" +
  "  return out;\n}\n\n" +
  "/** The vendored VGI browser client bundle, decoded to its raw bytes. */\n" +
  "export const CLIENT_BUNDLE_BYTES: Uint8Array = decodeBase64(CLIENT_BUNDLE_B64);\n";
writeFileSync(new URL("../src/http/client-bundle.ts", import.meta.url), header + body);
console.log(`vendored ${bytes.length} bytes from ${src}`);
