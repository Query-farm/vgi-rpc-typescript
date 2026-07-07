// Regenerates src/http/landing-html.ts from the shared landing page.
// Usage: node scripts/vendor-landing-html.mjs [path-to-landing.html]
import { readFileSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const src = process.argv[2] ?? join(homedir(), "Development/vgi-web-frontend/public/landing.html");
const bytes = readFileSync(src);
const b64 = bytes.toString("base64");
const header =
  "// © Copyright 2025-2026, Query.Farm LLC - https://query.farm\n" +
  "// SPDX-License-Identifier: Apache-2.0\n//\n" +
  "// VENDORED ASSET — DO NOT EDIT BY HAND.\n" +
  "// Byte-identical copy of the shared VGI landing page authored in\n" +
  "// vgi-web-frontend/public/landing.html and served by every VGI worker\n" +
  "// (see vgi/docs/http-landing-contract.md). The page is base64-embedded so\n" +
  "// it survives single-file bundling (bun build) unchanged and is served as\n" +
  "// raw bytes. Regenerate with scripts/vendor-landing-html.mjs.\n\n";
const body =
  `const LANDING_HTML_B64 =\n  "${b64}";\n\n` +
  "function decodeBase64(b64: string): Uint8Array {\n" +
  "  // atob is available on Node >=16, Bun, Deno, and workerd.\n" +
  "  const bin = atob(b64);\n" +
  "  const out = new Uint8Array(bin.length);\n" +
  "  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);\n" +
  "  return out;\n}\n\n" +
  "/** The vendored shared landing page, decoded to its raw UTF-8 bytes. */\n" +
  "export const LANDING_HTML_BYTES: Uint8Array = decodeBase64(LANDING_HTML_B64);\n";
writeFileSync(new URL("../src/http/landing-html.ts", import.meta.url), header + body);
console.log(`vendored ${bytes.length} bytes from ${src}`);
