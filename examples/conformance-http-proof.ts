// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Conformance HTTP server gated on proxy proof.
 *
 * Backs the shared `TestProxyProof` group, which mints tokens with the Python
 * reference implementation and posts them here — so a canonical string framed
 * differently than the spec fails, which round-tripping inside one
 * implementation could never reveal.
 *
 * Accepts the reference worker CLI shared by every port:
 *
 *   --port <n>
 *   --proof-secrets <kid>:<hex>,...
 *   --proof-origin-id <id>
 *   [--proof-mode off|allow|require]   (default: require)
 *   [--proof-skew <seconds>]           (default: 30)
 *   [--proof-no-replay-cache]
 *
 * Run: bun run examples/conformance-http-proof.ts --proof-secrets k:$(...)
 */
import { createHttpHandler, type ProofMode, parseProofSecrets, requireProxyProof } from "../src/http/index.js";
import { protocol } from "./conformance-protocol.js";

const args = process.argv.slice(2);
let port = 0;
let mode: ProofMode = "require";
let originId = "conformance-origin";
let secretSpec = "";
let skewSeconds = 30;
let replayCache = true;

for (let i = 0; i < args.length; i++) {
  const a = args[i];
  if (a === "--port" && i + 1 < args.length) {
    port = Number.parseInt(args[++i], 10);
  } else if (a === "--proof-mode" && i + 1 < args.length) {
    mode = args[++i] as ProofMode;
  } else if (a === "--proof-origin-id" && i + 1 < args.length) {
    originId = args[++i];
  } else if (a === "--proof-secrets" && i + 1 < args.length) {
    secretSpec = args[++i];
  } else if (a === "--proof-skew" && i + 1 < args.length) {
    skewSeconds = Number.parseInt(args[++i], 10);
  } else if (a === "--proof-no-replay-cache") {
    replayCache = false;
  }
}

// `off` means the feature is not installed at all, not a gate that always
// passes: an unconfigured worker must behave exactly as if the module did not
// exist, which is what TestProxyProofOffMode pins down.
const authenticate =
  mode === "off"
    ? undefined
    : requireProxyProof({
        mode,
        originId,
        secrets: parseProofSecrets(secretSpec),
        skewSeconds,
        replayCache,
      });

const handler = createHttpHandler(protocol, {
  serverId: "conformance-http-proof",
  protocolName: "ConformanceService",
  prefix: "/vgi",
  authenticate,
  // The gate is opaque to the handler, so the posture is declared alongside it.
  proxyProofRequired: mode === "require",
});

const server = Bun.serve({ port, fetch: handler });
console.log(`PORT:${server.port}`);
