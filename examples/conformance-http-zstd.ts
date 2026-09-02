// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { createHttpHandler } from "../src/http/index.js";
/**
 * HTTP conformance server pinned to a non-default zstd level.
 *
 * Response compression is on by default now (level 1), so this worker is no
 * longer the only compressing fixture — it exists to prove an explicitly
 * configured level still round-trips. Prints PORT:<n> on stdout so test
 * fixtures can discover the port.
 *
 * Run: bun run examples/conformance-http-zstd.ts
 */
import { protocol } from "./conformance-protocol.js";

const handler = createHttpHandler(protocol, {
  serverId: "conformance-http-zstd",
  protocolName: "ConformanceService",
  compressionLevel: 3,
});

const server = Bun.serve({ port: 0, fetch: handler });
console.log(`PORT:${server.port}`);
