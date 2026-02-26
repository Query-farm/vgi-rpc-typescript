// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * HTTP conformance server — serves the conformance protocol over HTTP.
 * Prints PORT:<n> on stdout so test fixtures can discover the port.
 *
 * Run: bun run examples/conformance-http.ts
 */
import { protocol } from "./conformance-protocol.js";
import { createHttpHandler } from "../src/http/index.js";

const handler = createHttpHandler(protocol, {
  prefix: "/vgi",
  serverId: "conformance-http",
});

const server = Bun.serve({ port: 0, fetch: handler });
console.log(`PORT:${server.port}`);
