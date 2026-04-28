// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Conformance HTTP server with a reject-all authenticate callback.
 *
 * Used by the TestHealth conformance suite to verify that GET /health is
 * exempt from authentication: every RPC endpoint returns 401, but the
 * health probe must still succeed.
 */
import { createHttpHandler } from "../src/http/index.js";
import { protocol } from "./conformance-protocol.js";

const portArg = process.argv.indexOf("--port");
const port = portArg >= 0 ? parseInt(process.argv[portArg + 1] ?? "0", 10) : 0;

const handler = createHttpHandler(protocol, {
  serverId: "conformance-http-auth",
  protocolName: "ConformanceService",
  authenticate: () => {
    throw new Error("authentication required");
  },
});

const server = Bun.serve({ port, fetch: handler });
console.log(`PORT:${server.port}`);
