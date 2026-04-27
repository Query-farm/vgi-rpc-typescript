// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Conformance HTTP server with OAuth PKCE + token-proxy enabled.
 *
 * Used by the cross-impl token-proxy conformance test to verify that the
 * TypeScript implementation produces the same wire-level behavior as the
 * Python and Go implementations for {prefix}/_oauth/token.
 *
 * Flags:
 *   --port <n>      Bind port (default: random).
 *   --idp-url <u>   OIDC issuer base URL of the mock IdP (default: http://127.0.0.1:9999).
 *   --resource <r>  Resource URL advertised in metadata (default: http://127.0.0.1:8000/vgi).
 */
import { createHttpHandler } from "../src/http/index.js";
import { protocol } from "./conformance-protocol.js";

function flag(name: string, fallback: string): string {
  const idx = process.argv.indexOf(name);
  return idx >= 0 ? (process.argv[idx + 1] ?? fallback) : fallback;
}

const port = parseInt(flag("--port", "0"), 10);
const idpUrl = flag("--idp-url", "http://127.0.0.1:9999");
const resource = flag("--resource", "http://127.0.0.1:8000/vgi");

const handler = createHttpHandler(protocol, {
  prefix: "/vgi",
  serverId: "conformance-http-pkce-ts",
  protocolName: "ConformanceService",
  authenticate: () => {
    throw new Error("authentication required");
  },
  oauthResourceMetadata: {
    resource,
    authorizationServers: [idpUrl],
    clientId: "my-client-id",
    clientSecret: "my-client-secret",
  },
});

const server = Bun.serve({ port, fetch: handler });
console.log(`PORT:${server.port}`);
