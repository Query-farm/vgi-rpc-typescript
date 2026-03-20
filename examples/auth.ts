// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// HTTP authentication with Bearer tokens and guarded methods.
//
// Demonstrates how to wire up an `authenticate` callback, access
// `AuthContext` in method handlers, and gate access with
// `requireAuthenticated()`.
//
// Run:
//   bun run examples/auth.ts
//
// Test with the vgi-rpc CLI:
//   vgi-rpc --url http://localhost:8080 call status
//   vgi-rpc --url http://localhost:8080 -H "Authorization: Bearer alice" call whoami
//   vgi-rpc --url http://localhost:8080 -H "Authorization: Bearer alice" call secret_data

import { AuthContext, bearerAuthenticate, type CallContext, createHttpHandler, Protocol, str } from "../src/index.js";

// ---------------------------------------------------------------------------
// 1. Define an authenticate callback
// ---------------------------------------------------------------------------

const authenticate = bearerAuthenticate({
  validate(token: string): AuthContext {
    // In production, validate the token against a database or auth service.
    // Here we accept any token and use it as the principal.
    return new AuthContext("bearer", true, token, { role: "admin" });
  },
});

// ---------------------------------------------------------------------------
// 2. Define the service
// ---------------------------------------------------------------------------

const protocol = new Protocol("SecureService");

protocol.unary("status", {
  params: {},
  result: { message: str },
  handler: async () => ({ message: "ok" }),
  doc: "Public health-check endpoint.",
});

protocol.unary("whoami", {
  params: {},
  result: { identity: str },
  handler: async (_params, ctx: CallContext) => ({
    identity: ctx.auth.principal ?? "anonymous",
  }),
  doc: "Return the caller's identity.",
});

protocol.unary("secret_data", {
  params: {},
  result: { data: str },
  handler: async (_params, ctx: CallContext) => {
    ctx.auth.requireAuthenticated();
    const role = ctx.auth.claims.role ?? "unknown";
    return { data: `secret for ${ctx.auth.principal} (role=${role})` };
  },
  doc: "Return secret data (requires authentication).",
});

// ---------------------------------------------------------------------------
// 3. Start the server
// ---------------------------------------------------------------------------

const handler = createHttpHandler(protocol, {
  authenticate,
  oauthResourceMetadata: {
    resource: "https://api.example.com/vgi",
    authorizationServers: ["https://auth.example.com"],
    scopesSupported: ["read", "write"],
    resourceName: "Example Secure Service",
  },
});

const server = Bun.serve({ port: 8080, fetch: handler });

console.log(`Secure service listening on http://localhost:${server.port}`);
