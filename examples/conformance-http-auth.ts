// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Conformance HTTP server with a reject-all authenticate callback.
 *
 * Used by the TestHealth conformance suite to verify that GET /health is
 * exempt from authentication: every RPC endpoint returns 401, but the
 * health probe must still succeed.
 *
 * It also backs `TestUnauthorized`'s reason-code tests. A request may name the
 * reason it wants refused with, via `X-Conformance-Auth-Reason`, which is what
 * lets the suite prove a server *discriminates* between reason codes rather
 * than stamping one constant on every 401. Without the header the worker
 * throws a bare `Error` — the unclassified path, which must land on
 * `unauthorized`.
 */
import { AuthFailure, AuthReason, createHttpHandler } from "../src/http/index.js";
import { protocol } from "./conformance-protocol.js";

const portArg = process.argv.indexOf("--port");
const port = portArg >= 0 ? parseInt(process.argv[portArg + 1] ?? "0", 10) : 0;

const REASON_HEADER = "X-Conformance-Auth-Reason";

// The reasons a *request* may ask to be refused with. `proxy_required` is
// deliberately absent: docs/unauthorized-spec.md §5 derives it from server
// configuration, never from the request, so a worker that let a caller summon
// it would be modelling the contract wrong. Anything not in this map —
// including `proxy_required` and any typo — falls through to the unclassified
// path, so a test asking for a reason it cannot get fails rather than quietly
// passing.
const REQUESTABLE: Record<string, AuthReason> = {
  missing_credential: AuthReason.MissingCredential,
  invalid_credential: AuthReason.InvalidCredential,
  expired_credential: AuthReason.ExpiredCredential,
  insufficient_scope: AuthReason.InsufficientScope,
};

const handler = createHttpHandler(protocol, {
  serverId: "conformance-http-auth",
  protocolName: "ConformanceService",
  authenticate: (request: Request) => {
    const reason = REQUESTABLE[request.headers.get(REASON_HEADER) ?? ""];
    if (reason) {
      // The detail is the reason code itself so the suite can assert the
      // header and the body agree without pinning prose.
      throw new AuthFailure(reason, reason);
    }
    throw new Error("authentication required");
  },
});

const server = Bun.serve({ port, fetch: handler });
console.log(`PORT:${server.port}`);
