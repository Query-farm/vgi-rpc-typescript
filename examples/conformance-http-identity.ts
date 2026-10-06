// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Conformance HTTP server hosting `vgi_rpc.Identity.v1` under the pinned
 * deployment policy of `IDENTITY_CONFORMANCE_FIXTURE.md`.
 *
 * Identity is almost entirely *guards*, and every guard reads deployment
 * policy: who may introspect, what a credential resolves to, whether a grant
 * is minted, how recently the caller authenticated. Against a worker whose
 * allowlist and hooks are unknown no cross-port assertion exists — every
 * answer is explicable as policy. So the policy is pinned, identically in six
 * ports, and every constant below is part of the fixture's contract rather
 * than a choice this port gets to make.
 *
 * One binary, two fixtures, selected by `--identity`:
 *
 * | flag | hooks | runner fixture |
 * |---|---|---|
 * | `both` | resolve **and** mint | `conformance_http_identity_port` |
 * | `introspect-only` | resolve only | `conformance_http_identity_introspect_only_port` |
 * | `off` | neither | (unused by the group — see below) |
 *
 * `off` is here for the mutation checks §8 asks for and for symmetry with the
 * reference's flag, not because the group drives it. "A worker configuring no
 * hook hosts no identity protocol at all" is asserted against the *plain*
 * conformance worker (`examples/conformance-http.ts`), which must stay
 * identity-free: a fixture that opted in would make the property untestable
 * by making every worker in the suite an opt-in one.
 *
 * Run: `bun run examples/conformance-http-identity.ts --identity both`
 *
 * The policy itself (authenticator, resolver, minter, allowlist) lives in
 * `src/conformance/identity-fixture.ts`, exported as
 * `@query-farm/vgi-rpc/conformance` so SDK fixture workers share it.
 *
 * @packageDocumentation
 */

import { buildSecondaryProtocol, conformanceAuthenticate, conformanceIdentity } from "../src/conformance/index.js";
import { createHttpHandler } from "../src/http/index.js";
import { VgiRpcServer } from "../src/server.js";
import { protocol } from "./conformance-protocol.js";

// ---------------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------------

const args = process.argv.slice(2);
const modeArg = args.indexOf("--identity");
const mode = modeArg >= 0 ? (args[modeArg + 1] ?? "") : "both";
if (mode !== "both" && mode !== "introspect-only" && mode !== "off") {
  throw new Error(`--identity must be one of both, introspect-only, off (got ${JSON.stringify(mode)})`);
}

// A `VgiRpcServer` rather than a bare `Protocol`: identity is a *secondary*
// protocol, and only a host can carry one. The constructor registers
// reflection, so identity lands after it and appears in its own server's
// listing — which is how a client discovers it instead of calling to find out.
const server = new VgiRpcServer(protocol, {
  serverId: `conformance-http-identity-${mode}`,
  // Every conformance worker hosts the fixture secondary, registered through
  // the public hosting API rather than special-cased.
  protocols: [buildSecondaryProtocol()],
  ...(mode === "off" ? {} : { identity: conformanceIdentity(mode) }),
});

const handler = createHttpHandler(server, {
  serverId: `conformance-http-identity-${mode}`,
  protocolName: "ConformanceService",
  authenticate: conformanceAuthenticate,
});

const listener = Bun.serve({ port: 0, fetch: handler });
console.log(`PORT:${listener.port}`);
