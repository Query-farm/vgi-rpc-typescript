// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * The `conformance_negotiating_client` hook of the shared suite
 * (PROTOCOL_VERSIONS_V1 §9, `TestClientNegotiation`): an HTTP client of this
 * port bound to one protocol, declaring a newest version and a supported
 * window, negotiating per §6.
 *
 *     bun run conformance/negotiating-client.ts <base-url> <protocol> <version> <version-range>
 *
 * Each line `ping` on stdin makes one call of the protocol's `ping()` method
 * (no arguments, utf8 result) on the same client, so negotiation state carries
 * across calls, and prints one JSON line: `{"result": "..."}` on success, or
 * `{"error_kind", "error_code", "error_message"}` on failure. EOF exits.
 *
 * The client is handed a static description of `ping`, so it makes no
 * reflection call first: the suite asserts what the client sends before it has
 * seen any response.
 */

import { Field, Schema, Utf8 } from "@query-farm/apache-arrow";
import { httpConnect } from "../src/client/connect.js";

const [baseUrl, protocol, version, range] = process.argv.slice(2);
if (!baseUrl || !protocol || !version || !range) {
  process.stderr.write("usage: negotiating-client.ts <base-url> <protocol> <version> <version-range>\n");
  process.exit(2);
}

const client = httpConnect(baseUrl, {
  protocol,
  protocolVersion: version,
  protocolVersionRange: range,
  description: {
    protocolName: protocol,
    protocolVersion: version,
    methods: [
      {
        name: "ping",
        type: "unary",
        hasReturn: true,
        paramsSchema: new Schema([]),
        resultSchema: new Schema([new Field("result", new Utf8(), false)]),
      },
    ],
  } as never,
});

async function ping(): Promise<Record<string, unknown>> {
  try {
    const out = await client.call("ping", {});
    return { result: String((out as Record<string, unknown> | null)?.result ?? "") };
  } catch (error) {
    const e = error as { errorKind?: string; errorCode?: string; errorMessage?: string; message?: string };
    return {
      error_kind: e.errorKind ?? "",
      error_code: e.errorCode ?? "",
      error_message: e.errorMessage ?? e.message ?? String(error),
    };
  }
}

let buffer = "";
for await (const chunk of process.stdin) {
  buffer += new TextDecoder().decode(chunk as Uint8Array);
  let nl = buffer.indexOf("\n");
  while (nl >= 0) {
    const line = buffer.slice(0, nl).trim();
    buffer = buffer.slice(nl + 1);
    const reply =
      line === "ping" ? await ping() : { error_kind: "driver_error", error_message: `unknown command ${line}` };
    process.stdout.write(`${JSON.stringify(reply)}\n`);
    nl = buffer.indexOf("\n");
  }
}
