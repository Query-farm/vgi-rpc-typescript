// © Copyright 2025-2026, Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

/** Minimal versionless worker used by the shared transport-kind probes. */
import { createHttpHandler } from "../src/http/index.js";
import { Protocol, serveTcp, serveUnix, str, VgiRpcServer } from "../src/index.js";

const protocol = new Protocol("TransportKindProbe");
protocol.unary("report_transport_kind", {
  params: {},
  result: { result: str },
  handler: (_params, ctx) => ({ result: ctx.kind ?? "none" }),
});

const args = process.argv.slice(2);
const mode = args[0];

if (mode === "--http") {
  const handler = createHttpHandler(protocol, {
    protocolName: "TransportKindProbe",
    compressionLevel: null,
  });
  const server = Bun.serve({ port: 0, fetch: handler });
  console.log(`PORT:${server.port}`);
} else if (mode === "--unix") {
  const path = args[1];
  if (!path) throw new Error("--unix requires a socket path");
  const handle = await serveUnix(protocol, {
    unixPath: path,
    idleTimeout: 0,
  });
  await handle.done;
} else if (mode === "--tcp") {
  const address = args[1] ?? "127.0.0.1:0";
  const separator = address.lastIndexOf(":");
  const host = separator >= 0 ? address.slice(0, separator) || "127.0.0.1" : "127.0.0.1";
  const port = Number.parseInt(separator >= 0 ? address.slice(separator + 1) : address, 10);
  if (!Number.isFinite(port)) throw new Error(`invalid --tcp address: ${address}`);
  const handle = await serveTcp(protocol, {
    host,
    port,
    idleTimeout: 0,
  });
  await handle.done;
} else {
  await new VgiRpcServer(protocol).run();
}
