// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * HTTP conformance server for Deno — serves the conformance protocol over HTTP.
 * Prints PORT:<n> on stdout so test fixtures can discover the port.
 *
 * Set VGI_COMPRESSION_LEVEL=3 to enable zstd response compression.
 *
 * Run: deno run --allow-all examples/conformance-http-deno.ts
 */
import { protocol } from "./conformance-protocol.js";
import { createHttpHandler } from "../src/http/index.js";

const compressionLevel = Deno.env.get("VGI_COMPRESSION_LEVEL")
  ? parseInt(Deno.env.get("VGI_COMPRESSION_LEVEL")!)
  : undefined;

const handler = createHttpHandler(protocol, {
  prefix: "/vgi",
  serverId: compressionLevel ? "conformance-deno-zstd" : "conformance-deno",
  compressionLevel,
});

Deno.serve(
  {
    port: 0,
    hostname: "127.0.0.1",
    onListen({ port }) {
      console.log(`PORT:${port}`);
    },
  },
  (request) => handler(request),
);
