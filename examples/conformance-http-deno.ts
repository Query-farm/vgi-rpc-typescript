// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { createHttpHandler } from "../src/http/index.js";
/**
 * HTTP conformance server for Deno — serves the conformance protocol over HTTP.
 * Prints PORT:<n> on stdout so test fixtures can discover the port.
 *
 * Response compression follows the library default (ON, zstd level 1).
 * VGI_COMPRESSION_LEVEL=3 pins a different level; VGI_COMPRESSION_LEVEL=off
 * disables it, which is what makes the present-but-empty
 * `VGI-Supported-Encodings` advertisement reachable.
 *
 * Run: deno run --allow-all examples/conformance-http-deno.ts
 */
import { protocol } from "./conformance-protocol.js";

const rawLevel = Deno.env.get("VGI_COMPRESSION_LEVEL");
// undefined => leave the option unset so the library default applies.
const compressionLevel: number | null | undefined = !rawLevel
  ? undefined
  : rawLevel === "off" || rawLevel === "none"
    ? null
    : parseInt(rawLevel, 10);

const handler = createHttpHandler(protocol, {
  serverId: compressionLevel ? "conformance-deno-zstd" : "conformance-deno",
  protocolName: "ConformanceService",
  ...(compressionLevel !== undefined ? { compressionLevel } : {}),
  maxStreamResponseBytes: 1,
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
