// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * HTTP conformance server for Node.js — serves the conformance protocol over HTTP.
 * Prints PORT:<n> on stdout so test fixtures can discover the port.
 *
 * Set VGI_COMPRESSION_LEVEL=3 to enable zstd response compression.
 *
 * Run: npx tsx examples/conformance-http-node.ts
 */
import { createServer, type IncomingMessage } from "node:http";
import { protocol } from "./conformance-protocol.js";
import { createHttpHandler } from "../src/http/index.js";

const compressionLevel = process.env.VGI_COMPRESSION_LEVEL
  ? parseInt(process.env.VGI_COMPRESSION_LEVEL)
  : undefined;

const handler = createHttpHandler(protocol, {
  prefix: "/vgi",
  serverId: compressionLevel ? "conformance-node-zstd" : "conformance-node",
  compressionLevel,
});

/** Collect request body into a single Uint8Array. */
async function collectBody(req: IncomingMessage): Promise<Uint8Array | undefined> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(chunk as Buffer);
  }
  return chunks.length > 0 ? Buffer.concat(chunks) : undefined;
}

const server = createServer(async (req, res) => {
  const addr = server.address() as { port: number };
  const url = new URL(req.url!, `http://127.0.0.1:${addr.port}`);

  // Preserve original headers from raw header pairs
  const headers = new Headers();
  const raw = req.rawHeaders;
  for (let i = 0; i < raw.length; i += 2) {
    headers.append(raw[i], raw[i + 1]);
  }

  const body =
    req.method !== "GET" && req.method !== "HEAD"
      ? await collectBody(req)
      : undefined;

  const request = new Request(url.href, {
    method: req.method,
    headers,
    body,
  });

  try {
    const response = await handler(request);

    response.headers.forEach((value, key) => {
      res.setHeader(key, value);
    });
    res.writeHead(response.status);

    const buffer = Buffer.from(await response.arrayBuffer());
    res.end(buffer);
  } catch (err) {
    res.writeHead(500);
    res.end("Internal Server Error");
  }
});

server.listen(0, () => {
  const addr = server.address() as { port: number };
  console.log(`PORT:${addr.port}`);
});
