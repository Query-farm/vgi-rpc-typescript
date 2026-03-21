// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * HTTP conformance server — serves the conformance protocol over HTTP.
 * Prints PORT:<n> on stdout so test fixtures can discover the port.
 *
 * Set VGI_OTEL_FILE to a file path to enable OTel span export.
 *
 * Run: bun run examples/conformance-http.ts
 */
import { createHttpHandler } from "../src/http/index.js";
import type { DispatchHook } from "../src/types.js";
import { protocol } from "./conformance-protocol.js";

const otelFile = process.env.VGI_OTEL_FILE;

let dispatchHook: DispatchHook | undefined;
let shutdownOtel: (() => Promise<void>) | undefined;

if (otelFile) {
  const { createOtelHook } = await import("../src/otel.js");
  const { BasicTracerProvider, SimpleSpanProcessor, InMemorySpanExporter } = await import(
    "@opentelemetry/sdk-trace-base"
  );

  const spanExporter = new InMemorySpanExporter();
  const tracerProvider = new BasicTracerProvider({
    spanProcessors: [new SimpleSpanProcessor(spanExporter)],
  });

  dispatchHook = createOtelHook({
    tracerProvider,
    serviceName: "conformance-ts",
  });

  shutdownOtel = async () => {
    await tracerProvider.forceFlush();

    const spans = spanExporter.getFinishedSpans().map((s) => ({
      name: s.name,
      kind: s.kind,
      status: { code: s.status.code },
      attributes: { ...s.attributes },
    }));

    const output = JSON.stringify({ spans, metrics: [] }, null, 2);
    await Bun.write(otelFile, output);

    await tracerProvider.shutdown();
  };
}

const handler = createHttpHandler(protocol, {
  serverId: "conformance-http",
  ...(dispatchHook ? { dispatchHook } : {}),
});

const server = Bun.serve({ port: 0, fetch: handler });
console.log(`PORT:${server.port}`);

if (shutdownOtel) {
  const shutdown = async () => {
    await shutdownOtel!();
    process.exit(0);
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}
