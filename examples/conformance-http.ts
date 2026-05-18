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
import type { ExternalLocationConfig, ExternalStorage, UploadUrl, UploadUrlProvider } from "../src/external.js";
import { createHttpHandler } from "../src/http/index.js";
import type { DispatchHook } from "../src/types.js";
import { protocol } from "./conformance-protocol.js";

const otelFile = process.env.VGI_OTEL_FILE;

// ---------------------------------------------------------------------------
// CLI args (positional, kept simple to match other-language workers)
// ---------------------------------------------------------------------------

const args = process.argv.slice(2);
let fakeStorageUrl: string | undefined;
let externalizeThreshold = 4096;
let maxRequestBytesArg: number | undefined;
let compression: ExternalLocationConfig["compression"] | undefined;
let strictMode = false;
let maxResponseBytesArg: number | undefined;
let maxExternalizedResponseBytesArg: number | undefined;
for (let i = 0; i < args.length; i++) {
  const a = args[i];
  if (a === "--fake-storage" && i + 1 < args.length) {
    fakeStorageUrl = args[++i];
  } else if (a === "--externalize-threshold" && i + 1 < args.length) {
    externalizeThreshold = Number.parseInt(args[++i], 10);
  } else if (a === "--max-request-bytes" && i + 1 < args.length) {
    maxRequestBytesArg = Number.parseInt(args[++i], 10);
  } else if (a === "--compression" && i + 1 < args.length) {
    const v = args[++i];
    if (v === "zstd") compression = { algorithm: "zstd", level: 3 };
  } else if (a === "--strict") {
    strictMode = true;
  } else if (a === "--max-response-bytes" && i + 1 < args.length) {
    maxResponseBytesArg = Number.parseInt(args[++i], 10);
  } else if (a === "--max-externalized-response-bytes" && i + 1 < args.length) {
    maxExternalizedResponseBytesArg = Number.parseInt(args[++i], 10);
  }
}
// Strict-cap mode: tight body + external caps so the http_response_cap.*
// conformance tests can deliberately overshoot. Defaults to 1 MiB matching
// Python's tests/serve_conformance_http_strict.py.
const STRICT_DEFAULT = 1024 * 1024;
const maxResponseBytes = maxResponseBytesArg ?? (strictMode ? STRICT_DEFAULT : undefined);
const maxExternalizedResponseBytes = maxExternalizedResponseBytesArg ?? (strictMode ? STRICT_DEFAULT : undefined);
// Inline-request cap defaults to the externalize threshold for backward compat
// with previous worker invocations. The ``externalize-always`` variant passes
// both flags explicitly so server-side externalization fires on every batch
// while clients can still send normal-sized inline requests.
const maxRequestBytes = maxRequestBytesArg ?? externalizeThreshold;

// ---------------------------------------------------------------------------
// FakeStorage adapter — speaks the 4-endpoint contract documented in
// vgi_rpc.conformance.fake_storage (POST /alloc, PUT /blob/{id}, ...).
// ---------------------------------------------------------------------------

class FakeStorage implements ExternalStorage, UploadUrlProvider {
  constructor(private readonly baseUrl: string) {}

  async upload(data: Uint8Array, contentEncoding: string): Promise<string> {
    const allocResp = await fetch(`${this.baseUrl}/alloc`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: contentEncoding ? JSON.stringify({ content_encoding: contentEncoding }) : "{}",
    });
    if (!allocResp.ok) {
      throw new Error(`fake-storage /alloc failed: ${allocResp.status}`);
    }
    const { object_url: objectUrl } = (await allocResp.json()) as { object_url: string };

    const putHeaders: Record<string, string> = { "Content-Type": "application/octet-stream" };
    if (contentEncoding) putHeaders["Content-Encoding"] = contentEncoding;
    const putResp = await fetch(objectUrl, { method: "PUT", headers: putHeaders, body: data });
    if (!putResp.ok) {
      throw new Error(`fake-storage PUT failed: ${putResp.status}`);
    }
    return objectUrl;
  }

  async generateUploadUrl(): Promise<UploadUrl> {
    const allocResp = await fetch(`${this.baseUrl}/alloc`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
    if (!allocResp.ok) {
      throw new Error(`fake-storage /alloc failed: ${allocResp.status}`);
    }
    const { object_url: objectUrl } = (await allocResp.json()) as { object_url: string };
    // The fake storage uses the same path for PUT and GET — disambiguates by HTTP method.
    return {
      uploadUrl: objectUrl,
      downloadUrl: objectUrl,
      expiresAt: new Date(Date.now() + 3600_000),
    };
  }
}

let externalLocation: ExternalLocationConfig | undefined;
let fakeStorage: FakeStorage | undefined;
if (fakeStorageUrl) {
  fakeStorage = new FakeStorage(fakeStorageUrl);
  externalLocation = {
    storage: fakeStorage,
    externalizeThresholdBytes: externalizeThreshold,
    urlValidator: null, // fake storage runs on http://127.0.0.1
    ...(compression ? { compression } : {}),
  };
}

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

// Sticky-session fixture wiring — TestSticky in vgi_rpc.conformance is
// capability-gated on `VGI-Sticky-Enabled: true`, so the conformance
// worker enables sticky by default with a fixed marker echo header. A
// `/__test_drain__` admin endpoint (POST / DELETE) toggles the drain
// flag so `TestSticky::test_drain_rejects_new_opens` can exercise it
// without sending SIGTERM mid-fixture.
let stickyDrainHandle: import("../src/http/sticky.js").DrainHandle | null = null;

const handler = createHttpHandler(protocol, {
  serverId: "conformance-http",
  protocolName: "ConformanceService",
  enableSticky: true,
  stickyDefaultTtl: 300,
  stickyEchoHeaders: { "x-vgi-conformance-echo": "conformance-fixed-marker" },
  _onStickyHandle: (h) => {
    stickyDrainHandle = h;
  },
  // Bound per-response size so infinite producers (e.g. ``cancellable_producer``)
  // return promptly and the client can follow continuation tokens or cancel
  // mid-stream. Any positive value works; 1 byte forces a continuation after
  // every produce cycle, matching the Python reference server's default.
  // In strict-cap mode we use a real byte cap instead so the strict-fail
  // tests can deliberately overshoot.
  ...(strictMode
    ? {
        maxResponseBytes: maxResponseBytes!,
        maxExternalizedResponseBytes: maxExternalizedResponseBytes!,
      }
    : { maxStreamResponseBytes: 1 }),
  ...(dispatchHook ? { dispatchHook } : {}),
  ...(externalLocation ? { externalLocation } : {}),
  ...(fakeStorage
    ? {
        uploadUrlProvider: fakeStorage,
        maxRequestBytes,
        maxUploadBytes: 64 * 1024 * 1024,
      }
    : {}),
});

// Wrap the handler with the test-only `/__test_drain__` admin endpoint so
// canonical conformance tests can drive the registry's drain flag over
// the wire without sending SIGTERM (which would kill the fixture).
const wrappedHandler = async (req: Request): Promise<Response> => {
  const url = new URL(req.url);
  if (url.pathname === "/__test_drain__") {
    if (!stickyDrainHandle) return new Response(null, { status: 404 });
    if (req.method === "POST") {
      stickyDrainHandle.drain();
      return new Response(null, { status: 204 });
    }
    if (req.method === "DELETE") {
      stickyDrainHandle.setDraining(false);
      return new Response(null, { status: 204 });
    }
    return new Response(null, { status: 405 });
  }
  return handler(req);
};

const server = Bun.serve({ port: 0, fetch: wrappedHandler });
console.log(`PORT:${server.port}`);

if (shutdownOtel) {
  const shutdown = async () => {
    await shutdownOtel!();
    process.exit(0);
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}
