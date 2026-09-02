// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * HTTP conformance server — serves the conformance protocol over HTTP.
 * Prints PORT:<n> on stdout so test fixtures can discover the port.
 *
 * Set VGI_OTEL_FILE to a file path to enable OTel span export.
 *
 * Response compression follows the library default (ON, zstd level 1).
 * Pass `--response-compression off` for the explicitly-disabled variant that
 * advertises a present-but-empty `VGI-Supported-Encodings`, or
 * `--response-compression <n>` to pin a different zstd level.
 *
 * Pass `--no-call-state-cache` for the cold-cache variant, which forces every
 * stream continuation to re-open the call token the client echoed.
 *
 * Pass `--cors-origin <origin>` for the browser-facing variant `TestCors`
 * drives. CORS is strictly opt-in, so the plain worker keeps answering
 * preflights with no `Access-Control-Allow-Origin` for `TestCorsOffMode`.
 *
 * Pass `--introspect` for the token-introspection variant `TestTokenIntrospection`
 * drives. Also opt-in, so the plain worker keeps answering `404 not_enabled` for
 * `TestTokenIntrospectionOffMode`.
 *
 * Run: bun run examples/conformance-http.ts
 */
import { openSync } from "node:fs";
import { AccessLogHook, FdSink } from "../src/access-log.js";
import { AuthContext } from "../src/auth.js";
import type { ExternalLocationConfig, ExternalStorage, UploadUrl, UploadUrlProvider } from "../src/external.js";
import type { AuthenticateFn } from "../src/http/auth.js";
import { AuthUnavailableError, createHttpHandler } from "../src/http/index.js";
import type { TokenIdentity } from "../src/http/introspect.js";
import type { DispatchHook, HookToken, ServeStartHook } from "../src/types.js";
import { protocol } from "./conformance-protocol.js";

/** Decode a hex string into bytes — used only for the `--token-key` fixture flag. */
function hexToBytes(hex: string): Uint8Array {
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) {
    out[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

const otelFile = process.env.VGI_OTEL_FILE;

// ---------------------------------------------------------------------------
// CLI args (positional, kept simple to match other-language workers)
// ---------------------------------------------------------------------------

const args = process.argv.slice(2);
let fakeStorageUrl: string | undefined;
let externalizeThreshold = 4096;
let maxRequestBytesArg: number | undefined;
let maxFetchBytesArg: number | undefined;
let maxDecompressedFetchBytesArg: number | undefined;
let rejectLocalhostRedirects = false;
let compression: ExternalLocationConfig["compression"] | undefined;
let strictMode = false;
let maxResponseBytesArg: number | undefined;
let hostingMaxRequestBytesArg: number | undefined;
let hostingMaxResponseBytesArg: number | undefined;
let preferredResponseBytesArg: number | undefined;
let maxExternalizedResponseBytesArg: number | undefined;
// `undefined` keeps the library default (response compression ON at zstd
// level 1); `--response-compression off` passes an explicit `null`, the only
// way to reach the present-but-empty `VGI-Supported-Encodings` advertisement
// that `TestHttpCompressionNegotiationConformance` pins down.
let responseCompressionLevel: number | null | undefined;
// Sticky failure-path fixture knobs — see the reference repo's
// docs/sticky-sessions-spec.md §9.1. Each backs one TestSticky case that
// cannot run against the plain worker.
let serverIdArg: string | undefined;
let tokenKeyHex: string | undefined;
let stickyTtlArg: number | undefined;
let stickyAuth = false;
// Backs TestColdCallStateCache: with the resolved-call cache off, every
// continuation takes the miss path, so a client that forgets to echo its call
// token fails deterministically instead of only on a cold worker.
let callStateCacheEntries: number | undefined;
// Backs TestCors. Left unset the handler emits no CORS headers at all, which
// is what TestCorsOffMode asserts against the plain worker.
let corsOrigin: string | undefined;
// Backs TestTokenIntrospection. Left off the endpoint stays disabled and
// answers a definitive 404, which is what TestTokenIntrospectionOffMode
// asserts against the plain worker.
let introspect = false;
// Access-log fixture flags, mirroring the Python reference's CLI. The sample
// rate is validated when the hook is built — i.e. at startup, so `100` meaning
// "100%" is a launch failure rather than a deployment that silently logs
// everything.
let accessLogPath: string | undefined;
let accessLogSample = 1;
let accessLogAsync = false;
// Without this the record's `request_data` is a `payload_omitted` marker, so
// `vgi-rpc-test --require-request-data` has nothing to validate against.
let accessLogDebug = false;
// Lifecycle fault-injection fixture.  The hook is deliberately wired into
// the real HTTP handler so the shared suite observes the same first-request
// path an application would use.
let failServeStartOnce = false;
for (let i = 0; i < args.length; i++) {
  const a = args[i];
  if (a === "--server-id" && i + 1 < args.length) {
    serverIdArg = args[++i];
  } else if (a === "--token-key" && i + 1 < args.length) {
    tokenKeyHex = args[++i];
  } else if (a === "--sticky-ttl" && i + 1 < args.length) {
    stickyTtlArg = Number.parseInt(args[++i], 10);
  } else if (a === "--sticky-auth") {
    stickyAuth = true;
  } else if (a === "--no-call-state-cache") {
    callStateCacheEntries = 0;
  } else if (a === "--cors-origin" && i + 1 < args.length) {
    corsOrigin = args[++i];
  } else if (a === "--introspect") {
    introspect = true;
  } else if (a === "--response-compression" && i + 1 < args.length) {
    const v = args[++i];
    responseCompressionLevel = v === "off" || v === "none" ? null : Number.parseInt(v, 10);
  } else if (a === "--fake-storage" && i + 1 < args.length) {
    fakeStorageUrl = args[++i];
  } else if (a === "--externalize-threshold" && i + 1 < args.length) {
    externalizeThreshold = Number.parseInt(args[++i], 10);
  } else if (a === "--max-request-bytes" && i + 1 < args.length) {
    maxRequestBytesArg = Number.parseInt(args[++i], 10);
  } else if (a === "--hosting-max-request-bytes" && i + 1 < args.length) {
    hostingMaxRequestBytesArg = Number.parseInt(args[++i], 10);
  } else if (a === "--max-fetch-bytes" && i + 1 < args.length) {
    maxFetchBytesArg = Number.parseInt(args[++i], 10);
  } else if (a === "--max-decompressed-fetch-bytes" && i + 1 < args.length) {
    maxDecompressedFetchBytesArg = Number.parseInt(args[++i], 10);
  } else if (a === "--reject-localhost-redirects") {
    rejectLocalhostRedirects = true;
  } else if (a === "--compression" && i + 1 < args.length) {
    const v = args[++i];
    if (v === "zstd") compression = { algorithm: "zstd", level: 3 };
  } else if (a === "--strict") {
    strictMode = true;
  } else if (a === "--max-response-bytes" && i + 1 < args.length) {
    maxResponseBytesArg = Number.parseInt(args[++i], 10);
  } else if (a === "--hosting-max-response-bytes" && i + 1 < args.length) {
    hostingMaxResponseBytesArg = Number.parseInt(args[++i], 10);
  } else if (a === "--preferred-response-bytes" && i + 1 < args.length) {
    preferredResponseBytesArg = Number.parseInt(args[++i], 10);
  } else if (a === "--max-externalized-response-bytes" && i + 1 < args.length) {
    maxExternalizedResponseBytesArg = Number.parseInt(args[++i], 10);
  } else if (a === "--access-log" && i + 1 < args.length) {
    accessLogPath = args[++i];
  } else if (a === "--access-log-sample" && i + 1 < args.length) {
    accessLogSample = Number.parseFloat(args[++i]);
  } else if (a === "--access-log-async") {
    accessLogAsync = true;
  } else if (a === "--access-log-debug") {
    accessLogDebug = true;
  } else if (a === "--fail-serve-start-once") {
    failServeStartOnce = true;
  }
}
// Strict-cap mode: tight body + external caps so the http_response_cap.*
// conformance tests can deliberately overshoot. Defaults to 1 MiB matching
// Python's tests/serve_conformance_http_strict.py.
// Passing either cap flag on its own also selects the real-byte-cap mode, so a
// fixture can pin ONE cap tight and leave the other generous — which is what
// the externalized-cap group needs: with both tight, the body cap fails first
// and the group proves nothing about the external channel.
const STRICT_DEFAULT = 1024 * 1024;
const capsConfigured = strictMode || maxResponseBytesArg !== undefined || maxExternalizedResponseBytesArg !== undefined;
const maxResponseBytes = maxResponseBytesArg ?? STRICT_DEFAULT;
const maxExternalizedResponseBytes = maxExternalizedResponseBytesArg ?? STRICT_DEFAULT;
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
    const allocation = (await allocResp.json()) as {
      object_url: string;
      upload_url?: string;
      download_url?: string;
    };
    const uploadUrl = allocation.upload_url ?? allocation.object_url;
    const downloadUrl = allocation.download_url ?? allocation.object_url;

    const putHeaders: Record<string, string> = { "Content-Type": "application/octet-stream" };
    if (contentEncoding) putHeaders["Content-Encoding"] = contentEncoding;
    const putResp = await fetch(uploadUrl, { method: "PUT", headers: putHeaders, body: data });
    if (!putResp.ok) {
      throw new Error(`fake-storage PUT failed: ${putResp.status}`);
    }
    return downloadUrl;
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
    const allocation = (await allocResp.json()) as {
      object_url: string;
      upload_url?: string;
      download_url?: string;
    };
    return {
      uploadUrl: allocation.upload_url ?? allocation.object_url,
      downloadUrl: allocation.download_url ?? allocation.object_url,
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
    urlValidator: rejectLocalhostRedirects
      ? (location) => {
          const parsed = new URL(location);
          if (parsed.protocol !== "http:" || parsed.hostname !== "127.0.0.1") {
            throw new Error("fixture permits only the 127.0.0.1 fake-storage origin");
          }
        }
      : null, // fake storage runs on http://127.0.0.1
    ...(maxFetchBytesArg !== undefined ? { maxFetchBytes: maxFetchBytesArg } : {}),
    ...(maxDecompressedFetchBytesArg !== undefined ? { maxDecompressedBytes: maxDecompressedFetchBytesArg } : {}),
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

if (accessLogPath) {
  const accessHook = new AccessLogHook(new FdSink(openSync(accessLogPath, "a")), {
    serverVersion: "vgi-rpc-typescript-conformance",
    sampleRate: accessLogSample,
    async: accessLogAsync,
    level: accessLogDebug ? "DEBUG" : "INFO",
  });
  const otelHook = dispatchHook;
  // Both hooks want the dispatch boundary; fan out rather than making the
  // operator choose between tracing and an audit trail.
  dispatchHook = otelHook
    ? {
        onDispatchStart: (info) => [otelHook.onDispatchStart(info), accessHook.onDispatchStart(info)],
        onDispatchEnd: (token, info, stats, error) => {
          const [otelToken, accessToken] = token as [HookToken, HookToken];
          otelHook.onDispatchEnd(otelToken, info, stats, error);
          accessHook.onDispatchEnd(accessToken, info, stats, error);
        },
      }
    : accessHook;
  // Queued records are only durable once drained; flush on the way out.
  if (accessLogAsync) {
    process.on("exit", () => accessHook.flush());
  }
}

// Sticky-session fixture wiring — TestSticky in vgi_rpc.conformance is
// capability-gated on `VGI-Sticky-Enabled: true`, so the conformance
// worker enables sticky by default with a fixed marker echo header. A
// `/__test_drain__` admin endpoint (POST / DELETE) toggles the drain
// flag so `TestSticky::test_drain_rejects_new_opens` can exercise it
// without sending SIGTERM mid-fixture.
let stickyDrainHandle: import("../src/http/sticky.js").DrainHandle | null = null;

/**
 * Resolve the principal named in `X-Conformance-Principal`, or stay anonymous.
 *
 * Backs `TestSticky::test_cross_principal_replay_rejected`, which needs one
 * worker reachable as two identities so it can open a session as one and replay
 * the token as the other. Naming yourself in a header is obviously not
 * authentication — it is the cheapest thing every port can implement
 * identically, and the test only needs the identities to be distinguishable.
 *
 * Requests without the header stay anonymous rather than being rejected: the
 * conformance suite probes `/health` and the capability endpoint before it
 * authenticates anything.
 */
const principalHeaderAuth: AuthenticateFn = (request: Request) => {
  const principal = request.headers.get("X-Conformance-Principal");
  return principal ? new AuthContext("conformance", true, principal) : AuthContext.anonymous();
};

// ---------------------------------------------------------------------------
// Token-introspection fixture wiring
// ---------------------------------------------------------------------------

/** Fixed conformance values a runner supplying `conformance_http_introspect_port`
 *  MUST configure: the shared tests post the subject credential and assert the
 *  principal, so these are part of the fixture's contract, not decoration. */
const CONFORMANCE_INTROSPECTOR = "conformance-introspector";
const CONFORMANCE_SUBJECT_TOKEN = "conformance-opaque-subject-token";
const CONFORMANCE_SUBJECT_PRINCIPAL = "subject@conformance.example";
const CONFORMANCE_SUBJECT_TOKEN_NAME = "conformance-subject";
/** A JWS-shaped credential the resolver *would* resolve. Deliberately
 *  resolvable: if the fixture only offered an unknown JWS, a port with no shape
 *  guard would reject it as unknown and pass the test for the wrong reason.
 *  Made resolvable, the guard becomes observable — a port that fails to reject
 *  JWS shapes answers 200 and fails. */
const CONFORMANCE_JWS_TRAP_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbGljZSJ9.c2lnbmF0dXJl";
/** The credential whose resolution is *unknowable* rather than unknown. The
 *  shared suite posts it to check that a backing-store outage surfaces as a
 *  transient 503 and not as the endpoint's own definitive 404 — which a caller
 *  may negative-cache, so a briefly unreachable store would be remembered as a
 *  bad credential for the cache's lifetime. */
const CONFORMANCE_UNAVAILABLE_TOKEN = "conformance-unavailable-token";

/** Resolve the fixed credentials the shared tests post.
 *
 *  Three answers, deliberately: an identity, `null` for "does not resolve", and
 *  a thrown {@link AuthUnavailableError} for "I could not find out". The third is
 *  not a flavour of the second — `null` becomes the definitive 404 a caller may
 *  negative-cache. */
function conformanceResolver(token: string): TokenIdentity | null {
  if (token === CONFORMANCE_UNAVAILABLE_TOKEN) {
    throw new AuthUnavailableError("conformance: mapping store unreachable");
  }
  if (token === CONFORMANCE_SUBJECT_TOKEN || token === CONFORMANCE_JWS_TRAP_TOKEN) {
    return { principal: CONFORMANCE_SUBJECT_PRINCIPAL, tokenName: CONFORMANCE_SUBJECT_TOKEN_NAME, ttlSeconds: 300 };
  }
  return null;
}

const handler = createHttpHandler(protocol, {
  serverId: serverIdArg ?? "conformance-http",
  protocolName: "ConformanceService",
  enableSticky: true,
  stickyDefaultTtl: stickyTtlArg ?? 300,
  ...(tokenKeyHex ? { tokenKey: hexToBytes(tokenKeyHex) } : {}),
  ...(callStateCacheEntries !== undefined ? { callStateCacheEntries } : {}),
  // `--introspect` implies principal-header auth so the introspector allowlist
  // has something to check.
  ...(stickyAuth || introspect ? { authenticate: principalHeaderAuth } : {}),
  ...(corsOrigin ? { corsOrigins: corsOrigin } : {}),
  ...(introspect ? { introspectResolver: conformanceResolver, introspectPrincipals: [CONFORMANCE_INTROSPECTOR] } : {}),
  stickyEchoHeaders: { "x-vgi-conformance-echo": "conformance-fixed-marker" },
  _onStickyHandle: (h) => {
    stickyDrainHandle = h;
  },
  // Omitted unless --response-compression was passed, so the plain worker
  // runs on the library default (zstd level 1, gzip fallback on runtimes
  // without a zstd encoder).
  ...(responseCompressionLevel !== undefined ? { compressionLevel: responseCompressionLevel } : {}),
  // Strict response caps are configured only in the dedicated fixture. The
  // former plain-worker `maxStreamResponseBytes: 1` soft hint is deliberately
  // gone: producer turns now obey the same hard cap and would all fail.
  ...(capsConfigured
    ? {
        maxResponseBytes,
        maxExternalizedResponseBytes,
      }
    : {}),
  ...(hostingMaxRequestBytesArg !== undefined ? { hostingMaxRequestBytes: hostingMaxRequestBytesArg } : {}),
  ...(hostingMaxResponseBytesArg !== undefined ? { hostingMaxResponseBytes: hostingMaxResponseBytesArg } : {}),
  ...(preferredResponseBytesArg !== undefined ? { preferredResponseBytes: preferredResponseBytesArg } : {}),
  ...(dispatchHook ? { dispatchHook } : {}),
  ...(externalLocation ? { externalLocation } : {}),
  ...(failServeStartOnce
    ? {
        onServeStart: (() => {
          let calls = 0;
          return () => {
            calls += 1;
            if (calls === 1) {
              throw new Error("conformance injected on_serve_start failure");
            }
          };
        })() satisfies ServeStartHook,
      }
    : {}),
  ...(fakeStorage || maxRequestBytesArg !== undefined
    ? {
        ...(fakeStorage ? { uploadUrlProvider: fakeStorage } : {}),
        maxRequestBytes,
        ...(fakeStorage ? { maxUploadBytes: 64 * 1024 * 1024 } : {}),
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
  try {
    return await handler(req);
  } catch (error) {
    // Bun's default rejected-fetch handling is runtime-version dependent.
    // The fixture contract is not: expose the injected lifecycle fault as a
    // deterministic 500 while leaving the listener alive for the retry.
    if (failServeStartOnce) {
      return new Response("Internal Server Error", { status: 500 });
    }
    throw error;
  }
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
