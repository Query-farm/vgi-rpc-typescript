// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Worker-visible response budgets + producer-stream externalization
 * accounting.  Mirrors the conformance contract from Python commit
 * 929945b: a producer that exfiltrates data via tiny pointer batches
 * still hits the external cap.
 */

import { describe, expect, test } from "bun:test";
import {
  RecordBatch,
  RecordBatchReader,
  RecordBatchStreamWriter,
  recordBatchFromArrays,
  Schema,
} from "@query-farm/apache-arrow";
import {
  CALL_STATE_KEY,
  LOCATION_KEY,
  LOG_LEVEL_KEY,
  LOG_MESSAGE_KEY,
  REQUEST_VERSION,
  REQUEST_VERSION_KEY,
  RPC_ERROR_HEADER,
  RPC_METHOD_KEY,
  STATE_KEY,
} from "../../src/constants.js";
import type { ExternalLocationConfig, ExternalStorage } from "../../src/external.js";
import { AuthFailure, AuthReason } from "../../src/http/unauthorized.js";
import { parseResponseBudgetDecimal } from "../../src/http/response-budget.js";
import { ARROW_CONTENT_TYPE, bytes, createHttpHandler, Protocol, str } from "../../src/index.js";

function buildRequestIpc(
  schema: Schema,
  values: Record<string, any[]>,
  methodName: string,
  extraMetadata?: ReadonlyMap<string, string>,
): Uint8Array {
  const batch = recordBatchFromArrays(values, schema);
  const meta = new Map<string, string>();
  meta.set(RPC_METHOD_KEY, methodName);
  meta.set(REQUEST_VERSION_KEY, REQUEST_VERSION);
  if (extraMetadata) for (const [key, value] of extraMetadata) meta.set(key, value);
  const batchWithMeta = new RecordBatch(schema, batch.data, meta);
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, schema);
  writer.write(batchWithMeta);
  writer.close();
  return writer.toUint8Array(true);
}

async function readBody(response: Response): Promise<{ batches: RecordBatch[] }> {
  const body = new Uint8Array(await response.arrayBuffer());
  const reader = await RecordBatchReader.from(body);
  await reader.open();
  const batches = reader.readAll();
  return { batches };
}

/** In-memory storage that records every upload size. */
class MemoryStorage implements ExternalStorage {
  uploads: Array<{ url: string; size: number }> = [];
  async upload(data: Uint8Array, _contentEncoding: string): Promise<string> {
    const url = `https://memory.test/${this.uploads.length}`;
    this.uploads.push({ url, size: data.byteLength });
    return url;
  }
}

describe("OutputCollector budget snapshots (worker-visible)", () => {
  test("unary handler sees remaining budgets when caps are configured", async () => {
    let observed: { wire?: number; ext?: number; enabled?: boolean } = {};
    const protocol = new Protocol("BudgetSvc").unary("ping", {
      params: { msg: str },
      result: { msg: str },
      handler: (params, ctx) => {
        const c = ctx as unknown as {
          remainingResponseBytes?: number;
          remainingExternalizedResponseBytes?: number;
          externalizationEnabled?: boolean;
        };
        observed = {
          wire: c.remainingResponseBytes,
          ext: c.remainingExternalizedResponseBytes,
          enabled: c.externalizationEnabled,
        };
        return { msg: String(params.msg) };
      },
    });

    const storage = new MemoryStorage();
    const externalLocation: ExternalLocationConfig = { storage, externalizeThresholdBytes: 100 };
    const handler = createHttpHandler(protocol, {
      maxResponseBytes: 65_536,
      maxExternalizedResponseBytes: 4096,
      externalLocation,
    });

    const reqSchema = new Schema([
      new (await import("@query-farm/apache-arrow")).Field(
        "msg",
        new (await import("@query-farm/apache-arrow")).Utf8(),
      ),
    ]);
    const body = buildRequestIpc(reqSchema, { msg: ["hi"] }, "ping");
    const response = await handler(
      new Request("http://test/ping", {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body: body as unknown as BodyInit,
      }),
    );
    expect(response.status).toBe(200);
    expect(observed.wire).toBe(65_536);
    expect(observed.ext).toBe(4096);
    expect(observed.enabled).toBe(true);
  });

  test("application, hosting, and client limits use the minimum and clamp the preferred target", async () => {
    let observed: { limit?: number; preferred?: number } = {};
    const protocol = new Protocol("MinimumSvc").unary("ping", {
      params: { msg: str },
      result: { msg: str },
      handler: (params, ctx) => {
        const c = ctx as unknown as { responseLimitBytes?: number; preferredResponseBytes?: number };
        observed = { limit: c.responseLimitBytes, preferred: c.preferredResponseBytes };
        return { msg: String(params.msg) };
      },
    });
    const handler = createHttpHandler(protocol, {
      maxRequestBytes: 9000,
      hostingMaxRequestBytes: 7000,
      maxResponseBytes: 80_000,
      hostingMaxResponseBytes: 70_000,
      preferredResponseBytes: 75_000,
      corsOrigins: "*",
    });
    const options = await handler(new Request("http://test/health", { method: "OPTIONS" }));
    expect(options.headers.get("VGI-Max-Request-Bytes")).toBe("7000");
    expect(options.headers.get("VGI-Max-Response-Bytes")).toBe("70000");
    expect(options.headers.get("VGI-Accept-Max-Response-Bytes-Support")).toBe("true");
    expect(options.headers.get("Access-Control-Allow-Headers")).toContain("VGI-Accept-Max-Response-Bytes");
    expect(options.headers.get("Access-Control-Expose-Headers")).toContain("VGI-Accept-Max-Response-Bytes-Support");

    const M = await import("@query-farm/apache-arrow");
    const reqSchema = new Schema([new M.Field("msg", new M.Utf8())]);
    const body = buildRequestIpc(reqSchema, { msg: ["hi"] }, "ping");
    const response = await handler(
      new Request("http://test/ping", {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE, "VGI-Accept-Max-Response-Bytes": "65536" },
        body: body as unknown as BodyInit,
      }),
    );
    expect(response.status).toBe(200);
    expect(observed).toEqual({ limit: 65_536, preferred: 65_536 });
  });

  test("OPTIONS validates a present accepted response limit and returns an Arrow ValueError", async () => {
    const handler = createHttpHandler(new Protocol("OptionsBudgetSvc"), { corsOrigins: "*" });

    for (const value of [null, "65536"]) {
      const headers = value === null ? undefined : { "VGI-Accept-Max-Response-Bytes": value };
      const response = await handler(new Request("http://test/health", { method: "OPTIONS", headers }));
      expect(response.status).toBe(204);
      expect(response.headers.get("VGI-Accept-Max-Response-Bytes-Support")).toBe("true");
    }

    const invalidHeaders: Headers[] = ["065536", "65536, 70000", "65535"].map((value) => {
      const headers = new Headers();
      headers.set("VGI-Accept-Max-Response-Bytes", value);
      return headers;
    });
    const duplicate = new Headers();
    duplicate.append("VGI-Accept-Max-Response-Bytes", "65536");
    duplicate.append("VGI-Accept-Max-Response-Bytes", "70000");
    invalidHeaders.push(duplicate);

    // Fetch's Headers implementation rejects non-ByteString field values
    // before application dispatch. The shared parser retains the same ASCII-
    // only invariant for runtimes that provide a less strict Headers shim.
    expect(() => parseResponseBudgetDecimal("٦٥٥٣٦")).toThrow();
    expect(() => new Headers({ "VGI-Accept-Max-Response-Bytes": "٦٥٥٣٦" })).toThrow();

    for (const headers of invalidHeaders) {
      const response = await handler(new Request("http://test/health", { method: "OPTIONS", headers }));
      expect(response.status).toBe(400);
      expect(response.headers.get("Content-Type")).toBe(ARROW_CONTENT_TYPE);
      expect(response.headers.get(RPC_ERROR_HEADER)).toBe("true");
      expect(response.headers.get("VGI-Accept-Max-Response-Bytes-Support")).toBe("true");
      const { batches } = await readBody(response);
      expect(batches[0].metadata?.get(LOG_MESSAGE_KEY)).toContain("ValueError: Invalid VGI-Accept-Max-Response-Bytes");
    }
  });

  test("rejects non-canonical or unsafe accepted response decimals before dispatch", async () => {
    let calls = 0;
    const protocol = new Protocol("DecimalSvc").unary("ping", {
      params: { msg: str },
      result: { msg: str },
      handler: (params) => {
        calls += 1;
        return { msg: String(params.msg) };
      },
    });
    const handler = createHttpHandler(protocol);
    const M = await import("@query-farm/apache-arrow");
    const reqSchema = new Schema([new M.Field("msg", new M.Utf8())]);
    const body = buildRequestIpc(reqSchema, { msg: ["hi"] }, "ping");
    // Fetch Headers strips surrounding OWS before application code can
    // inspect it; every other non-canonical spelling remains detectable.
    for (const value of ["0", "1", "65535", "01", "+1", "1.0", "9007199254740992"]) {
      const response = await handler(
        new Request("http://test/ping", {
          method: "POST",
          headers: { "Content-Type": ARROW_CONTENT_TYPE, "VGI-Accept-Max-Response-Bytes": value },
          body: body as unknown as BodyInit,
        }),
      );
      expect(response.status).toBe(400);
      expect(response.headers.get("Content-Type")).toBe(ARROW_CONTENT_TYPE);
      expect(response.headers.get(RPC_ERROR_HEADER)).toBe("true");
      expect(response.headers.get("VGI-Accept-Max-Response-Bytes-Support")).toBe("true");
    }
    expect(calls).toBe(0);
  });

  test("authentication precedes response-budget and body parsing", async () => {
    let calls = 0;
    const protocol = new Protocol("AuthOrderSvc").unary("ping", {
      params: { msg: str },
      result: { msg: str },
      handler: (params) => {
        calls += 1;
        return { msg: String(params.msg) };
      },
    });
    const handler = createHttpHandler(protocol, {
      maxRequestBytes: 65_536,
      authenticate: () => {
        throw new AuthFailure(AuthReason.MissingCredential, "credential required");
      },
    });
    const response = await handler(
      new Request("http://test/ping", {
        method: "POST",
        headers: { "Content-Type": "not-arrow", "VGI-Accept-Max-Response-Bytes": "1" },
        body: new Uint8Array(70_000) as unknown as BodyInit,
      }),
    );
    expect(response.status).toBe(401);
    expect(calls).toBe(0);
  });

  test("unary handler sees externalizationEnabled=false when no storage configured", async () => {
    let enabled: boolean | undefined;
    const protocol = new Protocol("BudgetSvc2").unary("ping", {
      params: { msg: str },
      result: { msg: str },
      handler: (params, ctx) => {
        enabled = (ctx as unknown as { externalizationEnabled?: boolean }).externalizationEnabled;
        return { msg: String(params.msg) };
      },
    });
    const handler = createHttpHandler(protocol, { maxResponseBytes: 65_536 });
    const M = await import("@query-farm/apache-arrow");
    const reqSchema = new Schema([new M.Field("msg", new M.Utf8())]);
    const body = buildRequestIpc(reqSchema, { msg: ["hi"] }, "ping");
    await handler(
      new Request("http://test/ping", {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body: body as unknown as BodyInit,
      }),
    );
    expect(enabled).toBe(false);
  });

  test("negotiated limit rescues a unary result through external storage", async () => {
    const protocol = new Protocol("UnaryRescueSvc").unary("blob", {
      params: { _placeholder: str },
      result: { blob: bytes },
      handler: () => ({ blob: new Uint8Array(128 * 1024) }),
    });
    const storage = new MemoryStorage();
    const handler = createHttpHandler(protocol, {
      maxResponseBytes: 1024 * 1024,
      externalLocation: { storage, externalizeThresholdBytes: 1024 * 1024 },
    });
    const M = await import("@query-farm/apache-arrow");
    const requestSchema = new Schema([new M.Field("_placeholder", new M.Utf8())]);
    const body = buildRequestIpc(requestSchema, { _placeholder: [""] }, "blob");
    const response = await handler(
      new Request("http://test/blob", {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE, "VGI-Accept-Max-Response-Bytes": "65536" },
        body: body as unknown as BodyInit,
      }),
    );

    expect(response.status).toBe(200);
    expect(response.headers.get(RPC_ERROR_HEADER)).toBeNull();
    expect(storage.uploads).toHaveLength(1);
    const { batches } = await readBody(response);
    expect(batches.at(-1)?.metadata?.get(LOCATION_KEY)).toBe("https://memory.test/0");
  });

  test("negotiated limit strictly replaces an inline unary overshoot", async () => {
    const protocol = new Protocol("UnaryStrictSvc").unary("blob", {
      params: { _placeholder: str },
      result: { blob: bytes },
      handler: () => ({ blob: new Uint8Array(128 * 1024) }),
    });
    const handler = createHttpHandler(protocol, { maxResponseBytes: 1024 * 1024 });
    const M = await import("@query-farm/apache-arrow");
    const requestSchema = new Schema([new M.Field("_placeholder", new M.Utf8())]);
    const body = buildRequestIpc(requestSchema, { _placeholder: [""] }, "blob");
    const response = await handler(
      new Request("http://test/blob", {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE, "VGI-Accept-Max-Response-Bytes": "65536" },
        body: body as unknown as BodyInit,
      }),
    );

    expect(response.status).toBe(200);
    expect(response.headers.get(RPC_ERROR_HEADER)).toBe("true");
    expect(Number(response.headers.get("Content-Length"))).toBeLessThanOrEqual(65_536);
    const { batches } = await readBody(response);
    expect(batches.at(-1)?.metadata?.get(LOG_LEVEL_KEY)).toBe("EXCEPTION");
  });
});

describe("producer stream external cap", () => {
  test("a cold continuation cannot raise the initial authenticated response cap", async () => {
    const protocol = new Protocol("SealedBudgetSvc").producer("drip", {
      params: { _placeholder: str },
      outputSchema: { blob: bytes },
      init: () => ({ turn: 0 }),
      produce: (state: { turn: number }, out) => {
        out.emit({ blob: [new Uint8Array(state.turn++ === 0 ? 1 : 128 * 1024)] });
      },
    });
    const handler = createHttpHandler(protocol, { callStateCacheEntries: 0 });
    const M = await import("@query-farm/apache-arrow");
    const reqSchema = new Schema([new M.Field("_placeholder", new M.Utf8())]);
    const init = await handler(
      new Request("http://test/drip/init", {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE, "VGI-Accept-Max-Response-Bytes": "65536" },
        body: buildRequestIpc(reqSchema, { _placeholder: [""] }, "drip") as unknown as BodyInit,
      }),
    );
    const { batches: initBatches } = await readBody(init);
    const cursor = initBatches.find((batch) => batch.metadata?.get(STATE_KEY))?.metadata?.get(STATE_KEY);
    const callToken = initBatches.find((batch) => batch.metadata?.get(CALL_STATE_KEY))?.metadata?.get(CALL_STATE_KEY);
    expect(cursor).toBeDefined();
    expect(callToken).toBeDefined();

    const continuationMetadata = new Map<string, string>([
      [STATE_KEY, cursor!],
      [CALL_STATE_KEY, callToken!],
    ]);
    const continuation = await handler(
      new Request("http://test/drip/exchange", {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE, "VGI-Accept-Max-Response-Bytes": "1048576" },
        body: buildRequestIpc(reqSchema, { _placeholder: [""] }, "drip", continuationMetadata) as unknown as BodyInit,
      }),
    );
    expect(continuation.status).toBe(200);
    expect(continuation.headers.get(RPC_ERROR_HEADER)).toBe("true");
    const { batches } = await readBody(continuation);
    expect(batches.some((batch) => batch.metadata?.has(STATE_KEY) || batch.metadata?.has(CALL_STATE_KEY))).toBe(false);
    expect(batches.at(-1)?.metadata?.get(LOG_LEVEL_KEY)).toBe("EXCEPTION");
  });

  test("producer rejects one externalized batch larger than the per-turn cap", async () => {
    const PAYLOAD_SIZE = 4096;
    let calls = 0;

    const protocol = new Protocol("ExfilSvc").producer("drip", {
      params: { _placeholder: str },
      outputSchema: { blob: bytes },
      init: () => ({ done: false }),
      produce: async (state: { done: boolean }, out) => {
        calls += 1;
        if (state.done) {
          out.finish();
          return;
        }
        // Emit a batch large enough to trip externalization.
        const blob = new Uint8Array(PAYLOAD_SIZE);
        out.emit({ blob: [blob] });
        state.done = true;
      },
    });

    const storage = new MemoryStorage();
    const externalLocation: ExternalLocationConfig = {
      storage,
      externalizeThresholdBytes: 100, // force externalize
    };
    const handler = createHttpHandler(protocol, {
      maxExternalizedResponseBytes: 3 * 1024,
      maxStreamResponseBytes: 1024 * 1024,
      externalLocation,
    });

    const M = await import("@query-farm/apache-arrow");
    const reqSchema = new Schema([new M.Field("_placeholder", new M.Utf8())]);
    const body = buildRequestIpc(reqSchema, { _placeholder: [""] }, "drip");
    const response = await handler(
      new Request("http://test/drip/init", {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE },
        body: body as unknown as BodyInit,
      }),
    );

    // 500 → 200 + X-VGI-RPC-Error: true (the cap-overshoot contract).
    expect(response.status).toBe(200);
    expect(response.headers.get(RPC_ERROR_HEADER)).toBe("true");

    // The strict pre-flight check rejects before uploading anything.
    expect(storage.uploads.length).toBe(0);
    expect(calls).toBe(1);

    // The body's final batch carries an EXCEPTION metadata.
    const { batches } = await readBody(response);
    const last = batches[batches.length - 1];
    expect(last.metadata?.get(LOG_LEVEL_KEY)).toBe("EXCEPTION");
  });

  test("hard response overshoot emits an error-only turn with no cursor", async () => {
    const protocol = new Protocol("StrictProducerSvc").producer("drip", {
      params: { _placeholder: str },
      outputSchema: { blob: bytes },
      init: () => ({ done: false }),
      produce: (state: { done: boolean }, out) => {
        out.emit({ blob: [new Uint8Array(128 * 1024)] });
        state.done = true;
      },
    });
    const handler = createHttpHandler(protocol, { maxResponseBytes: 1024 * 1024 });
    const M = await import("@query-farm/apache-arrow");
    const reqSchema = new Schema([new M.Field("_placeholder", new M.Utf8())]);
    const body = buildRequestIpc(reqSchema, { _placeholder: [""] }, "drip");
    const response = await handler(
      new Request("http://test/drip/init", {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE, "VGI-Accept-Max-Response-Bytes": "65536" },
        body: body as unknown as BodyInit,
      }),
    );
    expect(response.status).toBe(200);
    expect(response.headers.get(RPC_ERROR_HEADER)).toBe("true");
    const { batches } = await readBody(response);
    expect(batches.some((batch) => batch.metadata?.has(STATE_KEY) || batch.metadata?.has(CALL_STATE_KEY))).toBe(false);
    expect(batches.at(-1)?.metadata?.get(LOG_LEVEL_KEY)).toBe("EXCEPTION");
  });

  test("negotiated limit forces externalization below the normal threshold", async () => {
    const protocol = new Protocol("RescueProducerSvc").producer("drip", {
      params: { _placeholder: str },
      outputSchema: { blob: bytes },
      init: () => ({ done: false }),
      produce: (_state: { done: boolean }, out) => out.emit({ blob: [new Uint8Array(128 * 1024)] }),
    });
    const storage = new MemoryStorage();
    const handler = createHttpHandler(protocol, {
      maxResponseBytes: 1024 * 1024,
      externalLocation: { storage, externalizeThresholdBytes: 1024 * 1024 },
    });
    const M = await import("@query-farm/apache-arrow");
    const reqSchema = new Schema([new M.Field("_placeholder", new M.Utf8())]);
    const body = buildRequestIpc(reqSchema, { _placeholder: [""] }, "drip");
    const response = await handler(
      new Request("http://test/drip/init", {
        method: "POST",
        headers: { "Content-Type": ARROW_CONTENT_TYPE, "VGI-Accept-Max-Response-Bytes": "65536" },
        body: body as unknown as BodyInit,
      }),
    );
    expect(response.status).toBe(200);
    expect(response.headers.get(RPC_ERROR_HEADER)).toBeNull();
    expect(storage.uploads).toHaveLength(1);
    const { batches } = await readBody(response);
    expect(batches.some((batch) => batch.metadata?.has(STATE_KEY))).toBe(true);
  });
});
