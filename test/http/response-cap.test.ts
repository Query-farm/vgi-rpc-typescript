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
  LOG_LEVEL_KEY,
  REQUEST_VERSION,
  REQUEST_VERSION_KEY,
  RPC_ERROR_HEADER,
  RPC_METHOD_KEY,
} from "../../src/constants.js";
import type { ExternalLocationConfig, ExternalStorage } from "../../src/external.js";
import { ARROW_CONTENT_TYPE, bytes, createHttpHandler, Protocol, str } from "../../src/index.js";

function buildRequestIpc(schema: Schema, values: Record<string, any[]>, methodName: string): Uint8Array {
  const batch = recordBatchFromArrays(values, schema);
  const meta = new Map<string, string>();
  meta.set(RPC_METHOD_KEY, methodName);
  meta.set(REQUEST_VERSION_KEY, REQUEST_VERSION);
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
      maxResponseBytes: 1024,
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
    expect(observed.wire).toBe(1024);
    expect(observed.ext).toBe(4096);
    expect(observed.enabled).toBe(true);
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
    const handler = createHttpHandler(protocol, { maxResponseBytes: 1024 });
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
});

describe("producer stream external cap", () => {
  test("producer exfiltrating via tiny pointer batches still hits the external cap", async () => {
    // Each emit will externalize ~2KiB worth of data; the cap is 3KiB,
    // so the second emit must be refused.
    const PAYLOAD_SIZE = 2048;
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
        if (calls >= 5) state.done = true; // safety cutoff
      },
    });

    const storage = new MemoryStorage();
    const externalLocation: ExternalLocationConfig = {
      storage,
      externalizeThresholdBytes: 100, // force externalize
    };
    const handler = createHttpHandler(protocol, {
      maxExternalizedResponseBytes: 3 * 1024,
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

    // Only one externalized upload should have completed before the cap
    // overshoot replaced the response with an EXCEPTION batch.
    expect(storage.uploads.length).toBe(1);

    // The body's final batch carries an EXCEPTION metadata.
    const { batches } = await readBody(response);
    const last = batches[batches.length - 1];
    expect(last.metadata?.get(LOG_LEVEL_KEY)).toBe("EXCEPTION");
  });
});
