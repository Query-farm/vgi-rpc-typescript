// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * A stream call that neither emits a data batch nor finishes is an error.
 *
 * Before, the dispatchers wrote nothing for such a call. Over the byte-stream
 * transports (stdio, unix, tcp) the client then waited forever for the batch
 * its tick asked for; over HTTP it got a bare cursor back and re-polled the
 * same call forever. vgi-python raises "No data batch was emitted"
 * (`OutputCollector.validate()`); these tests pin the same behavior here.
 */

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import type { RpcClient } from "../src/client/index.js";
import { httpConnect } from "../src/client/index.js";
import { tcpConnect } from "../src/client/tcp.js";
import { createHttpHandler } from "../src/http/handler.js";
import { type ServeTcpHandle, serveTcp } from "../src/launcher/serve-tcp.js";
import { Protocol } from "../src/protocol.js";
import { float, int } from "../src/schema.js";

const protocol = new Protocol("EmptyCall")
  // Emits one row, then returns without emitting or finishing.
  .producer<{ calls: number }>("stall", {
    params: {},
    outputSchema: { value: int },
    init: () => ({ calls: 0 }),
    produce: (state, out) => {
      if (state.calls++ === 0) out.emitRow({ value: 1 });
    },
  })
  // Finishes without emitting on its first call: still a clean empty stream.
  .producer<Record<string, never>>("finish_only", {
    params: {},
    outputSchema: { value: int },
    init: () => ({}),
    produce: (_state, out) => out.finish(),
  })
  .exchange<Record<string, never>>("swallow", {
    params: {},
    inputSchema: { value: float },
    outputSchema: { value: float },
    init: () => ({}),
    exchange: () => {},
  });

/** Drain a producer stream; rejects if it errors, times out if it hangs. */
async function drain(client: RpcClient, method: string): Promise<number> {
  const session = await client.stream(method, {});
  let rows = 0;
  try {
    for await (const batch of session) rows += batch.length;
  } finally {
    session.close();
  }
  return rows;
}

function suite(name: string, connect: () => RpcClient) {
  describe(name, () => {
    test("a producer call that emits nothing and does not finish raises", async () => {
      const client = connect();
      try {
        await expect(drain(client, "stall")).rejects.toThrow("No data batch was emitted");
      } finally {
        client.close();
      }
    }, 10_000);

    test("a producer that only finishes is an empty stream", async () => {
      const client = connect();
      try {
        expect(await drain(client, "finish_only")).toBe(0);
      } finally {
        client.close();
      }
    }, 10_000);

    test("an exchange call that emits nothing raises", async () => {
      const client = connect();
      try {
        const session = await client.stream("swallow", {});
        try {
          await expect(session.exchange([{ value: 1.0 }])).rejects.toThrow("No data batch was emitted");
        } finally {
          session.close();
        }
      } finally {
        client.close();
      }
    }, 10_000);
  });
}

let tcp: ServeTcpHandle;
let http: ReturnType<typeof Bun.serve>;

beforeAll(async () => {
  tcp = await serveTcp(protocol, {
    host: "127.0.0.1",
    port: 0,
    idleTimeout: 0,
    announcementSink: { write: () => true } as unknown as NodeJS.WritableStream,
  });
  http = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: createHttpHandler(protocol) });
});

afterAll(async () => {
  await tcp.stop();
  http.stop(true);
});

suite("byte-stream transport (tcp)", () => tcpConnect(tcp.host, tcp.port));
suite("http transport", () => httpConnect(`http://127.0.0.1:${http.port}`));
