// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Raw-TCP transport tests: bind serveTcp on an OS-selected loopback port,
 * connect with tcpConnect, and exercise the unary / producer / exchange
 * round-trips over the same raw Arrow-IPC framing the AF_UNIX runner uses.
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { tcpConnect } from "../src/client/tcp.js";
import { type ServeTcpHandle, serveTcp } from "../src/launcher/serve-tcp.js";
import { Protocol } from "../src/protocol.js";
import { int, str } from "../src/schema.js";
import { TransportKind } from "../src/types.js";

const protocol = new Protocol("TcpSvc")
  .unary("ping", {
    params: { msg: str },
    result: { msg: str },
    handler: async (params) => ({ msg: `pong:${params.msg}` }),
  })
  .producer<{ n: number; current: number }>("count", {
    params: { n: int },
    outputSchema: { value: int },
    init: ({ n }) => ({ n: Number(n), current: 0 }),
    produce: (state, out) => {
      if (state.current >= state.n) {
        out.finish();
        return;
      }
      out.emitRow({ value: state.current });
      state.current++;
    },
  });

describe("serveTcp + tcpConnect", () => {
  let handle: ServeTcpHandle;

  beforeEach(async () => {
    handle = await serveTcp(protocol, {
      host: "127.0.0.1",
      port: 0,
      idleTimeout: 0,
      announcementSink: { write: () => true } as unknown as NodeJS.WritableStream,
    });
  });

  afterEach(async () => {
    await handle.stop();
  });

  test("binds loopback on an OS-selected port", () => {
    expect(handle.host).toBe("127.0.0.1");
    expect(handle.port).toBeGreaterThan(0);
  });

  test("unary round-trip over TCP", async () => {
    const client = tcpConnect(handle.host, handle.port);
    try {
      const result = await client.call("ping", { msg: "hi" });
      expect(result).toEqual({ msg: "pong:hi" });
    } finally {
      client.close();
    }
  });

  test("producer stream round-trip over TCP", async () => {
    const client = tcpConnect(handle.host, handle.port);
    try {
      const rows: number[] = [];
      const session = await client.stream("count", { n: 3 });
      for await (const batch of session) {
        for (const row of batch) rows.push(row.value as number);
      }
      session.close();
      expect(rows).toEqual([0, 1, 2]);
    } finally {
      client.close();
    }
  });

  test("TransportKind exposes TCP", () => {
    expect(TransportKind.TCP).toBe("tcp");
  });
});
