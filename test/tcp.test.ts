// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Raw-TCP transport tests: bind serveTcp on an OS-selected loopback port,
 * connect with tcpConnect, and exercise the unary / producer / exchange
 * round-trips over the same raw Arrow-IPC framing the AF_UNIX runner uses.
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { tcpConnect } from "../src/client/tcp.js";
import {
  IdentityAssurance,
  PeerIdentity,
  PeerIdentityResult,
  PeerIdentityStatus,
  PeerSubjectKind,
  peerIdentityPrimary,
  SubjectStability,
} from "../src/identity.js";
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
      announcementSink: {
        write: () => true,
      } as unknown as NodeJS.WritableStream,
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

  test("resolves and snapshots peer identity once per TCP connection", async () => {
    await handle.stop();
    let resolutions = 0;
    const identityProtocol = new Protocol("IdentityTcpSvc").unary("whoami", {
      params: { msg: str },
      result: { msg: str },
      handler: ({ msg }, context) => ({
        msg: `${msg}:${context.auth.domain}:${context.peerEvidence.status("test-peer")}`,
      }),
    });
    handle = await serveTcp(identityProtocol, {
      host: "127.0.0.1",
      port: 0,
      idleTimeout: 0,
      announcementSink: {
        write: () => true,
      } as unknown as NodeJS.WritableStream,
      peerIdentityProviders: [
        {
          provider: "test-peer",
          resolve: (context) => {
            resolutions += 1;
            expect(context.transport).toBe("tcp");
            expect(context.sourceEndpoint).toContain(":");
            return PeerIdentityResult.available(
              new PeerIdentity({
                provider: "test-peer",
                evidenceSource: "test_socket",
                assurance: IdentityAssurance.CRYPTOGRAPHIC_PEER,
                issuer: "test-issuer",
                transport: "tcp",
                subjectKind: PeerSubjectKind.WORKLOAD,
                subjectKey: "worker-1",
                subjectStability: SubjectStability.STABLE,
                subjectVerified: true,
                sourceAddress: context.sourceEndpoint,
              }),
            );
          },
        },
      ],
      peerAuthenticationPolicy: peerIdentityPrimary("test-peer"),
    });

    const client = tcpConnect(handle.host, handle.port);
    try {
      expect(await client.call("whoami", { msg: "first" })).toEqual({
        msg: "first:test-peer:available",
      });
      expect(await client.call("whoami", { msg: "second" })).toEqual({
        msg: "second:test-peer:available",
      });
      expect(resolutions).toBe(1);
    } finally {
      client.close();
    }
  });

  test("preserves completed invalid evidence when a sibling provider times out", async () => {
    await handle.stop();
    let observed: readonly string[] = [];
    handle = await serveTcp(protocol, {
      host: "127.0.0.1",
      port: 0,
      idleTimeout: 0,
      announcementSink: {
        write: () => true,
      } as unknown as NodeJS.WritableStream,
      identityResolutionTimeoutMs: 20,
      peerIdentityProviders: [
        {
          provider: "invalid",
          resolve: () => new PeerIdentityResult("invalid", PeerIdentityStatus.INVALID),
        },
        {
          provider: "hung",
          resolve: () => new Promise<PeerIdentityResult>(() => {}),
        },
      ],
      peerAuthenticationPolicy: (evidence, auth) => {
        observed = [evidence.status("invalid"), evidence.status("hung")];
        return auth;
      },
    });
    const client = tcpConnect(handle.host, handle.port);
    try {
      expect(await client.call("ping", { msg: "hi" })).toEqual({
        msg: "pong:hi",
      });
      expect(observed).toEqual([PeerIdentityStatus.INVALID, PeerIdentityStatus.UNAVAILABLE]);
    } finally {
      client.close();
    }
  });

  test("bounds provider calls across concurrent TCP connections", async () => {
    await handle.stop();
    let resolutions = 0;
    handle = await serveTcp(protocol, {
      host: "127.0.0.1",
      port: 0,
      idleTimeout: 0,
      announcementSink: {
        write: () => true,
      } as unknown as NodeJS.WritableStream,
      identityResolutionTimeoutMs: 50,
      peerProviderConcurrency: 1,
      peerIdentityProviders: [
        {
          provider: "hung",
          resolve: () => {
            resolutions++;
            return new Promise<PeerIdentityResult>(() => {});
          },
        },
      ],
      peerAuthenticationPolicy: (_evidence, auth) => auth,
    });
    const first = tcpConnect(handle.host, handle.port);
    const second = tcpConnect(handle.host, handle.port);
    try {
      const firstCall = first.call("ping", { msg: "first" });
      while (resolutions === 0) await Bun.sleep(1);
      expect(await second.call("ping", { msg: "second" })).toEqual({
        msg: "pong:second",
      });
      expect(await firstCall).toEqual({ msg: "pong:first" });
      expect(resolutions).toBe(1);
    } finally {
      first.close();
      second.close();
    }
  });

  test("redacts peer policy exception details from TCP connection logs", async () => {
    await handle.stop();
    const secret = "LOCALAPI_TOKEN_DO_NOT_LOG";
    let logged = "";
    const originalWrite = process.stderr.write;
    process.stderr.write = ((chunk: string | Uint8Array) => {
      logged += String(chunk);
      return true;
    }) as typeof process.stderr.write;
    try {
      handle = await serveTcp(protocol, {
        host: "127.0.0.1",
        port: 0,
        idleTimeout: 0,
        announcementSink: { write: () => true } as unknown as NodeJS.WritableStream,
        peerIdentityProviders: [
          {
            provider: "test",
            resolve: () => new PeerIdentityResult("test", PeerIdentityStatus.INVALID),
          },
        ],
        peerAuthenticationPolicy: () => {
          throw new Error(secret);
        },
      });
      const client = tcpConnect(handle.host, handle.port);
      try {
        await expect(client.call("ping", { msg: "hi" })).rejects.toThrow();
      } finally {
        client.close();
      }
      await Bun.sleep(5);
      expect(logged).toContain("connection identity failed");
      expect(logged).not.toContain(secret);
    } finally {
      process.stderr.write = originalWrite;
    }
  });

  test("TransportKind exposes TCP", () => {
    expect(TransportKind.TCP).toBe("tcp");
  });

  test("concurrent first connections share one startup hook and success is sticky", async () => {
    await handle.stop();
    let release!: () => void;
    let markEntered!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const entered = new Promise<void>((resolve) => {
      markEntered = resolve;
    });
    let hookCalls = 0;
    handle = await serveTcp(protocol, {
      host: "127.0.0.1",
      port: 0,
      idleTimeout: 0,
      announcementSink: {
        write: () => true,
      } as unknown as NodeJS.WritableStream,
      onServeStart: async (kind) => {
        hookCalls += 1;
        expect(kind).toBe(TransportKind.TCP);
        markEntered();
        await gate;
      },
    });

    const clients = Array.from({ length: 8 }, () => tcpConnect(handle.host, handle.port));
    try {
      const calls = clients.map((client, index) => client.call("ping", { msg: String(index) }));
      await entered;
      expect(hookCalls).toBe(1);
      release();
      expect((await Promise.all(calls)).map((result) => result?.msg)).toEqual(
        Array.from({ length: 8 }, (_, index) => `pong:${index}`),
      );

      const later = tcpConnect(handle.host, handle.port);
      try {
        expect(await later.call("ping", { msg: "later" })).toEqual({
          msg: "pong:later",
        });
      } finally {
        later.close();
      }
      expect(hookCalls).toBe(1);
    } finally {
      for (const client of clients) client.close();
    }
  });
});
