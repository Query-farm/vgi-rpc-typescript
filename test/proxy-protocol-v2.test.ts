// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import { createConnection, createServer, type Server, type Socket } from "node:net";
import { tcpConnect } from "../src/client/tcp.js";
import { PeerIdentityResult, PeerIdentityStatus, peerIdentityPrimary } from "../src/identity.js";
import {
  normalizeProxyIpAddress,
  ProxyProtocolV2Error,
  parseIrohProxyProtocolV2,
  parseProxyProtocolV2,
  VGI_IROH_ENDPOINT_TLV,
} from "../src/launcher/proxy-protocol-v2.js";
import { type ServeTcpHandle, serveTcp } from "../src/launcher/serve-tcp.js";
import { Protocol } from "../src/protocol.js";
import { str } from "../src/schema.js";

const SIGNATURE = Buffer.from([0x0d, 0x0a, 0x0d, 0x0a, 0, 0x0d, 0x0a, 0x51, 0x55, 0x49, 0x54, 0x0a]);
const protocol = new Protocol("ProxyV2Test").unary("ping", {
  params: { value: str },
  result: { value: str },
  handler: ({ value }) => ({ value: `pong:${value}` }),
});

function ipv4Header(tlv = Buffer.alloc(0)): Buffer {
  const address = Buffer.from([192, 0, 2, 7, 198, 51, 100, 9, 0x30, 0x39, 0x24, 0xb8]);
  const length = address.length + tlv.length;
  return Buffer.concat([SIGNATURE, Buffer.from([0x21, 0x11, length >> 8, length & 0xff]), address, tlv]);
}

function ipv6Header(): Buffer {
  const address = Buffer.alloc(36);
  // IPv4-mapped 192.0.2.7, plus 2001:db8::9.
  address.set([0xff, 0xff, 192, 0, 2, 7], 10);
  address.set([0x20, 0x01, 0x0d, 0xb8], 16);
  address[31] = 9;
  address.writeUInt16BE(12345, 32);
  address.writeUInt16BE(9400, 34);
  return Buffer.concat([SIGNATURE, Buffer.from([0x21, 0x21, 0, address.length]), address]);
}

function irohHeader(endpoint = Buffer.from(Array.from({ length: 32 }, (_unused, index) => index))): Buffer {
  const tlv = Buffer.concat([Buffer.from([VGI_IROH_ENDPOINT_TLV, 0, 33, 1]), endpoint]);
  return Buffer.concat([SIGNATURE, Buffer.from([0x21, 0, 0, tlv.length]), tlv]);
}

async function listen(server: Server): Promise<number> {
  server.listen(0, "127.0.0.1");
  await new Promise<void>((resolve, reject) => {
    server.once("listening", resolve);
    server.once("error", reject);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("test listener has no TCP address");
  return address.port;
}

async function connected(port: number): Promise<Socket> {
  const socket = createConnection({ host: "127.0.0.1", port });
  await new Promise<void>((resolve, reject) => {
    socket.once("connect", resolve);
    socket.once("error", reject);
  });
  return socket;
}

async function closes(socket: Socket, timeoutMs = 250): Promise<void> {
  await Promise.race([
    new Promise<void>((resolve) => {
      socket.once("close", () => resolve());
    }),
    Bun.sleep(timeoutMs).then(() => {
      throw new Error("connection did not close within the admission deadline");
    }),
  ]);
}

describe("PROXY protocol v2 parser", () => {
  test("accepts TCP/IPv4 and bounded unknown TLVs", () => {
    const parsed = parseProxyProtocolV2(ipv4Header(Buffer.from([0xee, 0, 2, 0xaa, 0xbb])));
    expect(parsed).toEqual({
      source: { address: "192.0.2.7", port: 12345 },
      destination: { address: "198.51.100.9", port: 9400 },
    });
  });

  test("accepts TCP/IPv6 and normalizes IPv4-mapped IPv6", () => {
    const parsed = parseProxyProtocolV2(ipv6Header());
    expect(parsed.source).toEqual({ address: "192.0.2.7", port: 12345 });
    expect(parsed.destination).toEqual({ address: "2001:db8::9", port: 9400 });
    expect(normalizeProxyIpAddress("::ffff:127.0.0.1")).toBe("127.0.0.1");
  });

  test("rejects unsupported commands and address families", () => {
    for (const [versionCommand, family] of [
      [0x20, 0x11], // LOCAL
      [0x11, 0x11], // version 1
      [0x21, 0x00], // UNSPEC
      [0x21, 0x12], // UDP/IPv4
      [0x21, 0x31], // Unix stream
    ]) {
      const value = ipv4Header();
      value[12] = versionCommand;
      value[13] = family;
      expect(() => parseProxyProtocolV2(value)).toThrow(ProxyProtocolV2Error);
    }
  });

  test("rejects signatures, truncation, overlong values, malformed TLVs, and configured oversize", () => {
    const signature = ipv4Header();
    signature[0] ^= 0xff;
    const overlong = Buffer.concat([ipv4Header(), Buffer.from([0])]);
    const badTlv = ipv4Header(Buffer.from([0xee, 0, 2, 0xaa]));
    for (const value of [signature, ipv4Header().subarray(0, 15), ipv4Header().subarray(0, -1), overlong, badTlv]) {
      expect(() => parseProxyProtocolV2(value)).toThrow(ProxyProtocolV2Error);
    }
    expect(() => parseProxyProtocolV2(ipv4Header(), 20)).toThrow("configured limit");
  });

  test("rejects non-exact trusted proxy address syntax", () => {
    for (const value of ["127.0.0.1/32", "localhost", "[::1]", "fe80::1%lo0", "01.2.3.4"]) {
      expect(() => normalizeProxyIpAddress(value)).toThrow(TypeError);
    }
  });

  test("accepts Iroh identity only through the dedicated PROXY/UNSPEC parser", () => {
    expect(() => parseProxyProtocolV2(irohHeader())).toThrow(ProxyProtocolV2Error);
    expect(parseIrohProxyProtocolV2(irohHeader())).toEqual({
      endpointId: "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
    });
    const extended = Buffer.concat([irohHeader(), Buffer.from([0xee, 0, 1, 7])]);
    extended.writeUInt16BE(extended.length - 16, 14);
    expect(parseIrohProxyProtocolV2(extended)).toEqual({
      endpointId: "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
    });
    const duplicate = Buffer.concat([irohHeader(), irohHeader().subarray(16)]);
    duplicate.writeUInt16BE(duplicate.length - 16, 14);
    const missing = Buffer.concat([SIGNATURE, Buffer.from([0x21, 0, 0, 0])]);
    const wrongVersion = irohHeader();
    wrongVersion[19] = 2;
    const ipFamily = irohHeader();
    ipFamily[13] = 0x11;
    for (const value of [duplicate, missing, wrongVersion, ipFamily]) {
      expect(() => parseIrohProxyProtocolV2(value)).toThrow(ProxyProtocolV2Error);
    }
  });
});

describe("serveTcp PROXY protocol v2 admission", () => {
  const sink = { write: () => true } as unknown as NodeJS.WritableStream;

  test("requires exact trust and validates bounds at startup", async () => {
    await expect(
      serveTcp(protocol, {
        idleTimeout: 0,
        announcementSink: sink,
        proxyProtocolV2Required: true,
      }),
    ).rejects.toThrow("trusted proxy");
    await expect(
      serveTcp(protocol, {
        idleTimeout: 0,
        announcementSink: sink,
        trustedProxyAddresses: ["127.0.0.0/8"],
      }),
    ).rejects.toThrow("exact IPv4 or IPv6");
    await expect(
      serveTcp(protocol, {
        idleTimeout: 0,
        announcementSink: sink,
        trustedProxyAddresses: ["127.0.0.1", "::ffff:127.0.0.1"],
      }),
    ).rejects.toThrow("duplicate trusted proxy");
    await expect(
      serveTcp(protocol, {
        idleTimeout: 0,
        announcementSink: sink,
        proxyProtocolV2Required: true,
        trustedProxyAddresses: ["127.0.0.1"],
        irohProxyIssuer: "production\tmesh",
      }),
    ).rejects.toThrow("without controls");
  });

  test("rejects an untrusted immediate peer before waiting for bytes", async () => {
    const handle = await serveTcp(protocol, {
      host: "127.0.0.1",
      port: 0,
      idleTimeout: 0,
      announcementSink: sink,
      proxyProtocolV2Required: true,
      trustedProxyAddresses: ["127.0.0.2"],
      proxyPreambleTimeoutMs: 5_000,
    });
    try {
      const socket = await connected(handle.port);
      await closes(socket);
    } finally {
      await handle.stop();
    }
  });

  test("uses one short preamble deadline against a slow client", async () => {
    const handle = await serveTcp(protocol, {
      host: "127.0.0.1",
      port: 0,
      idleTimeout: 0,
      announcementSink: sink,
      proxyProtocolV2Required: true,
      trustedProxyAddresses: ["127.0.0.1"],
      proxyPreambleTimeoutMs: 25,
    });
    try {
      const socket = await connected(handle.port);
      socket.write(SIGNATURE.subarray(0, 1));
      await Bun.sleep(12);
      socket.write(SIGNATURE.subarray(1, 2));
      await closes(socket, 150);
    } finally {
      await handle.stop();
    }
  });

  test("closes malformed, truncated, and oversized wire preambles", async () => {
    const handle = await serveTcp(protocol, {
      host: "127.0.0.1",
      port: 0,
      idleTimeout: 0,
      announcementSink: sink,
      proxyProtocolV2Required: true,
      trustedProxyAddresses: ["127.0.0.1"],
      proxyPreambleTimeoutMs: 100,
      maximumProxyPreambleBytes: 536,
    });
    try {
      const malformed = ipv4Header();
      malformed[0] ^= 0xff;
      const oversized = Buffer.concat([SIGNATURE, Buffer.from([0x21, 0x11, 0x02, 0x09])]);
      const badTlv = ipv4Header(Buffer.from([0xee, 0, 2, 0xaa]));
      for (const value of [malformed, oversized, badTlv]) {
        const socket = await connected(handle.port);
        const closed = closes(socket);
        socket.write(value);
        await closed;
      }
      const truncated = await connected(handle.port);
      const closed = closes(truncated);
      truncated.end(ipv4Header().subarray(0, -1));
      await closed;
    } finally {
      await handle.stop();
    }
  });

  test("preserves coalesced Arrow bytes and snapshots asserted identity once", async () => {
    let handle: ServeTcpHandle | undefined;
    let relay: Server | undefined;
    let resolutions = 0;
    const contexts: Array<{
      immediatePeer?: string;
      sourceEndpoint?: string;
      assertedPeer?: string;
      destinationAddress?: string;
      proxyProtocol?: unknown;
    }> = [];
    try {
      handle = await serveTcp(protocol, {
        host: "127.0.0.1",
        port: 0,
        idleTimeout: 0,
        announcementSink: sink,
        proxyProtocolV2Required: true,
        trustedProxyAddresses: ["::ffff:127.0.0.1"],
        peerIdentityProviders: [
          {
            provider: "capture",
            resolve: (context) => {
              resolutions++;
              contexts.push({
                immediatePeer: context.immediatePeer,
                sourceEndpoint: context.sourceEndpoint,
                assertedPeer: context.assertedPeer,
                destinationAddress: context.destinationAddress,
                proxyProtocol: context.metadata.proxy_protocol_v2,
              });
              return new PeerIdentityResult("capture", PeerIdentityStatus.NO_MATCH);
            },
          },
        ],
      });
      const backendPort = handle.port;
      relay = createServer((downstream) => {
        const upstream = createConnection({
          host: "127.0.0.1",
          port: backendPort,
        });
        downstream.once("data", (firstRequestBytes) => {
          // Deliberately put the preamble and the first Arrow request in one
          // write. The server must consume only the declared preamble.
          upstream.write(Buffer.concat([ipv4Header(), firstRequestBytes]));
          downstream.pipe(upstream);
          upstream.pipe(downstream);
        });
        const closeBoth = () => {
          downstream.destroy();
          upstream.destroy();
        };
        downstream.once("error", closeBoth);
        upstream.once("error", closeBoth);
      });
      const relayPort = await listen(relay);
      const client = tcpConnect("127.0.0.1", relayPort);
      try {
        expect(await client.call("ping", { value: "first" })).toEqual({
          value: "pong:first",
        });
        expect(await client.call("ping", { value: "second" })).toEqual({
          value: "pong:second",
        });
      } finally {
        client.close();
      }
      expect(resolutions).toBe(1);
      expect(contexts).toEqual([
        {
          immediatePeer: "127.0.0.1",
          sourceEndpoint: expect.stringMatching(/^127\.0\.0\.1:/u),
          assertedPeer: "192.0.2.7:12345",
          destinationAddress: "198.51.100.9:9400",
          proxyProtocol: true,
        },
      ]);
    } finally {
      if (relay) await new Promise<void>((resolve) => relay?.close(() => resolve()));
      if (handle) await handle.stop();
    }
  });

  test("promotes one bridge-verified Iroh EndpointId for the connection", async () => {
    let handle: ServeTcpHandle | undefined;
    let relay: Server | undefined;
    try {
      handle = await serveTcp(protocol, {
        host: "127.0.0.1",
        port: 0,
        idleTimeout: 0,
        announcementSink: sink,
        proxyProtocolV2Required: true,
        trustedProxyAddresses: ["127.0.0.1"],
        irohProxyIssuer: "production-mesh",
        peerAuthenticationPolicy: peerIdentityPrimary("iroh"),
      });
      const backendPort = handle.port;
      relay = createServer((downstream) => {
        const upstream = createConnection({
          host: "127.0.0.1",
          port: backendPort,
        });
        downstream.once("data", (firstRequestBytes) => {
          upstream.write(Buffer.concat([irohHeader(), firstRequestBytes]));
          downstream.pipe(upstream);
          upstream.pipe(downstream);
        });
      });
      const client = tcpConnect("127.0.0.1", await listen(relay));
      try {
        expect(await client.call("ping", { value: "iroh" })).toEqual({
          value: "pong:iroh",
        });
      } finally {
        client.close();
      }
    } finally {
      if (relay) await new Promise<void>((resolve) => relay?.close(() => resolve()));
      if (handle) await handle.stop();
    }
  });

  test("keeps ordinary IP PROXY connections compatible when Iroh opt-in is enabled", async () => {
    let handle: ServeTcpHandle | undefined;
    let relay: Server | undefined;
    try {
      handle = await serveTcp(protocol, {
        host: "127.0.0.1",
        port: 0,
        idleTimeout: 0,
        announcementSink: sink,
        proxyProtocolV2Required: true,
        trustedProxyAddresses: ["127.0.0.1"],
        irohProxyIssuer: "production-mesh",
      });
      const backendPort = handle.port;
      relay = createServer((downstream) => {
        const upstream = createConnection({
          host: "127.0.0.1",
          port: backendPort,
        });
        downstream.once("data", (firstRequestBytes) => {
          upstream.write(Buffer.concat([ipv4Header(), firstRequestBytes]));
          downstream.pipe(upstream);
          upstream.pipe(downstream);
        });
      });
      const client = tcpConnect("127.0.0.1", await listen(relay));
      try {
        expect(await client.call("ping", { value: "ip" })).toEqual({
          value: "pong:ip",
        });
      } finally {
        client.close();
      }
    } finally {
      if (relay) await new Promise<void>((resolve) => relay?.close(() => resolve()));
      if (handle) await handle.stop();
    }
  });
});
