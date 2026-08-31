// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { afterEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { createServer, type Server } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PeerIdentityStatus, PeerResolutionContext, PeerSubjectKind, SubjectStability } from "../src/identity.js";
import { tailscaleLocalApiIdentityProvider, tailscaleServeIdentityProvider } from "../src/tailscale.js";

const servers: Server[] = [];
afterEach(async () => {
  for (const server of servers.splice(0)) {
    server.close();
    server.closeAllConnections();
  }
});

function context(options: ConstructorParameters<typeof PeerResolutionContext>[1]): PeerResolutionContext {
  return new PeerResolutionContext("http", options);
}

function whoIs(tagged = false): string {
  return JSON.stringify({
    Node: { StableID: "n123CNTRL", Name: "client.example.ts.net.", Tags: tagged ? ["tag:worker"] : [] },
    UserProfile: { ID: 123, LoginName: "alice@example.com", DisplayName: "Alice Architect" },
    CapMap: { "example.com/cap/run": [{ queue: "blue" }] },
  });
}

// Pinned from vgi-rpc-python transport_identity_vectors.json v1.
// Whole-file SHA-256: 3667fc1e15e11fb2b134c55ed88895779868c56c9385f1e45c3e9f5a50797518.
const TAILSCALE_SERVE_VECTORS = [
  {
    name: "login",
    headers: { "Tailscale-User-Login": ["alice@example.com"], "Tailscale-User-Name": ["Alice"] },
    expected: PeerIdentityStatus.AVAILABLE,
    stability: SubjectStability.LOGIN,
  },
  {
    name: "capability_only",
    headers: { "Tailscale-App-Capabilities": ['{"query.farm/can-run":[{"worker":"analytics"}]}'] },
    expected: PeerIdentityStatus.AVAILABLE,
    stability: SubjectStability.NONE,
  },
  {
    name: "funnel",
    headers: { "Tailscale-Funnel-Request": ["?1"], "Tailscale-User-Login": ["spoof@example.com"] },
    expected: PeerIdentityStatus.NOT_APPLICABLE,
  },
  {
    name: "malformed_capability",
    headers: { "Tailscale-App-Capabilities": ['{"query.farm/can-run":{}}'] },
    expected: PeerIdentityStatus.INVALID,
  },
  {
    name: "profile_without_login",
    headers: { "Tailscale-User-Name": ["Alice"] },
    expected: PeerIdentityStatus.INVALID,
  },
] as const;

describe("Tailscale Serve identity", () => {
  test("requires unique normalized exact trusted proxy IPs", () => {
    expect(() =>
      tailscaleServeIdentityProvider({ issuer: "tailnet:x", trustedProxyAddresses: ["proxy.example"] }),
    ).toThrow();
    expect(() =>
      tailscaleServeIdentityProvider({ issuer: "tailnet:x", trustedProxyAddresses: ["10.0.0.0/8"] }),
    ).toThrow();
    expect(() =>
      tailscaleServeIdentityProvider({
        issuer: "tailnet:x",
        trustedProxyAddresses: ["127.0.0.1", "::ffff:127.0.0.1"],
      }),
    ).toThrow();
  });

  test("matches the canonical Serve adapter vectors", async () => {
    const provider = tailscaleServeIdentityProvider({ issuer: "tailnet:x", trustedProxyAddresses: ["127.0.0.1"] });
    for (const vector of TAILSCALE_SERVE_VECTORS) {
      const resolved = await provider.resolve(context({ immediatePeer: "127.0.0.1", headers: vector.headers }));
      expect(resolved.status, vector.name).toBe(vector.expected);
      if ("stability" in vector) expect(resolved.identities[0].subjectStability, vector.name).toBe(vector.stability);
    }
  });

  test("accepts exact proxy evidence, strict Q text, and capability-only evidence", async () => {
    const provider = tailscaleServeIdentityProvider({
      issuer: "tailnet:example",
      trustedProxyAddresses: ["127.0.0.1"],
    });
    const resolved = await provider.resolve(
      context({
        immediatePeer: "::ffff:127.0.0.1",
        assertedPeer: "100.64.0.1",
        headers: {
          "Tailscale-User-Login": ["alice@example.com"],
          "Tailscale-User-Name": ["=?utf-8?q?Ferris_B=C3=BCller?="],
          "Tailscale-App-Capabilities": ['{"example.com/cap/run":[{"queue":"blue"}]}'],
        },
      }),
    );
    expect(resolved.status).toBe(PeerIdentityStatus.AVAILABLE);
    expect(resolved.identities[0].attributes.user_display_name).toBe("Ferris Büller");
    expect(resolved.identities[0].subjectStability).toBe(SubjectStability.LOGIN);

    const capabilityOnly = await provider.resolve(
      context({
        immediatePeer: "127.0.0.1",
        headers: { "Tailscale-App-Capabilities": ['{"example.com/cap/monitor":[{}]}'] },
      }),
    );
    expect(capabilityOnly.identities[0].subjectKey).toBeUndefined();
    expect(capabilityOnly.identities[0].capabilitiesVerified).toBe(true);
  });

  test("fails closed on untrusted, Funnel, duplicate, malformed, and duplicate-JSON evidence", async () => {
    const provider = tailscaleServeIdentityProvider({ issuer: "tailnet:x", trustedProxyAddresses: ["127.0.0.1"] });
    expect(
      (
        await provider.resolve(
          context({
            immediatePeer: "127.0.0.2",
            headers: { "Tailscale-User-Login": ["admin@example.com"] },
          }),
        )
      ).status,
    ).toBe(PeerIdentityStatus.UNTRUSTED_PROXY);
    expect(
      (
        await provider.resolve(
          context({
            immediatePeer: "127.0.0.1",
            headers: { "Tailscale-Funnel-Request": ["?1"] },
          }),
        )
      ).status,
    ).toBe(PeerIdentityStatus.NOT_APPLICABLE);
    const badHeaders = [
      { "Tailscale-User-Login": ["one", "two"] },
      { "Tailscale-User-Login": ["=?utf-8?b?YWxpY2U=?="] },
      { "Tailscale-User-Name": ["Alice"] },
      { "Tailscale-App-Capabilities": ['{"example.com/cap/run":[],"example.com/cap/run":[]}'] },
      { "Tailscale-App-Capabilities": ['{"example.com/cap/run":["admin"]}'] },
      { "Tailscale-Funnel-Request": ["true"] },
    ];
    for (const headers of badHeaders) {
      expect((await provider.resolve(context({ immediatePeer: "127.0.0.1", headers }))).status).toBe(
        PeerIdentityStatus.INVALID,
      );
    }
  });
});

describe("Tailscale LocalAPI identity", () => {
  test("uses official scope, basic auth, and performs a fresh WhoIs every time", async () => {
    let requests = 0;
    const server = createServer((request, response) => {
      requests++;
      expect(request.headers.host).toBe("local-tailscaled.sock");
      expect(request.headers.authorization).toBe(`Basic ${Buffer.from(":secret").toString("base64")}`);
      const url = new URL(request.url!, "http://local/");
      expect(url.searchParams.get("addr")).toBe("100.64.0.10:4242");
      expect(url.searchParams.get("proto")).toBe("tcp");
      expect(url.searchParams.get("svc_name")).toBe("svc:analytics");
      response.setHeader("Content-Type", "application/json");
      response.end(whoIs());
    });
    servers.push(server);
    server.listen(0, "127.0.0.1");
    await new Promise((resolve) => server.once("listening", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing address");
    const provider = tailscaleLocalApiIdentityProvider({
      issuer: "tailnet:example",
      endpoint: `http://127.0.0.1:${address.port}`,
      password: "secret",
    });
    const resolution = context({
      immediatePeer: "100.64.0.10",
      sourceEndpoint: "100.64.0.10:4242",
      serviceName: "svc:analytics",
    });
    const first = await provider.resolve(resolution);
    const second = await provider.resolve(resolution);
    expect(requests).toBe(2);
    expect(second.status).toBe(PeerIdentityStatus.AVAILABLE);
    expect(first.identities[0].subjectKey).toBe("user:123");
    expect(first.identities[0].sourceAddress).toBe("100.64.0.10");
    expect(first.identities[0].attributes.capability_target).toEqual({ kind: "service", value: "svc:analytics" });
  });

  test("supports an explicit Unix socket and tagged stable node", async () => {
    if (process.platform === "win32") return;
    const directory = mkdtempSync(join(tmpdir(), "vgi-ts-"));
    const socket = join(directory, "tailscaled.sock");
    const server = createServer((request, response) => {
      expect(new URL(request.url!, "http://local/").searchParams.get("dst_ip")).toBe("2001:db8::8");
      response.setHeader("Content-Type", "application/json");
      response.end(whoIs(true));
    });
    servers.push(server);
    server.listen(socket);
    await new Promise((resolve) => server.once("listening", resolve));
    const provider = tailscaleLocalApiIdentityProvider({ issuer: "tailnet:x", unixSocket: socket });
    const resolved = await provider.resolve(
      context({
        assertedPeer: "100.64.0.10:1",
        destinationAddress: "[2001:db8::8]:443",
      }),
    );
    expect(resolved.status).toBe(PeerIdentityStatus.AVAILABLE);
    expect(resolved.identities[0].subjectKind).toBe(PeerSubjectKind.TAGGED_NODE);
    expect(resolved.identities[0].subjectKey).toBe("node:n123CNTRL");
    expect(resolved.identities[0].attributes.user_id).toBeUndefined();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    servers.splice(servers.indexOf(server), 1);
    rmSync(directory, { recursive: true });
  });

  const localApiCases: Array<{
    name: string;
    status?: number;
    body: string;
    headers?: Array<[string, string]>;
    expected: PeerIdentityStatus;
    delay?: number;
    maxResponseBytes?: number;
    maxResponseHeaderBytes?: number;
    oversizedHeader?: string;
  }> = [
    { name: "permission denied", status: 403, body: "{}", expected: PeerIdentityStatus.PERMISSION_DENIED },
    { name: "not found", status: 404, body: "{}", expected: PeerIdentityStatus.NO_MATCH },
    { name: "daemon unavailable", status: 503, body: "{}", expected: PeerIdentityStatus.UNAVAILABLE },
    {
      name: "duplicate key",
      body: '{"Node":{},"Node":{},"UserProfile":{"ID":1}}',
      expected: PeerIdentityStatus.INVALID,
    },
    {
      name: "invalid CapMap",
      body: '{"Node":{"Tags":[]},"UserProfile":{"ID":1},"CapMap":[]}',
      expected: PeerIdentityStatus.INVALID,
    },
    {
      name: "invalid JSON number",
      body: '{"Node":{"Tags":[]},"UserProfile":{"ID":1},"CapMap":{"x":[NaN]}}',
      expected: PeerIdentityStatus.INVALID,
    },
    {
      name: "duplicate Content-Type",
      body: whoIs(),
      headers: [
        ["Content-Type", "application/json"],
        ["Content-Type", "application/json"],
      ],
      expected: PeerIdentityStatus.INVALID,
    },
    {
      name: "body bound",
      body: "x".repeat(128),
      expected: PeerIdentityStatus.INVALID,
      maxResponseBytes: 64,
    },
    {
      name: "header bound",
      body: whoIs(),
      expected: PeerIdentityStatus.INVALID,
      maxResponseHeaderBytes: 512,
      oversizedHeader: "x".repeat(2_048),
    },
    { name: "deadline", body: whoIs(), expected: PeerIdentityStatus.UNAVAILABLE, delay: 100 },
  ];

  for (const fixture of localApiCases) {
    test(`maps LocalAPI ${fixture.name}`, async () => {
      const server = createServer((_request, response) => {
        const send = () => {
          if (fixture.headers) for (const [name, value] of fixture.headers) response.appendHeader(name, value);
          else response.setHeader("Content-Type", "application/json");
          if (fixture.oversizedHeader) response.setHeader("X-Oversized", fixture.oversizedHeader);
          response.statusCode = fixture.status ?? 200;
          response.end(fixture.body);
        };
        if (fixture.delay) setTimeout(send, fixture.delay);
        else send();
      });
      servers.push(server);
      server.listen(0, "127.0.0.1");
      await new Promise((resolve) => server.once("listening", resolve));
      const address = server.address();
      if (!address || typeof address === "string") throw new Error("missing address");
      const provider = tailscaleLocalApiIdentityProvider({
        issuer: "tailnet:x",
        endpoint: `http://127.0.0.1:${address.port}`,
        timeoutMs: fixture.delay ? 20 : 500,
        maxResponseBytes: fixture.maxResponseBytes,
        maxResponseHeaderBytes: fixture.maxResponseHeaderBytes,
      });
      expect((await provider.resolve(context({ immediatePeer: "100.64.0.1:1" }))).status).toBe(fixture.expected);
      server.close();
      server.closeAllConnections();
      servers.splice(servers.indexOf(server), 1);
    });
  }
});
