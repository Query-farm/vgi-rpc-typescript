// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Protocol version negotiation (PROTOCOL_VERSIONS_V1): parsing, the per-endpoint
// negotiation state, the server gate's refusal texts and the window overrides.
// The wire behaviour (headers on every response, reflection metadata, the
// client against a scripted server) is the shared conformance suite's.

import { describe, expect, test } from "bun:test";
import { gateProtocolVersion, protocolVersionsAdvertisement } from "../src/binding.js";
import { RpcError } from "../src/errors.js";
import { Protocol } from "../src/protocol.js";
import {
  ClientVersioning,
  EndpointNegotiation,
  formatVersions,
  highestCommon,
  NoCommonVersionError,
  parseVersions,
  VersionRange,
} from "../src/protocol-versions.js";
import { VgiRpcServer } from "../src/server.js";

describe("wire syntax", () => {
  test("parse is tolerant; format emits both ends", () => {
    const v = parseVersions(" vgi.v3=3.4-3.5 ,vgi.secret.v1=1.0, bad, x=3.x, vgi.v3=9.9-9.9");
    expect([...v.entries()].map(([k, r]) => `${k}=${r}`)).toEqual(["vgi.v3=3.4-3.5", "vgi.secret.v1=1.0-1.0"]);
    expect(parseVersions("")).toEqual(new Map());
    expect(formatVersions([["a.v1", VersionRange.parse("1.2.3-1.4.0")]])).toBe("a.v1=1.2-1.4");
    expect(() => VersionRange.parse("3.5-3.4")).toThrow("min above max");
  });

  test("highest common version", () => {
    expect(highestCommon(VersionRange.parse("3.4-3.6"), VersionRange.parse("3.3-3.5"))).toEqual([3, 5]);
    expect(highestCommon(VersionRange.parse("3.5-3.6"), VersionRange.parse("3.4-3.4"))).toBeNull();
  });
});

describe("EndpointNegotiation", () => {
  const client = new ClientVersioning("p.v1", "3.5.2", VersionRange.parse("3.4-3.5"));

  test("own newest, then the common version, then no overlap", () => {
    const n = new EndpointNegotiation("http://w");
    expect(n.wireVersion(client)).toBe("3.5.2");
    n.observe("p.v1=3.4-3.4");
    expect(n.wireVersion(client)).toBe("3.4.2");
    n.observe("p.v1=3.6-3.7");
    expect(() => n.wireVersion(client)).toThrow(NoCommonVersionError);
    try {
      n.wireVersion(client);
    } catch (e) {
      const err = e as NoCommonVersionError;
      expect(err.errorKind).toBe("protocol_version_unsupported");
      expect(err.errorMessage).toContain("3.4-3.5");
      expect(err.errorMessage).toContain("3.6-3.7");
      expect(err.errorMessage).toContain("too old");
    }
    n.observe(null); // absent header: keeps what it learned
    expect(n.hasAdvertisement).toBe(true);
  });

  test("a pre-spec refusal names the version to speak", () => {
    const n = new EndpointNegotiation();
    const legacy = new RpcError(
      "ProtocolVersionError",
      "VGI client/worker protocol_version mismatch for protocol 'p.v1'.\n  Client: 3.5.2\n  Server: 3.4.0",
      "",
      { errorKind: "protocol_version_mismatch" },
    );
    n.observeRefusal("p.v1", legacy);
    expect(n.wireVersion(client)).toBe("3.4.2");
  });

  test("a refusal's ErrorInfo windows are learned", () => {
    const n = new EndpointNegotiation();
    const refusal = new RpcError("ProtocolVersionError", "x", "", {
      errorKind: "protocol_version_unsupported",
      errorDetails: [{ "@type": "vgi_rpc.ErrorInfo", metadata: { "vgi_rpc.protocol_versions": "p.v1=3.4-3.4" } }],
    });
    n.observeRefusal("p.v1", refusal);
    expect(n.wireVersion(client)).toBe("3.4.2");
  });

  test("a range must end at the declared version", () => {
    expect(() => new ClientVersioning("p.v1", "3.5.0", VersionRange.parse("3.4-3.4"))).toThrow("must end at");
  });
});

describe("server gate and advertisement", () => {
  const proto = new Protocol("svc.v1", { protocolVersion: "3.4.0" });
  const binding = { name: "svc.v1", protocol: proto, versionExempt: false };

  test("refusal texts name both windows and the direction", () => {
    const refuse = (v: string | undefined, clientVersions?: string) => {
      try {
        gateProtocolVersion(binding, v, {
          advertisement: "svc.v1=3.4-3.4",
          ...(clientVersions ? { clientVersions } : {}),
        });
      } catch (e) {
        return e as Error & { errorKind: string; errorDetails: unknown[]; errorMetadata: Map<string, string> };
      }
      return null;
    };
    expect(refuse("3.4.9")).toBeNull();
    const tooNew = refuse("3.6.0", "svc.v1=3.5-3.6")!;
    expect(tooNew.errorKind).toBe("protocol_version_unsupported");
    expect(tooNew.message).toBe(
      "protocol_version_unsupported: client speaks svc.v1 3.6 (supports 3.5-3.6); this server supports svc.v1 " +
        "3.4-3.4. The server is too old (the client is too new for it). Upgrade the worker, or use a client " +
        "that supports svc.v1 3.4.",
    );
    expect(tooNew.errorMetadata.get("vgi_rpc.protocol_versions")).toBe("svc.v1=3.4-3.4");
    expect(JSON.stringify(tooNew.errorDetails)).toContain("vgi_rpc.ErrorInfo");
    expect(refuse("3.3.0")!.message).toContain("The client is too old (this server is too new for it)");
    expect(refuse(undefined)!.message).toContain("did not send vgi_rpc.protocol_version");
    expect(refuse("3.4")!.message).toContain("malformed protocol_version '3.4'");
  });

  test("a declared range widens downward; overrides apply and are validated", () => {
    expect(() => new Protocol("x.v1", { protocolVersion: "3.4.0", protocolVersionRange: "3.3-3.5" })).toThrow();
    const wide = new Protocol("w.v1", { protocolVersion: "3.4.0", protocolVersionRange: "3.3-3.4" });
    const server = new VgiRpcServer(wide, { protocols: [proto], grantKeys: null });
    expect(server.protocolVersions).toBe("w.v1=3.3-3.4, svc.v1=3.4-3.4");
    server.setProtocolVersionRanges({ "svc.v1": "3.3-3.3" });
    expect(server.protocolVersions).toBe("w.v1=3.3-3.4, svc.v1=3.3-3.3");
    const bad = new VgiRpcServer(wide, { protocolVersionRanges: { "vgi_rpc.Reflection.v1": "1.0" }, grantKeys: null });
    expect(() => bad.seal()).toThrow("not a hosted protocol that declares a protocolVersion");
    expect(
      protocolVersionsAdvertisement([{ name: "u.v1", protocol: new Protocol("u.v1"), versionExempt: false }]),
    ).toBe("");
  });
});
