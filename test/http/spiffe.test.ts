// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import {
  awsAlbSpiffeProvider,
  azureApplicationGatewaySpiffeProvider,
  envoyXfccSpiffeProvider,
  gcpLoadBalancerSpiffeProvider,
  headersFromNodeRawHeaders,
  nginxSpiffeProvider,
  spiffeX509HeaderProvider,
  validateSpiffeId,
} from "../../src/http/spiffe.js";
import { IdentityAssurance, PeerIdentityStatus, PeerResolutionContext } from "../../src/identity.js";

const PEM = `-----BEGIN CERTIFICATE-----
MIIDITCCAgmgAwIBAgIUVPAiaXmJA/RCg0Cwqthu6MfE+/IwDQYJKoZIhvcNAQEL
BQAwDzENMAsGA1UEAwwEdGVzdDAeFw0yNjA4MjkyMzQ5MjNaFw0zNjA4MjcyMzQ5
MjNaMA8xDTALBgNVBAMMBHRlc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK
AoIBAQDmoIFKdkMezf9ByS/zfDplLHyg1JzR2LetBSjYrPB6SZVe8Su/LZoYMSFY
ZcF7XpBCWoCQJJBTX6z3oOKOK0wgRrC5kjVDqZd5jxK2W0qJhT8T3Itg+uLcxGv1
YGdPRMtrpWtvsTMOns4zZQfqqyP0jmqS9tjele8ohB4Tiqvb6H0leA4FTprfHfAI
49xnNqzSQgmwADrFMkf3onPbsn9+jfDie5VPKFyklMSrr1g5Ir71XZQDDxbCdYxO
XYz4GC4GpNOw5ScesgEDqp0EkVGOMTrc21TEoGJ17l9ge7/SxHK3FmwMBm7XIisI
hnJSicYhypYL3yENJQxjVFc6Z+XNAgMBAAGjdTBzMDQGA1UdEQQtMCuGKXNwaWZm
ZTovL2V4YW1wbGUub3JnL25zL2RlZmF1bHQvc2Evd29ya2VyMAwGA1UdEwEB/wQC
MAAwDgYDVR0PAQH/BAQDAgeAMB0GA1UdJQQWMBQGCCsGAQUFBwMCBggrBgEFBQcD
ATANBgkqhkiG9w0BAQsFAAOCAQEAx96Dkfg2+ZUBxE0pSgvZhJ0BUQgxyywWvgPD
P6io0/rdOi615s4b7zby2IXW4CvhGr0Z/ya26x1iKSIY9nOQFa0FGhPSpXhJaZLA
rRPXdNpZuzwa4mmPD/89I/Ue4W15hVfVUwc0FhSjKh2IoF/PjloyEGOCEp6py675
pdAu46wxg6wXMNoglj+xjPa8LEu7yhEZqD/8rreWEnGZ/Cx6GdhRvin4aupjZFtZ
Webp2BE6M6O6uy0Z7cpHUash91LNusTULpHUInEjNdOYV39gPyFc5T5JlLrBlITV
ZX/TWvA+CdFMurlN99Ilbh5LmuKK45p4krzB6UVHHj4WpUglBA==
-----END CERTIFICATE-----
`;
const ENCODED_PEM = encodeURIComponent(PEM);

function context(peer: string, headers: Readonly<Record<string, readonly string[]>>): PeerResolutionContext {
  return new PeerResolutionContext("http", {
    immediatePeer: peer,
    assertedPeer: "198.51.100.7:1234",
    headers,
  });
}

describe("trusted HTTP proxy SPIFFE providers", () => {
  test("Node rawHeaders preserve duplicates and case ambiguity", () => {
    const repeated = headersFromNodeRawHeaders(["X-Peer", "alice", "X-Peer", "mallory"]);
    expect(() => new PeerResolutionContext("http", { headers: repeated }).header("x-peer")).toThrow();
    const varied = headersFromNodeRawHeaders(["X-Peer", "alice", "x-peer", "mallory"]);
    expect(() => new PeerResolutionContext("http", { headers: varied })).toThrow();
    expect(() => headersFromNodeRawHeaders(["X-Peer"])).toThrow();
  });

  test("canonical SPIFFE IDs reject aliases", () => {
    const domains = new Set(["example.org"]);
    expect(validateSpiffeId("spiffe://example.org/ns/default/sa/worker", domains)).toBe("example.org");
    for (const value of [
      "spiffe://Example.org/workload",
      "spiffe://example.org/a%2Fb",
      "spiffe://example.org/a//b",
      "spiffe://example.org/a/../b",
      "spiffe://example.org/a/",
      "spiffe://example.org/a:b",
      "spiffe://other.org/workload",
    ]) {
      expect(() => validateSpiffeId(value, domains)).toThrow();
    }
  });

  test("trusted proxies are normalized exact IP literals", async () => {
    expect(() =>
      nginxSpiffeProvider({ trustDomains: ["example.org"], trustedProxyAddresses: ["proxy.example"] }),
    ).toThrow();
    expect(() =>
      nginxSpiffeProvider({ trustDomains: ["example.org"], trustedProxyAddresses: ["10.0.0.0/8"] }),
    ).toThrow();
    expect(() =>
      nginxSpiffeProvider({
        trustDomains: ["example.org"],
        trustedProxyAddresses: ["127.0.0.1", "::ffff:127.0.0.1"],
      }),
    ).toThrow();

    const provider = nginxSpiffeProvider({
      trustDomains: ["example.org"],
      trustedProxyAddresses: ["2001:0db8:0:0:0:0:0:1"],
    });
    const resolved = await provider.resolve(
      context("2001:db8::1", {
        "X-SSL-Client-Cert": [ENCODED_PEM],
        "X-SSL-Client-Verify": ["SUCCESS"],
      }),
    );
    expect(resolved.status).toBe(PeerIdentityStatus.AVAILABLE);
  });

  test("generic certificate provider requires exact proxy and positive verification", async () => {
    expect(() =>
      spiffeX509HeaderProvider({
        trustDomains: ["example.org"],
        trustedProxyAddresses: ["127.0.0.1"],
        chainVerifiedHeader: "",
      }),
    ).toThrow();
    const provider = spiffeX509HeaderProvider({
      trustDomains: ["example.org"],
      trustedProxyAddresses: ["127.0.0.1"],
      chainVerifiedHeader: "X-Client-Cert-Verified",
    });
    expect((await provider.resolve(context("127.0.0.2", {}))).status).toBe(PeerIdentityStatus.UNTRUSTED_PROXY);
    expect(
      (
        await provider.resolve(
          context("127.0.0.1", {
            "X-SSL-Client-Cert": [ENCODED_PEM],
          }),
        )
      ).status,
    ).toBe(PeerIdentityStatus.INVALID);
    const result = await provider.resolve(
      context("127.0.0.1", {
        "X-SSL-Client-Cert": [ENCODED_PEM],
        "X-Client-Cert-Verified": ["true"],
      }),
    );
    expect(result.status).toBe(PeerIdentityStatus.AVAILABLE);
    expect(result.identities[0].subjectKey).toBe("spiffe://example.org/ns/default/sa/worker");
    expect(result.identities[0].assurance).toBe(IdentityAssurance.CONFIGURED_PROXY);
    expect(result.identities[0].sourceAddress).toBe("198.51.100.7:1234");
  });

  test("certificate wrappers preserve their cloud/proxy evidence source", async () => {
    const cases = [
      {
        provider: nginxSpiffeProvider({ trustDomains: ["example.org"], trustedProxyAddresses: ["127.0.0.1"] }),
        headers: { "X-SSL-Client-Cert": [ENCODED_PEM], "X-SSL-Client-Verify": ["SUCCESS"] },
        source: "nginx_mtls",
      },
      {
        provider: azureApplicationGatewaySpiffeProvider({
          trustDomains: ["example.org"],
          trustedProxyAddresses: ["127.0.0.1"],
        }),
        headers: { "X-Client-Certificate": [ENCODED_PEM], "X-Client-Certificate-Verification": ["SUCCESS"] },
        source: "azure_application_gateway_mtls_strict",
      },
      {
        provider: awsAlbSpiffeProvider({ trustDomains: ["example.org"], trustedProxyAddresses: ["127.0.0.1"] }),
        headers: { "X-Amzn-Mtls-Clientcert-Leaf": [ENCODED_PEM] },
        source: "aws_alb_mtls_verify",
      },
    ];
    for (const item of cases) {
      const result = await item.provider.resolve(context("127.0.0.1", item.headers));
      expect(result.status).toBe(PeerIdentityStatus.AVAILABLE);
      expect(result.identities[0].evidenceSource).toBe(item.source);
    }
  });

  test("certificate headers reject multiplicity, combination, and bounds", async () => {
    const provider = nginxSpiffeProvider({
      trustDomains: ["example.org"],
      trustedProxyAddresses: ["127.0.0.1"],
      maxHeaderBytes: ENCODED_PEM.length + 10,
    });
    expect(
      (
        await provider.resolve(
          context("127.0.0.1", {
            "X-SSL-Client-Cert": [ENCODED_PEM, ENCODED_PEM],
            "X-SSL-Client-Verify": ["SUCCESS"],
          }),
        )
      ).status,
    ).toBe(PeerIdentityStatus.INVALID);
    expect(
      (
        await provider.resolve(
          context("127.0.0.1", {
            "X-SSL-Client-Cert": [`${ENCODED_PEM},duplicate`],
            "X-SSL-Client-Verify": ["SUCCESS"],
          }),
        )
      ).status,
    ).toBe(PeerIdentityStatus.INVALID);
  });

  test("GCP requires every validation signal and rejects duplicate raw headers", async () => {
    const provider = gcpLoadBalancerSpiffeProvider({
      trustDomains: ["example.org"],
      trustedProxyAddresses: ["127.0.0.1"],
    });
    const headers = {
      "X-Client-Cert-Present": ["true"],
      "X-Client-Cert-Chain-Verified": ["true"],
      "X-Client-Cert-Spiffe-Id": ["spiffe://example.org/ns/default/sa/client"],
    };
    const result = await provider.resolve(context("127.0.0.1", headers));
    expect(result.status).toBe(PeerIdentityStatus.AVAILABLE);
    expect(result.identities[0].evidenceSource).toBe("gcp_load_balancer_mtls");
    expect((await provider.resolve(context("127.0.0.1", { "X-Client-Cert-Present": ["false"] }))).status).toBe(
      PeerIdentityStatus.NO_MATCH,
    );
    expect(
      (await provider.resolve(context("127.0.0.1", { ...headers, "X-Client-Cert-Chain-Verified": ["true", "false"] })))
        .status,
    ).toBe(PeerIdentityStatus.INVALID);
  });

  test("Envoy accepts one SANITIZE_SET element and rejects chains and ambiguity", async () => {
    const provider = envoyXfccSpiffeProvider({
      trustDomains: ["example.org"],
      trustedProxyAddresses: ["127.0.0.1"],
    });
    const valid = `By=spiffe://mesh.example/proxy;Hash=${"a".repeat(64)};URI="spiffe://example.org/ns/default/sa/client"`;
    const result = await provider.resolve(context("127.0.0.1", { "X-Forwarded-Client-Cert": [valid] }));
    expect(result.status).toBe(PeerIdentityStatus.AVAILABLE);
    expect(result.identities[0].attributes.certificate_sha256).toBe("a".repeat(64));
    for (const value of [
      `${valid},By=spiffe://mesh.example/second;Hash=${"b".repeat(64)};URI=spiffe://example.org/other`,
      "URI=spiffe://example.org/client",
      `Hash=abc;URI=spiffe://example.org/client`,
      `Hash=${"a".repeat(64)};URI=spiffe://example.org/one;URI=spiffe://example.org/two`,
      `Hash=${"a".repeat(64)};Hash=${"b".repeat(64)};URI=spiffe://example.org/client`,
      `Unknown=value;Hash=${"a".repeat(64)};URI=spiffe://example.org/client`,
      `Hash=${"a".repeat(64)};URI=spiffe://example.org/client%ZZ`,
    ]) {
      expect((await provider.resolve(context("127.0.0.1", { "X-Forwarded-Client-Cert": [value] }))).status).toBe(
        PeerIdentityStatus.INVALID,
      );
    }
  });
});
