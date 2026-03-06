// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import { createHash, X509Certificate } from "node:crypto";
import { AuthContext } from "../src/auth.js";
import { bearerAuthenticateStatic, chainAuthenticate } from "../src/http/bearer.js";
import {
  type CertValidateFn,
  mtlsAuthenticate,
  mtlsAuthenticateFingerprint,
  mtlsAuthenticateSubject,
  mtlsAuthenticateXfcc,
  parseXfcc,
  type XfccElement,
} from "../src/http/mtls.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

interface TestCert {
  cert: X509Certificate;
  pem: string;
}

function makeTestCert(cn: string, opts?: { daysValid?: number; notBeforeOffset?: number }): TestCert {
  const daysValid = opts?.daysValid ?? 365;
  const notBeforeOffset = opts?.notBeforeOffset; // hours

  const fmtDate = (d: Date) =>
    d
      .toISOString()
      .replace(/[-:T]/g, "")
      .replace(/\.\d+Z$/, "Z");

  if (notBeforeOffset !== undefined && (notBeforeOffset > 0 || (notBeforeOffset < 0 && daysValid === 0))) {
    // Need custom validity dates: generate key + CSR, then sign with openssl x509
    const tmpKey = `/tmp/mtls-test-${Date.now()}-${Math.random().toString(36).slice(2)}.key`;
    const tmpCsr = `${tmpKey}.csr`;

    const genResult = Bun.spawnSync(
      ["openssl", "req", "-newkey", "rsa:2048", "-nodes", "-keyout", tmpKey, "-out", tmpCsr, "-subj", `/CN=${cn}`],
      { stdout: "pipe", stderr: "pipe" },
    );
    if (genResult.exitCode !== 0) {
      throw new Error(`openssl req failed: ${genResult.stderr.toString()}`);
    }

    const now = new Date();
    const notBefore = new Date(now.getTime() + notBeforeOffset * 3600_000);
    const notAfter = new Date(notBefore.getTime() + daysValid * 86400_000);

    const signResult = Bun.spawnSync(
      [
        "openssl",
        "x509",
        "-req",
        "-in",
        tmpCsr,
        "-signkey",
        tmpKey,
        "-not_before",
        fmtDate(notBefore),
        "-not_after",
        fmtDate(notAfter),
        "-set_serial",
        String(Math.floor(Math.random() * 1e15)),
      ],
      { stdout: "pipe", stderr: "pipe" },
    );
    try {
      Bun.spawnSync(["rm", "-f", tmpKey, tmpCsr]);
    } catch {}
    if (signResult.exitCode !== 0) {
      throw new Error(`openssl x509 failed: ${signResult.stderr.toString()}`);
    }
    const pem = signResult.stdout.toString();
    return { cert: new X509Certificate(pem), pem };
  }

  const result = Bun.spawnSync(
    [
      "openssl",
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-keyout",
      "/dev/null",
      "-subj",
      `/CN=${cn}`,
      "-days",
      String(daysValid),
      "-set_serial",
      String(Math.floor(Math.random() * 1e15)),
    ],
    { stdout: "pipe", stderr: "pipe" },
  );
  if (result.exitCode !== 0) {
    throw new Error(`openssl failed: ${result.stderr.toString()}`);
  }
  const pem = result.stdout.toString();
  return { cert: new X509Certificate(pem), pem };
}

function certToHeader(pem: string): string {
  return encodeURIComponent(pem);
}

function makeRequest(opts?: {
  certHeader?: string;
  headerName?: string;
  xfcc?: string;
  authorization?: string;
}): Request {
  const headers: Record<string, string> = {};
  if (opts?.certHeader !== undefined) {
    headers[opts.headerName ?? "X-SSL-Client-Cert"] = opts.certHeader;
  }
  if (opts?.xfcc !== undefined) {
    headers["x-forwarded-client-cert"] = opts.xfcc;
  }
  if (opts?.authorization !== undefined) {
    headers.Authorization = opts.authorization;
  }
  return new Request("http://localhost/test", { headers });
}

// ---------------------------------------------------------------------------
// parseXfcc
// ---------------------------------------------------------------------------

describe("parseXfcc", () => {
  test("single element with hash and subject", () => {
    const result = parseXfcc('Hash=abc123;Subject="CN=client1"');
    expect(result).toHaveLength(1);
    expect(result[0].hash).toBe("abc123");
    expect(result[0].subject).toBe("CN=client1");
  });

  test("multiple elements", () => {
    const result = parseXfcc('Hash=a;Subject="CN=first",Hash=b;Subject="CN=second"');
    expect(result).toHaveLength(2);
    expect(result[0].subject).toBe("CN=first");
    expect(result[1].subject).toBe("CN=second");
  });

  test("quoted subject with commas", () => {
    const result = parseXfcc('Subject="CN=test,O=Acme\\, Inc."');
    expect(result).toHaveLength(1);
    expect(result[0].subject).toBe("CN=test,O=Acme, Inc.");
  });

  test("quoted subject with semicolons", () => {
    const result = parseXfcc('Subject="CN=test;extra=val";Hash=abc');
    expect(result).toHaveLength(1);
    expect(result[0].subject).toBe("CN=test;extra=val");
    expect(result[0].hash).toBe("abc");
  });

  test("URL-encoded cert field", () => {
    const encoded = encodeURIComponent("-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----\n");
    const result = parseXfcc(`Cert=${encoded}`);
    expect(result).toHaveLength(1);
    expect(result[0].cert).not.toBeNull();
    expect(result[0].cert!.startsWith("-----BEGIN CERTIFICATE-----")).toBe(true);
  });

  test("empty header", () => {
    expect(parseXfcc("")).toEqual([]);
  });

  test("multiple DNS fields", () => {
    const result = parseXfcc("DNS=a.example.com;DNS=b.example.com");
    expect(result).toHaveLength(1);
    expect(result[0].dns).toEqual(["a.example.com", "b.example.com"]);
  });

  test("URI field is URL-decoded", () => {
    const encoded = encodeURIComponent("spiffe://cluster.local/ns/default/sa/client");
    const result = parseXfcc(`URI=${encoded}`);
    expect(result).toHaveLength(1);
    expect(result[0].uri).toBe("spiffe://cluster.local/ns/default/sa/client");
  });

  test("By field is URL-decoded", () => {
    const encoded = encodeURIComponent("spiffe://cluster.local/ns/default/sa/server");
    const result = parseXfcc(`By=${encoded}`);
    expect(result).toHaveLength(1);
    expect(result[0].by).toBe("spiffe://cluster.local/ns/default/sa/server");
  });
});

// ---------------------------------------------------------------------------
// mtlsAuthenticateXfcc
// ---------------------------------------------------------------------------

describe("mtlsAuthenticateXfcc", () => {
  test("valid XFCC extracts principal from Subject CN", async () => {
    const authFn = mtlsAuthenticateXfcc();
    const req = makeRequest({ xfcc: 'Hash=abc;Subject="CN=client1,O=Acme"' });
    const auth = await authFn(req);
    expect(auth.principal).toBe("client1");
    expect(auth.domain).toBe("mtls");
    expect(auth.authenticated).toBe(true);
  });

  test("missing header throws", async () => {
    const authFn = mtlsAuthenticateXfcc();
    const req = makeRequest();
    expect(authFn(req)).rejects.toThrow("Missing");
  });

  test("custom validate callback", async () => {
    function validate(elem: XfccElement): AuthContext {
      if (elem.hash === "trusted") {
        return new AuthContext("xfcc", true, "validated", {});
      }
      throw new Error("untrusted");
    }
    const authFn = mtlsAuthenticateXfcc({ validate });
    const req = makeRequest({ xfcc: "Hash=trusted" });
    const auth = await authFn(req);
    expect(auth.principal).toBe("validated");
  });

  test("custom validate rejection", async () => {
    function validate(_elem: XfccElement): AuthContext {
      throw new Error("nope");
    }
    const authFn = mtlsAuthenticateXfcc({ validate });
    const req = makeRequest({ xfcc: "Hash=whatever" });
    expect(authFn(req)).rejects.toThrow("nope");
  });

  test("default CN extraction from Subject", async () => {
    const authFn = mtlsAuthenticateXfcc();
    const req = makeRequest({ xfcc: 'Subject="CN=my-service,OU=Engineering"' });
    const auth = await authFn(req);
    expect(auth.principal).toBe("my-service");
  });

  test("select first element", async () => {
    const authFn = mtlsAuthenticateXfcc({ selectElement: "first" });
    const req = makeRequest({ xfcc: 'Subject="CN=original",Subject="CN=proxy"' });
    const auth = await authFn(req);
    expect(auth.principal).toBe("original");
  });

  test("select last element", async () => {
    const authFn = mtlsAuthenticateXfcc({ selectElement: "last" });
    const req = makeRequest({ xfcc: 'Subject="CN=original",Subject="CN=proxy"' });
    const auth = await authFn(req);
    expect(auth.principal).toBe("proxy");
  });

  test("claims populated from XFCC fields", async () => {
    const encodedUri = encodeURIComponent("spiffe://cluster/ns/default");
    const authFn = mtlsAuthenticateXfcc();
    const req = makeRequest({ xfcc: `Hash=deadbeef;Subject="CN=svc";URI=${encodedUri}` });
    const auth = await authFn(req);
    expect(auth.claims.hash).toBe("deadbeef");
    expect(auth.claims.subject).toBe("CN=svc");
    expect(auth.claims.uri).toBe("spiffe://cluster/ns/default");
  });
});

// ---------------------------------------------------------------------------
// mtlsAuthenticate
// ---------------------------------------------------------------------------

describe("mtlsAuthenticate", () => {
  test("valid cert calls validate", async () => {
    const { pem } = makeTestCert("alice");
    const validate: CertValidateFn = (c) => {
      // Node's subject is \n-separated
      const cn = c.subject
        .split("\n")
        .find((s) => s.trim().startsWith("CN="))
        ?.trim()
        .slice(3);
      return new AuthContext("mtls", true, cn ?? "", {});
    };
    const authFn = mtlsAuthenticate({ validate });
    const req = makeRequest({ certHeader: certToHeader(pem) });
    const auth = await authFn(req);
    expect(auth.authenticated).toBe(true);
    expect(auth.principal).toBe("alice");
  });

  test("invalid PEM throws", async () => {
    const validate: CertValidateFn = () => new AuthContext("mtls", true, "", {});
    const authFn = mtlsAuthenticate({ validate });
    const req = makeRequest({ certHeader: encodeURIComponent("not a certificate") });
    expect(authFn(req)).rejects.toThrow("not a PEM certificate");
  });

  test("missing header throws", async () => {
    const validate: CertValidateFn = () => new AuthContext("mtls", true, "", {});
    const authFn = mtlsAuthenticate({ validate });
    const req = makeRequest();
    expect(authFn(req)).rejects.toThrow("Missing");
  });

  test("custom header name", async () => {
    const { pem } = makeTestCert("bob");
    const validate: CertValidateFn = () => new AuthContext("mtls", true, "bob", {});
    const authFn = mtlsAuthenticate({ validate, header: "X-Amzn-Mtls-Clientcert" });
    const req = makeRequest({
      certHeader: certToHeader(pem),
      headerName: "X-Amzn-Mtls-Clientcert",
    });
    const auth = await authFn(req);
    expect(auth.principal).toBe("bob");
  });

  test("validate rejection", async () => {
    const { pem } = makeTestCert("evil");
    const validate: CertValidateFn = () => {
      throw new Error("certificate revoked");
    };
    const authFn = mtlsAuthenticate({ validate });
    const req = makeRequest({ certHeader: certToHeader(pem) });
    expect(authFn(req)).rejects.toThrow("certificate revoked");
  });

  test("expired cert rejected with checkExpiry", async () => {
    const { pem } = makeTestCert("expired", { daysValid: 0, notBeforeOffset: -48 });
    const validate: CertValidateFn = () => new AuthContext("mtls", true, "x", {});
    const authFn = mtlsAuthenticate({ validate, checkExpiry: true });
    const req = makeRequest({ certHeader: certToHeader(pem) });
    expect(authFn(req)).rejects.toThrow("expired");
  });

  test("not-yet-valid cert rejected with checkExpiry", async () => {
    const { pem } = makeTestCert("future", { daysValid: 365, notBeforeOffset: 720 });
    const validate: CertValidateFn = () => new AuthContext("mtls", true, "x", {});
    const authFn = mtlsAuthenticate({ validate, checkExpiry: true });
    const req = makeRequest({ certHeader: certToHeader(pem) });
    expect(authFn(req)).rejects.toThrow("not yet valid");
  });
});

// ---------------------------------------------------------------------------
// mtlsAuthenticateFingerprint
// ---------------------------------------------------------------------------

describe("mtlsAuthenticateFingerprint", () => {
  test("known fingerprint returns mapped context", async () => {
    const { cert, pem } = makeTestCert("known");
    const fp = createHash("sha256").update(cert.raw).digest("hex");
    const ctx = new AuthContext("mtls", true, "known-client", {});
    const authFn = mtlsAuthenticateFingerprint({ fingerprints: { [fp]: ctx } });
    const req = makeRequest({ certHeader: certToHeader(pem) });
    const auth = await authFn(req);
    expect(auth.principal).toBe("known-client");
  });

  test("unknown fingerprint throws", async () => {
    const { pem } = makeTestCert("unknown");
    const ctx = new AuthContext("mtls", true, "x", {});
    const authFn = mtlsAuthenticateFingerprint({ fingerprints: { deadbeef: ctx } });
    const req = makeRequest({ certHeader: certToHeader(pem) });
    expect(authFn(req)).rejects.toThrow("Unknown certificate fingerprint");
  });

  test("SHA-1 algorithm", async () => {
    const { cert, pem } = makeTestCert("sha1-client");
    const fp = createHash("sha1").update(cert.raw).digest("hex");
    const ctx = new AuthContext("mtls", true, "sha1-ok", {});
    const authFn = mtlsAuthenticateFingerprint({
      fingerprints: { [fp]: ctx },
      algorithm: "sha1",
    });
    const req = makeRequest({ certHeader: certToHeader(pem) });
    const auth = await authFn(req);
    expect(auth.principal).toBe("sha1-ok");
  });

  test("fingerprint is lowercase hex without colons", async () => {
    const { cert, pem } = makeTestCert("norm");
    const fp = createHash("sha256").update(cert.raw).digest("hex");
    expect(fp).toBe(fp.toLowerCase());
    expect(fp).not.toContain(":");
    const ctx = new AuthContext("mtls", true, "norm-ok", {});
    const authFn = mtlsAuthenticateFingerprint({ fingerprints: { [fp]: ctx } });
    const req = makeRequest({ certHeader: certToHeader(pem) });
    const auth = await authFn(req);
    expect(auth.principal).toBe("norm-ok");
  });

  test("unsupported algorithm throws at construction", () => {
    const ctx = new AuthContext("mtls", true, "x", {});
    expect(() => mtlsAuthenticateFingerprint({ fingerprints: { abc: ctx }, algorithm: "md5" })).toThrow(
      "Unsupported hash algorithm",
    );
  });
});

// ---------------------------------------------------------------------------
// mtlsAuthenticateSubject
// ---------------------------------------------------------------------------

describe("mtlsAuthenticateSubject", () => {
  test("CN extraction as principal", async () => {
    const { pem } = makeTestCert("my-service");
    const authFn = mtlsAuthenticateSubject();
    const req = makeRequest({ certHeader: certToHeader(pem) });
    const auth = await authFn(req);
    expect(auth.principal).toBe("my-service");
    expect(auth.domain).toBe("mtls");
    expect(auth.authenticated).toBe(true);
  });

  test("allowed subjects pass", async () => {
    const { pem } = makeTestCert("allowed");
    const authFn = mtlsAuthenticateSubject({
      allowedSubjects: new Set(["allowed", "also-ok"]),
    });
    const req = makeRequest({ certHeader: certToHeader(pem) });
    const auth = await authFn(req);
    expect(auth.principal).toBe("allowed");
  });

  test("allowed subjects reject", async () => {
    const { pem } = makeTestCert("forbidden");
    const authFn = mtlsAuthenticateSubject({
      allowedSubjects: new Set(["allowed"]),
    });
    const req = makeRequest({ certHeader: certToHeader(pem) });
    expect(authFn(req)).rejects.toThrow("not in allowed subjects");
  });

  test("claims contain serial and validity", async () => {
    const { pem } = makeTestCert("claims-test");
    const authFn = mtlsAuthenticateSubject();
    const req = makeRequest({ certHeader: certToHeader(pem) });
    const auth = await authFn(req);
    expect(auth.claims).toHaveProperty("subject_dn");
    expect(auth.claims).toHaveProperty("serial");
    expect(auth.claims).toHaveProperty("not_valid_after");
    // Serial should be hex string
    expect(() => BigInt(`0x${auth.claims.serial}`)).not.toThrow();
  });

  test("expired cert rejected with checkExpiry", async () => {
    const { pem } = makeTestCert("expired-subj", { daysValid: 0, notBeforeOffset: -48 });
    const authFn = mtlsAuthenticateSubject({ checkExpiry: true });
    const req = makeRequest({ certHeader: certToHeader(pem) });
    expect(authFn(req)).rejects.toThrow("expired");
  });
});

// ---------------------------------------------------------------------------
// Chain integration
// ---------------------------------------------------------------------------

describe("chain integration", () => {
  test("mTLS + bearer chain: mTLS succeeds", async () => {
    const { pem } = makeTestCert("chain-client");
    const validate: CertValidateFn = () => new AuthContext("mtls", true, "chain-client", {});
    const mtlsAuth = mtlsAuthenticate({ validate });
    const bearerCtx = new AuthContext("bearer", true, "bearer-user", {});
    const bearerAuth = bearerAuthenticateStatic({ tokens: { "token-x": bearerCtx } });
    const chain = chainAuthenticate(mtlsAuth, bearerAuth);

    const req = makeRequest({ certHeader: certToHeader(pem) });
    const auth = await chain(req);
    expect(auth.domain).toBe("mtls");
    expect(auth.principal).toBe("chain-client");
  });

  test("mTLS fallback to bearer", async () => {
    const validate: CertValidateFn = () => new AuthContext("mtls", true, "x", {});
    const mtlsAuth = mtlsAuthenticate({ validate });
    const bearerCtx = new AuthContext("bearer", true, "bearer-user", {});
    const bearerAuth = bearerAuthenticateStatic({ tokens: { "my-token": bearerCtx } });
    const chain = chainAuthenticate(mtlsAuth, bearerAuth);

    const req = makeRequest({ authorization: "Bearer my-token" });
    const auth = await chain(req);
    expect(auth.domain).toBe("bearer");
    expect(auth.principal).toBe("bearer-user");
  });
});
