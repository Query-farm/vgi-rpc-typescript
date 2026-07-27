// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import { createHttpHandler } from "../src/http/handler.js";
import {
  canonicalString,
  deriveProofSecret,
  mintProof,
  NonceCache,
  PROOF_HEADER,
  PROOF_REQUIRED_HEADER,
  type ProofConfig,
  ProofError,
  parseProofSecrets,
  requireProxyProof,
  verifyProof,
} from "../src/http/proof.js";
import { Protocol } from "../src/protocol.js";
import { str } from "../src/schema.js";
import { hmacSha256 } from "../src/util/web-crypto.js";

// Golden vectors from the Python reference implementation. Verifying these is
// the only thing that proves TypeScript frames the canonical string
// identically — a port can round-trip perfectly against itself while framing
// the MAC input differently from every other language.
const GOLDEN_TOKEN =
  "v1.conformance-proxy.1700000000.Q0ZPUk1BTkNFTk9OQ0UxMQ.XQ2QBf35oajjaP7HIas3OfyEvNhyXTTptbrxWFxWk3I";
const GOLDEN_ORIGIN = "conformance-origin";
const GOLDEN_KID = "conformance-proxy";
const GOLDEN_TIME = 1700000000;
const GOLDEN_NONCE = "Q0ZPUk1BTkNFTk9OQ0UxMQ";
const GOLDEN_DERIVED = "af85db125b8270bc0a0971736340dc8476ba70e1fad472b72b68ba739bd1cd94";

const secret = () => new Uint8Array(32).fill(0x11);

function config(overrides: Partial<ProofConfig> = {}): ProofConfig {
  return {
    mode: "require",
    originId: GOLDEN_ORIGIN,
    secrets: { [GOLDEN_KID]: { secret: secret(), label: GOLDEN_KID } },
    skewSeconds: 30,
    ...overrides,
  };
}

const toHex = (b: Uint8Array) => [...b].map((x) => x.toString(16).padStart(2, "0")).join("");

const b64url = (b: Uint8Array) => {
  let s = "";
  for (const byte of b) s += String.fromCharCode(byte);
  return btoa(s).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
};

describe("cross-language agreement", () => {
  test("verifies a token minted by the Python reference implementation", async () => {
    const claims = await verifyProof(GOLDEN_TOKEN, config(), null, GOLDEN_TIME);
    expect(claims.verified).toBe("true");
    expect(claims.proxy).toBe(GOLDEN_KID);
  });

  test("minting produces byte-identical output to Python", async () => {
    const token = await mintProof(secret(), GOLDEN_KID, GOLDEN_ORIGIN, GOLDEN_TIME, GOLDEN_NONCE);
    expect(token).toBe(GOLDEN_TOKEN);
  });

  test("secret derivation matches Python", async () => {
    const base = new Uint8Array(32).map((_, i) => i);
    expect(toHex(await deriveProofSecret(base, "prod-use1", "worker-a"))).toBe(GOLDEN_DERIVED);
  });

  test("derivation boundaries cannot be shifted between the two ids", async () => {
    const base = new Uint8Array(32);
    const a = await deriveProofSecret(base, "ab", "c.d");
    const b = await deriveProofSecret(base, "a", "b.c.d");
    expect(toHex(a)).not.toBe(toHex(b));
  });
});

describe("malformed input", () => {
  const cases: [string, string][] = [
    ["empty", ""],
    ["not dotted", "garbage"],
    ["four fields", "v1.a.b.c"],
    ["six fields", "v1.a.b.c.d.e"],
    ["wrong version", `v2.${GOLDEN_KID}.1.${GOLDEN_NONCE}.${"A".repeat(43)}`],
    ["kid charset", `v1.bad!kid.1.${GOLDEN_NONCE}.${"A".repeat(43)}`],
    ["ts charset", `v1.${GOLDEN_KID}.xyz.${GOLDEN_NONCE}.${"A".repeat(43)}`],
    ["nonce charset", `v1.${GOLDEN_KID}.1.short.${"A".repeat(43)}`],
    ["mac charset", `v1.${GOLDEN_KID}.1.${GOLDEN_NONCE}.!!!`],
    ["oversized", `v1.${"x".repeat(600)}`],
  ];

  for (const [name, token] of cases) {
    test(`${name} is rejected as malformed`, async () => {
      // Reason codes are part of the wire contract, so every port must agree
      // on which failures are "malformed" versus a MAC mismatch.
      await expect(verifyProof(token, config(), null, GOLDEN_TIME)).rejects.toMatchObject({
        reason: "malformed",
      });
    });
  }
});

describe("verification", () => {
  test("an unconfigured key id is rejected", async () => {
    const cfg = config({ secrets: { other: { secret: secret(), label: "other" } } });
    await expect(verifyProof(GOLDEN_TOKEN, cfg, null, GOLDEN_TIME)).rejects.toMatchObject({
      reason: "unknown_kid",
    });
  });

  test("a proof minted for another worker does not verify here", async () => {
    // Audience binding: originId is folded into the MAC but never transmitted,
    // so it cannot be adjusted by the caller.
    const cfg = config({ originId: "some-other-worker" });
    await expect(verifyProof(GOLDEN_TOKEN, cfg, null, GOLDEN_TIME)).rejects.toMatchObject({
      reason: "bad_mac",
    });
  });

  test("a MAC over an incorrectly framed canonical string does not verify", async () => {
    // Catches a port whose crypto is right but whose framing is not — the
    // failure a self-round-trip inside one implementation cannot reveal.
    const concatenated = new TextEncoder().encode(
      `vgi.proxy.proof.v1${GOLDEN_KID}1700000000${GOLDEN_NONCE}${GOLDEN_ORIGIN}`,
    );
    const mac = await hmacSha256(secret(), concatenated);
    const token = `v1.${GOLDEN_KID}.1700000000.${GOLDEN_NONCE}.${b64url(mac)}`;
    await expect(verifyProof(token, config(), null, GOLDEN_TIME)).rejects.toMatchObject({
      reason: "bad_mac",
    });
  });

  test("the timestamp window is enforced at both ends", async () => {
    // The future case catches a verifier checking only an upper bound, which
    // would let a future-dated proof pass indefinitely.
    await expect(verifyProof(GOLDEN_TOKEN, config(), null, GOLDEN_TIME + 91)).rejects.toMatchObject({
      reason: "expired",
    });
    await expect(verifyProof(GOLDEN_TOKEN, config(), null, GOLDEN_TIME - 91)).rejects.toMatchObject({
      reason: "not_yet_valid",
    });
    const ok = await verifyProof(GOLDEN_TOKEN, config(), null, GOLDEN_TIME + 20);
    expect(ok.verified).toBe("true");
  });

  test("canonical string is NUL-separated", () => {
    const bytes = canonicalString("k", "1", "n", "o");
    expect([...bytes].filter((b) => b === 0).length).toBe(4);
  });
});

describe("replay cache", () => {
  test("a replayed nonce is rejected", async () => {
    const cache = new NonceCache(30, 100);
    const cfg = config();
    expect((await verifyProof(GOLDEN_TOKEN, cfg, cache, GOLDEN_TIME)).verified).toBe("true");
    await expect(verifyProof(GOLDEN_TOKEN, cfg, cache, GOLDEN_TIME)).rejects.toMatchObject({
      reason: "replayed",
    });
  });

  test("capacity is a hard cap", () => {
    // A TTL bounds how long an entry lives, never how many arrive inside the
    // window, so a TTL-only cache is a remote memory-exhaustion vector.
    const cache = new NonceCache(3600, 10);
    for (let i = 0; i < 500; i++) cache.checkAndAdd(`nonce-${i}`, GOLDEN_TIME);
    expect(cache.size).toBeLessThanOrEqual(10);
  });

  test("entries expire past the TTL", () => {
    const cache = new NonceCache(30, 100);
    expect(cache.checkAndAdd("n1", 1000)).toBe(true);
    expect(cache.checkAndAdd("n1", 1000)).toBe(false);
    expect(cache.checkAndAdd("n1", 1031)).toBe(true);
  });
});

describe("gate", () => {
  const req = (token?: string) =>
    new Request("http://localhost/vgi/echo", token ? { headers: { [PROOF_HEADER]: token } } : undefined);

  test("require accepts a valid proof and attributes it", async () => {
    const gate = requireProxyProof(config({ now: () => GOLDEN_TIME }));
    const ctx = await gate(req(GOLDEN_TOKEN));
    expect(ctx.claims.vgi_proxy_proof.proxy).toBe(GOLDEN_KID);
  });

  test("require rejects a missing proof", async () => {
    const gate = requireProxyProof(config({ now: () => GOLDEN_TIME }));
    await expect(gate(req())).rejects.toMatchObject({ reason: "no_proof" });
  });

  test("the rejection message does not echo caller input", async () => {
    const gate = requireProxyProof(config({ now: () => GOLDEN_TIME }));
    const forged = `v1.attacker-controlled.1.${GOLDEN_NONCE}.${"A".repeat(43)}`;
    await expect(gate(req(forged))).rejects.toThrow(/^proxy proof required$/);
  });

  test("multiple proof headers are rejected", async () => {
    const gate = requireProxyProof(config({ now: () => GOLDEN_TIME }));
    await expect(gate(req(`${GOLDEN_TOKEN}, ${GOLDEN_TOKEN}`))).rejects.toMatchObject({
      reason: "malformed",
    });
  });

  test("allow mode records the outcome without denying", async () => {
    const gate = requireProxyProof(config({ mode: "allow", now: () => GOLDEN_TIME }));
    const ctx = await gate(req());
    expect(ctx.claims.vgi_proxy_proof.verified).toBe("false");
    expect(ctx.claims.vgi_proxy_proof.reason).toBe("no_proof");
  });

  test("off mode installs no gate rather than a passing one", () => {
    expect(() => requireProxyProof(config({ mode: "off" }))).toThrow(/install no gate/);
  });

  test("the error is a plain Error so chainAuthenticate does not rethrow it", async () => {
    // chainAuthenticate treats an Error subclass as a non-credential failure
    // and rethrows, which would bypass composition.
    const gate = requireProxyProof(config({ now: () => GOLDEN_TIME }));
    const err = await gate(req()).catch((e) => e);
    expect(err).toBeInstanceOf(ProofError);
    expect(err.name).toBe("Error");
  });
});

describe("capability advertisement", () => {
  // The gate reaches the handler as an opaque authenticate callback, so the
  // handler cannot derive the posture — an operator that forgets to declare it
  // ships an enforcing worker that looks unenforced, and the reverse mistake
  // (advertising in `allow`) tells a proxy the hop is protected when it is not.
  function health(proxyProofRequired: boolean): Promise<Response> {
    const p = new Protocol("proof-capability");
    p.unary("echo", {
      params: { message: str },
      result: { message: str },
      handler: async (params) => ({ message: params.message }),
    });
    const handler = createHttpHandler(p, { proxyProofRequired, corsOrigins: "*" });
    return handler(new Request("http://localhost/health", { method: "GET" }));
  }

  test("require mode advertises the header", async () => {
    const resp = await health(true);
    expect(resp.headers.get(PROOF_REQUIRED_HEADER)).toBe("true");
  });

  test("allow and off modes do not advertise it", async () => {
    // Both postures leave the option unset — `allow` never denies and `off`
    // installs no gate at all, so neither may claim enforcement.
    const resp = await health(false);
    expect(resp.headers.get(PROOF_REQUIRED_HEADER)).toBeNull();
  });

  test("the advertisement is CORS-exposed only when emitted", async () => {
    expect((await health(true)).headers.get("Access-Control-Expose-Headers")).toContain(PROOF_REQUIRED_HEADER);
    expect((await health(false)).headers.get("Access-Control-Expose-Headers")).not.toContain(PROOF_REQUIRED_HEADER);
  });
});

describe("secret parsing", () => {
  test("kid doubles as the label", () => {
    const parsed = parseProofSecrets(`prod-use1:${"11".repeat(32)}`);
    expect(parsed["prod-use1"].label).toBe("prod-use1");
  });

  for (const bad of ["prod-use1", "prod-use1:zz", `bad!kid:${"11".repeat(32)}`, ""]) {
    test(`refuses ${JSON.stringify(bad)}`, () => {
      expect(() => parseProofSecrets(bad)).toThrow();
    });
  }
});
