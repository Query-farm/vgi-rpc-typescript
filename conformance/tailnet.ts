// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/** Live-Tailnet qualification adapter for the supported TypeScript surfaces. */

import { createHash } from "node:crypto";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import {
  type CallContext,
  createHttpHandler,
  createSocks5hFetch,
  headersFromNodeRawHeaders,
  httpConnect,
  PeerIdentityStatus,
  type PeerResolutionOptions,
  Protocol,
  peerIdentityPrimary,
  requirePeerIdentity,
  serveTcp,
  str,
  tailscaleLocalApiIdentityProvider,
  tailscaleServeIdentityProvider,
  tcpConnect,
  tcpConnectSocks5h,
} from "../src/index.js";

const PROVIDER = "tailscale";
const MAX_REQUEST_BYTES = 16 * 1024 * 1024;

export interface ClientExpectation {
  evidenceSource: string;
  assurance: string;
  issuer: string;
  subjectKind: string;
  subjectStability: string;
  capability: string;
  tag?: string;
  targetKind?: string;
  authenticated: boolean;
  proxyPresent: boolean;
  spoofLogin?: string;
}

type JsonObject = Record<string, unknown>;

function object(value: unknown, name: string): JsonObject {
  if (value === null || Array.isArray(value) || typeof value !== "object") throw new Error(`${name} was absent`);
  return value as JsonObject;
}

function array(value: unknown, name: string): unknown[] {
  if (!Array.isArray(value)) throw new Error(`${name} was absent`);
  return value;
}

/** Validate the redacted snapshot returned by the canonical Python worker. */
export function validateSnapshot(raw: string, expected: ClientExpectation): void {
  const payload = object(JSON.parse(raw), "snapshot");
  const statuses = object(payload.provider_status, "provider_status");
  const identities = array(payload.identities, "identities");
  if (statuses[PROVIDER] !== PeerIdentityStatus.AVAILABLE || identities.length !== 1) {
    throw new Error("snapshot did not contain one available Tailscale identity");
  }
  const identity = object(identities[0], "identity");
  const capabilities = array(identity.capability_names, "capability_names");
  const tags = array(identity.tags, "tags");
  const target = identity.capability_target == null ? null : object(identity.capability_target, "capability_target");
  const auth = object(payload.auth, "auth");
  const subjectExpected = expected.subjectStability !== "none";
  const targetMatches = expected.targetKind === undefined || target?.kind === expected.targetKind;
  const domainExpected = expected.authenticated ? PROVIDER : null;
  const matches =
    identity.provider === PROVIDER &&
    identity.evidence_source === expected.evidenceSource &&
    identity.assurance === expected.assurance &&
    identity.issuer === expected.issuer &&
    identity.subject_kind === expected.subjectKind &&
    identity.subject_stability === expected.subjectStability &&
    identity.subject_verified === subjectExpected &&
    (identity.subject_fingerprint != null) === subjectExpected &&
    identity.capabilities_verified === true &&
    capabilities.includes(expected.capability) &&
    (expected.tag === undefined || tags.includes(expected.tag)) &&
    targetMatches &&
    identity.proxy_present === expected.proxyPresent &&
    auth.authenticated === expected.authenticated &&
    auth.domain === domainExpected &&
    auth.principal_matches_identity === expected.authenticated &&
    auth.peer_evidence_binding_present === true;
  if (!matches) throw new Error("unexpected redacted Tailnet snapshot");
  if (expected.spoofLogin) {
    const spoofed = createHash("sha256").update(`login:${expected.spoofLogin}`).digest("hex");
    if (identity.subject_fingerprint === spoofed) throw new Error("Serve trusted a client-supplied identity header");
  }
}

function option(args: readonly string[], name: string): string | undefined {
  const index = args.indexOf(name);
  return index < 0 ? undefined : args[index + 1];
}

function required(args: readonly string[], name: string): string {
  const value = option(args, name);
  if (!value) throw new Error(`required option is missing: ${name}`);
  return value;
}

function flag(args: readonly string[], name: string): boolean {
  return args.includes(name);
}

function port(args: readonly string[], name: string): number {
  const value = Number(required(args, name));
  if (!Number.isInteger(value) || value < 1 || value > 65_535) throw new Error(`${name} must be 1..65535`);
  return value;
}

export function clientExpectation(args: readonly string[]): ClientExpectation {
  return {
    evidenceSource: required(args, "--expected-evidence-source"),
    assurance: required(args, "--expected-assurance"),
    issuer: required(args, "--expected-issuer"),
    subjectKind: required(args, "--expected-subject-kind"),
    subjectStability: required(args, "--expected-subject-stability"),
    capability: required(args, "--expected-capability"),
    tag: option(args, "--expected-tag"),
    targetKind: option(args, "--expected-target-kind"),
    authenticated: flag(args, "--expect-authenticated"),
    proxyPresent: flag(args, "--expect-proxy"),
    spoofLogin: option(args, "--spoof-login"),
  };
}

function injectingFetch(base: typeof fetch, spoofLogin?: string): typeof fetch {
  if (!spoofLogin) return base;
  return ((input: URL | RequestInfo, init?: RequestInit) => {
    const headers = new Headers(init?.headers);
    headers.set("Tailscale-User-Login", spoofLogin);
    return base(input, { ...init, headers });
  }) as typeof fetch;
}

async function callSnapshots(client: ReturnType<typeof httpConnect>, expected: ClientExpectation): Promise<void> {
  try {
    let first: string | undefined;
    for (let attempt = 0; attempt < 2; attempt++) {
      const result = await client.call("snapshot");
      const raw = result?.result;
      if (typeof raw !== "string") throw new Error("snapshot result was not a string");
      validateSnapshot(raw, expected);
      if (first !== undefined && first !== raw) throw new Error("identity evidence changed within a stable test peer");
      first = raw;
    }
  } finally {
    client.close();
  }
}

async function runHttpClient(args: readonly string[]): Promise<void> {
  const url = required(args, "--url");
  const proxy = option(args, "--proxy");
  const expected = clientExpectation(args);
  const baseFetch = proxy ? createSocks5hFetch(proxy, 20_000, 20_000) : fetch;
  const client = httpConnect(url, { fetch: injectingFetch(baseFetch, expected.spoofLogin) });
  await callSnapshots(client, expected);
  console.log("TypeScript HTTP client Tailnet probe passed");
}

async function runTcpClient(args: readonly string[]): Promise<void> {
  const host = required(args, "--host");
  const targetPort = port(args, "--port");
  const proxy = option(args, "--proxy");
  const client = proxy
    ? await tcpConnectSocks5h(host, targetPort, proxy, { connectTimeoutMs: 20_000 })
    : tcpConnect(host, targetPort);
  await callSnapshots(client, clientExpectation(args));
  console.log("TypeScript TCP client Tailnet probe passed");
}

function serveProtocol(issuer: string, capability: string): Protocol {
  return new Protocol("ConformanceService", { protocolVersion: "2.0.0" }).unary("echo_string", {
    params: { value: str },
    result: { result: str },
    handler: ({ value }, context) => {
      const ctx = context as CallContext;
      const evidence = ctx.peerEvidence;
      const identities = evidence?.forProvider(PROVIDER) ?? [];
      const identity = identities[0];
      const binding = ctx.auth.claims.peer_evidence_binding;
      if (
        evidence?.status(PROVIDER) !== PeerIdentityStatus.AVAILABLE ||
        identities.length !== 1 ||
        !identity ||
        identity.evidenceSource !== "serve_proxy" ||
        identity.assurance !== "configured_proxy" ||
        identity.issuer !== issuer ||
        identity.subjectKind !== "unknown" ||
        identity.subjectStability !== "none" ||
        identity.subjectKey !== undefined ||
        identity.subjectVerified ||
        !identity.capabilitiesVerified ||
        !(capability in identity.capabilities) ||
        identity.proxyAddress === undefined ||
        ctx.auth.authenticated ||
        ctx.auth.domain !== "" ||
        ctx.auth.principal !== null ||
        typeof binding !== "string" ||
        binding.length === 0
      ) {
        throw new Error("unexpected Tailscale identity or authentication context");
      }
      return { result: value };
    },
  });
}

async function collectBody(request: IncomingMessage): Promise<Uint8Array | undefined> {
  const declared = Number(request.headers["content-length"] ?? 0);
  if (Number.isFinite(declared) && declared > MAX_REQUEST_BYTES) throw new Error("request exceeds adapter limit");
  const chunks: Buffer[] = [];
  let total = 0;
  for await (const chunk of request) {
    const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    total += bytes.length;
    if (total > MAX_REQUEST_BYTES) throw new Error("request exceeds adapter limit");
    chunks.push(bytes);
  }
  return chunks.length === 0 ? undefined : Buffer.concat(chunks, total);
}

async function writeResponse(response: Response, output: ServerResponse): Promise<void> {
  response.headers.forEach((value, name) => {
    output.setHeader(name, value);
  });
  output.writeHead(response.status);
  output.end(Buffer.from(await response.arrayBuffer()));
}

async function runHttpServer(args: readonly string[]): Promise<void> {
  const host = option(args, "--host") ?? "127.0.0.1";
  const requestedPort = port(args, "--port");
  const issuer = required(args, "--issuer");
  const capability = required(args, "--expected-capability");
  const trusted = [option(args, "--trusted-proxy-ipv4") ?? "127.0.0.1", option(args, "--trusted-proxy-ipv6") ?? "::1"];
  const contexts = new WeakMap<Request, PeerResolutionOptions>();
  const handler = createHttpHandler(serveProtocol(issuer, capability), {
    enableLandingPage: false,
    enableDescribePage: false,
    peerIdentityProviders: [tailscaleServeIdentityProvider({ issuer, trustedProxyAddresses: trusted })],
    peerAuthenticationPolicy: requirePeerIdentity(PROVIDER),
    peerResolutionContext: (request) => contexts.get(request) ?? {},
  });
  const server = createServer(async (incoming, outgoing) => {
    try {
      const address = server.address() as AddressInfo;
      const url = new URL(incoming.url ?? "/", `http://${host}:${address.port}`);
      const headers = new Headers();
      for (let index = 0; index < incoming.rawHeaders.length; index += 2) {
        headers.append(incoming.rawHeaders[index], incoming.rawHeaders[index + 1]);
      }
      const body = incoming.method === "GET" || incoming.method === "HEAD" ? undefined : await collectBody(incoming);
      const request = new Request(url, { method: incoming.method, headers, body });
      contexts.set(request, {
        immediatePeer: incoming.socket.remoteAddress,
        sourceEndpoint:
          incoming.socket.remoteAddress && incoming.socket.remotePort
            ? `${incoming.socket.remoteAddress}:${incoming.socket.remotePort}`
            : undefined,
        headers: headersFromNodeRawHeaders(incoming.rawHeaders),
      });
      try {
        await writeResponse(await handler(request), outgoing);
      } finally {
        contexts.delete(request);
      }
    } catch {
      outgoing.writeHead(500);
      outgoing.end("Internal Server Error");
    }
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(requestedPort, host, resolve);
  });
  const address = server.address() as AddressInfo;
  console.log(`HTTP:${host}:${address.port}`);
  const close = () => server.close(() => process.exit(0));
  process.once("SIGINT", close);
  process.once("SIGTERM", close);
}

function serveTcpProtocol(issuer: string, capability: string, tag: string, proxyPresent: boolean): Protocol {
  return new Protocol("ConformanceService", { protocolVersion: "2.0.0" }).unary("echo_string", {
    params: { value: str },
    result: { result: str },
    handler: ({ value }, context) => {
      const ctx = context as CallContext;
      const identities = ctx.peerEvidence.forProvider(PROVIDER);
      const identity = identities[0];
      if (
        ctx.peerEvidence.status(PROVIDER) !== PeerIdentityStatus.AVAILABLE ||
        identities.length !== 1 ||
        !identity ||
        identity.evidenceSource !== "localapi" ||
        identity.assurance !== "local_daemon" ||
        identity.issuer !== issuer ||
        identity.subjectKind !== "tagged_node" ||
        identity.subjectStability !== "stable" ||
        !identity.subjectVerified ||
        !identity.capabilitiesVerified ||
        !(capability in identity.capabilities) ||
        !(identity.attributes.tags as readonly unknown[] | undefined)?.includes(tag) ||
        (identity.proxyAddress !== undefined) !== proxyPresent ||
        !ctx.auth.authenticated ||
        ctx.auth.domain !== PROVIDER
      ) {
        throw new Error("unexpected Tailscale identity or authentication context");
      }
      return { result: value };
    },
  });
}

async function runTcpServer(args: readonly string[]): Promise<void> {
  const host = option(args, "--host") ?? "0.0.0.0";
  const issuer = required(args, "--issuer");
  const capability = required(args, "--expected-capability");
  const tag = required(args, "--expected-tag");
  const provider = tailscaleLocalApiIdentityProvider({
    issuer,
    unixSocket: option(args, "--localapi-socket") ?? "/var/run/tailscale/tailscaled.sock",
    timeoutMs: 5_000,
  });
  const proxyProtocolV2Required = flag(args, "--proxy-protocol-v2");
  const trustedProxyAddress = option(args, "--trusted-proxy-address");
  if (proxyProtocolV2Required && !trustedProxyAddress) {
    throw new Error("--proxy-protocol-v2 requires --trusted-proxy-address");
  }
  const handle = await serveTcp(serveTcpProtocol(issuer, capability, tag, proxyProtocolV2Required), {
    host,
    port: port(args, "--port"),
    idleTimeout: 0,
    peerIdentityProviders: [provider],
    peerAuthenticationPolicy: peerIdentityPrimary(PROVIDER),
    identityResolutionTimeoutMs: 5_000,
    peerServiceName: option(args, "--service-name"),
    proxyProtocolV2Required,
    trustedProxyAddresses: trustedProxyAddress ? [trustedProxyAddress] : [],
  });
  const close = () => void handle.stop().then(() => process.exit(0));
  process.once("SIGINT", close);
  process.once("SIGTERM", close);
  await handle.done;
}

async function main(): Promise<void> {
  const [mode, ...args] = process.argv.slice(2);
  if (mode === "client-http") await runHttpClient(args);
  else if (mode === "client-tcp") await runTcpClient(args);
  else if (mode === "server-http") await runHttpServer(args);
  else if (mode === "server-tcp") await runTcpServer(args);
  else throw new Error("usage: tailnet.ts client-http|client-tcp|server-http|server-tcp [options]");
}

if (import.meta.main) {
  main().catch((error: unknown) => {
    console.error(`vgi-rpc-tailnet-typescript: ${error instanceof Error ? error.message : "adapter failed"}`);
    process.exit(1);
  });
}
