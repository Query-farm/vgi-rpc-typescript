// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/** Strict SPIFFE evidence delivered by explicitly trusted HTTP proxies. */

import {
  IdentityAssurance,
  PeerIdentity,
  type PeerIdentityProvider,
  PeerIdentityRejectedError,
  PeerIdentityResult,
  PeerIdentityStatus,
  type PeerResolutionContext,
  PeerSubjectKind,
  SubjectStability,
} from "../identity.js";
import { normalizeIpLiteral, normalizeTrustedProxyAddresses } from "../ip.js";

const PROVIDER = "spiffe";
const TRUST_DOMAIN = /^[a-z0-9](?:[a-z0-9._-]{0,253}[a-z0-9])?$/;
const SPIFFE_PATH = /^\/(?:[A-Za-z0-9._-]+)(?:\/[A-Za-z0-9._-]+)*$/;
const HTTP_FIELD_NAME = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;
const XFCC_KEY = /^[A-Za-z][A-Za-z0-9_-]*$/;
const SHA256 = /^[0-9A-Fa-f]{64}$/;
const PERCENT_ESCAPE = /%(?:[0-9A-Fa-f]{2})/g;
const NODE_CRYPTO = "node:crypto";

type X509Certificate = any;
let x509Constructor: Promise<any> | undefined;

function loadX509(): Promise<any> {
  x509Constructor ??= (async () => {
    const req: any = (import.meta as any).require ?? (globalThis as any).require ?? null;
    if (req) return req(NODE_CRYPTO).X509Certificate;
    // Keep node:crypto out of browser/workerd bundles while supporting Node ESM.
    const crypto = await import(NODE_CRYPTO);
    if (!crypto.X509Certificate) throw new Error("node:crypto X509Certificate is unavailable");
    return crypto.X509Certificate;
  })();
  return x509Constructor;
}

function ascii(value: string): boolean {
  for (let index = 0; index < value.length; index++) {
    if (value.charCodeAt(index) > 0x7f) return false;
  }
  return true;
}

function hasControl(value: string): boolean {
  for (let index = 0; index < value.length; index++) {
    const code = value.charCodeAt(index);
    if (code <= 0x1f || code === 0x7f) return true;
  }
  return false;
}

function byteLength(value: string): number {
  return new TextEncoder().encode(value).length;
}

/** Preserve Node/Bun `IncomingMessage.rawHeaders` multiplicity for identity resolution. */
export function headersFromNodeRawHeaders(rawHeaders: readonly string[]): ReadonlyMap<string, readonly string[]> {
  if (rawHeaders.length % 2 !== 0) throw new PeerIdentityRejectedError("rawHeaders contains an unmatched name");
  const headers = new Map<string, string[]>();
  for (let index = 0; index < rawHeaders.length; index += 2) {
    const name = rawHeaders[index];
    const value = rawHeaders[index + 1];
    const values = headers.get(name) ?? [];
    values.push(value);
    headers.set(name, values);
  }
  return new Map([...headers].map(([name, values]) => [name, Object.freeze(values)]));
}

function validateDomainsAndProxies(
  trustDomains: Iterable<string>,
  trustedProxyAddresses: Iterable<string>,
): { domains: ReadonlySet<string>; proxies: ReadonlySet<string> } {
  const domains = new Set(trustDomains);
  const proxies = normalizeTrustedProxyAddresses(trustedProxyAddresses, "trustedProxyAddresses");
  if (domains.size === 0) {
    throw new TypeError("trustDomains and trustedProxyAddresses must not be empty");
  }
  for (const domain of domains) {
    if (!TRUST_DOMAIN.test(domain)) throw new TypeError(`invalid SPIFFE trust domain: ${JSON.stringify(domain)}`);
  }
  return { domains, proxies };
}

function validateHeaderName(value: string, label: string): void {
  if (!HTTP_FIELD_NAME.test(value)) throw new TypeError(`${label} must be a valid HTTP field name`);
}

/** Validate one canonical workload SPIFFE ID and return its trust domain. */
export function validateSpiffeId(value: string, trustDomains: ReadonlySet<string>): string {
  if (!value || !ascii(value) || byteLength(value) > 2048 || value.includes("%")) {
    throw new TypeError("SPIFFE ID is empty, non-ASCII, percent-encoded, or exceeds 2048 bytes");
  }
  if (value.includes("?") || value.includes("#")) throw new TypeError("SPIFFE ID cannot contain query or fragment");
  const match = /^spiffe:\/\/([^/]+)(\/.*)$/.exec(value);
  if (!match) throw new TypeError("invalid SPIFFE ID scheme or authority");
  const trustDomain = match[1];
  const path = match[2];
  if (!TRUST_DOMAIN.test(trustDomain) || !SPIFFE_PATH.test(path)) {
    throw new TypeError("SPIFFE ID trust domain or path is not canonical");
  }
  if (path.split("/").some((segment) => segment === "." || segment === "..")) {
    throw new TypeError("SPIFFE ID path cannot contain dot segments");
  }
  if (!trustDomains.has(trustDomain)) throw new TypeError("SPIFFE trust domain is not allowed");
  return trustDomain;
}

interface DerNode {
  readonly tag: number;
  readonly start: number;
  readonly end: number;
  readonly bytes: Uint8Array;
}

function readDer(bytes: Uint8Array, offset: number): DerNode {
  if (offset + 2 > bytes.length) throw new TypeError("truncated DER value");
  const tag = bytes[offset];
  const first = bytes[offset + 1];
  let length = 0;
  let header = 2;
  if ((first & 0x80) === 0) {
    length = first;
  } else {
    const count = first & 0x7f;
    if (count === 0 || count > 4 || offset + 2 + count > bytes.length) throw new TypeError("invalid DER length");
    header += count;
    for (let index = 0; index < count; index++) length = length * 256 + bytes[offset + 2 + index];
    if (length < 128) throw new TypeError("non-canonical DER length");
  }
  const start = offset + header;
  const end = start + length;
  if (!Number.isSafeInteger(end) || end > bytes.length) throw new TypeError("truncated DER body");
  return { tag, start, end, bytes };
}

function derChildren(node: DerNode): DerNode[] {
  const children: DerNode[] = [];
  let offset = node.start;
  while (offset < node.end) {
    const child = readDer(node.bytes, offset);
    if (child.end > node.end) throw new TypeError("DER child exceeds parent");
    children.push(child);
    offset = child.end;
  }
  if (offset !== node.end) throw new TypeError("malformed DER children");
  return children;
}

function derContent(node: DerNode): Uint8Array {
  return node.bytes.subarray(node.start, node.end);
}

function oid(node: DerNode): string {
  if (node.tag !== 0x06) throw new TypeError("expected DER OID");
  const bytes = derContent(node);
  if (bytes.length === 0) throw new TypeError("empty DER OID");
  const parts = [Math.min(2, Math.floor(bytes[0] / 40)), 0];
  parts[1] = bytes[0] - parts[0] * 40;
  let value = 0;
  for (let index = 1; index < bytes.length; index++) {
    value = value * 128 + (bytes[index] & 0x7f);
    if (!Number.isSafeInteger(value)) throw new TypeError("oversized DER OID");
    if ((bytes[index] & 0x80) === 0) {
      parts.push(value);
      value = 0;
    }
  }
  if ((bytes[bytes.length - 1] & 0x80) !== 0) throw new TypeError("truncated DER OID");
  return parts.join(".");
}

interface SvidProfile {
  readonly spiffeId: string;
  readonly subjectEmpty: boolean;
  readonly sanCritical: boolean;
  readonly ca: boolean;
  readonly keyUsageCritical: boolean;
  readonly digitalSignature: boolean;
  readonly keyCertSign: boolean;
  readonly crlSign: boolean;
  readonly extendedKeyUsage?: ReadonlySet<string>;
}

function parseSvidProfile(raw: Uint8Array): SvidProfile {
  const certificate = readDer(raw, 0);
  if (certificate.tag !== 0x30 || certificate.end !== raw.length) throw new TypeError("invalid certificate DER");
  const certificateParts = derChildren(certificate);
  if (certificateParts.length !== 3 || certificateParts[0].tag !== 0x30)
    throw new TypeError("invalid certificate shape");
  const tbs = certificateParts[0];
  const parts = derChildren(tbs);
  const base = parts[0]?.tag === 0xa0 ? 1 : 0;
  if (parts.length < base + 6 || parts[base + 4].tag !== 0x30) throw new TypeError("invalid TBSCertificate");
  const subjectEmpty = parts[base + 4].start === parts[base + 4].end;
  const extensionWrapper = parts.find((part) => part.tag === 0xa3);
  if (!extensionWrapper) throw new TypeError("X.509-SVID extensions are missing");
  const wrapperParts = derChildren(extensionWrapper);
  if (wrapperParts.length !== 1 || wrapperParts[0].tag !== 0x30) throw new TypeError("invalid certificate extensions");
  const extensions = new Map<string, { critical: boolean; value: Uint8Array }>();
  for (const extension of derChildren(wrapperParts[0])) {
    if (extension.tag !== 0x30) throw new TypeError("invalid certificate extension");
    const values = derChildren(extension);
    if (values.length < 2 || values.length > 3) throw new TypeError("invalid certificate extension shape");
    const name = oid(values[0]);
    let critical = false;
    let valueNode = values[1];
    if (values[1].tag === 0x01) {
      const boolean = derContent(values[1]);
      if (boolean.length !== 1 || (boolean[0] !== 0x00 && boolean[0] !== 0xff) || values.length !== 3) {
        throw new TypeError("invalid extension critical flag");
      }
      critical = boolean[0] === 0xff;
      valueNode = values[2];
    }
    if (valueNode.tag !== 0x04 || extensions.has(name)) throw new TypeError("invalid or duplicate extension");
    extensions.set(name, { critical, value: derContent(valueNode) });
  }

  const san = extensions.get("2.5.29.17");
  const basic = extensions.get("2.5.29.19");
  const usage = extensions.get("2.5.29.15");
  if (!san || !basic || !usage) throw new TypeError("required X.509-SVID extension is missing");
  const sanRoot = readDer(san.value, 0);
  if (sanRoot.tag !== 0x30 || sanRoot.end !== san.value.length) throw new TypeError("invalid SAN extension");
  const uriSans = derChildren(sanRoot)
    .filter((name) => name.tag === 0x86)
    .map((name) => {
      const value = new TextDecoder("ascii", { fatal: true }).decode(derContent(name));
      if (!ascii(value)) throw new TypeError("non-ASCII URI SAN");
      return value;
    });
  if (uriSans.length !== 1) throw new TypeError("X.509-SVID must contain exactly one URI SAN");

  const basicRoot = readDer(basic.value, 0);
  if (basicRoot.tag !== 0x30 || basicRoot.end !== basic.value.length) throw new TypeError("invalid basic constraints");
  const basicParts = derChildren(basicRoot);
  const ca = basicParts[0]?.tag === 0x01 && derContent(basicParts[0])[0] !== 0;

  const usageRoot = readDer(usage.value, 0);
  if (usageRoot.tag !== 0x03 || usageRoot.end !== usage.value.length) throw new TypeError("invalid key usage");
  const bits = derContent(usageRoot);
  if (bits.length < 2 || bits[0] > 7) throw new TypeError("invalid key usage bits");
  const bit = (index: number): boolean => {
    const byte = bits[1 + Math.floor(index / 8)];
    return byte !== undefined && (byte & (0x80 >> (index % 8))) !== 0;
  };

  const eku = extensions.get("2.5.29.37");
  let extendedKeyUsage: ReadonlySet<string> | undefined;
  if (eku) {
    const root = readDer(eku.value, 0);
    if (root.tag !== 0x30 || root.end !== eku.value.length) throw new TypeError("invalid extended key usage");
    extendedKeyUsage = new Set(derChildren(root).map(oid));
  }
  return {
    spiffeId: uriSans[0],
    subjectEmpty,
    sanCritical: san.critical,
    ca,
    keyUsageCritical: usage.critical,
    digitalSignature: bit(0),
    keyCertSign: bit(5),
    crlSign: bit(6),
    extendedKeyUsage,
  };
}

function identityFromCertificate(
  cert: X509Certificate,
  domains: ReadonlySet<string>,
  context: PeerResolutionContext,
  evidenceSource: string,
): PeerIdentity {
  const now = Date.now();
  const notBefore = Date.parse(cert.validFrom);
  const notAfter = Date.parse(cert.validTo);
  if (!Number.isFinite(notBefore) || !Number.isFinite(notAfter) || now < notBefore || now > notAfter) {
    throw new TypeError("X.509-SVID is outside its validity period");
  }
  const profile = parseSvidProfile(new Uint8Array(cert.raw));
  if (profile.subjectEmpty && !profile.sanCritical) throw new TypeError("subjectless SVID requires critical SAN");
  if (profile.ca) throw new TypeError("X.509-SVID leaf cannot be a CA");
  if (!profile.keyUsageCritical || !profile.digitalSignature || profile.keyCertSign || profile.crlSign) {
    throw new TypeError("invalid X.509-SVID key usage");
  }
  if (
    profile.extendedKeyUsage &&
    (!profile.extendedKeyUsage.has("1.3.6.1.5.5.7.3.1") || !profile.extendedKeyUsage.has("1.3.6.1.5.5.7.3.2"))
  ) {
    throw new TypeError("X.509-SVID extended key usage must include clientAuth and serverAuth");
  }
  const trustDomain = validateSpiffeId(profile.spiffeId, domains);
  return new PeerIdentity({
    provider: PROVIDER,
    evidenceSource,
    assurance: IdentityAssurance.CONFIGURED_PROXY,
    issuer: `spiffe://${trustDomain}`,
    transport: "http",
    subjectKind: PeerSubjectKind.WORKLOAD,
    subjectKey: profile.spiffeId,
    subjectStability: SubjectStability.STABLE,
    subjectVerified: true,
    sourceAddress: context.assertedPeer,
    proxyAddress: context.immediatePeer,
  });
}

interface CertificateProviderOptions {
  readonly trustDomains: Iterable<string>;
  readonly trustedProxyAddresses: Iterable<string>;
  readonly certificateHeader: string;
  readonly verificationHeader?: string;
  readonly verificationValue?: string;
  readonly maxHeaderBytes: number;
  readonly evidenceSource: string;
}

function certificateProvider(options: CertificateProviderOptions): PeerIdentityProvider {
  const { domains, proxies } = validateDomainsAndProxies(options.trustDomains, options.trustedProxyAddresses);
  validateHeaderName(options.certificateHeader, "certificateHeader");
  if (options.verificationHeader !== undefined) {
    validateHeaderName(options.verificationHeader, "verificationHeader");
    if (options.certificateHeader.toLowerCase() === options.verificationHeader.toLowerCase()) {
      throw new TypeError("certificate and verification headers must be distinct");
    }
    if (hasControl(options.verificationValue ?? "")) throw new TypeError("invalid verification value");
  }
  if (!Number.isSafeInteger(options.maxHeaderBytes) || options.maxHeaderBytes <= 0) {
    throw new TypeError("maxHeaderBytes must be a positive integer");
  }
  return {
    provider: PROVIDER,
    async resolve(context): Promise<PeerIdentityResult> {
      const immediatePeer = normalizeIpLiteral(context.immediatePeer ?? "");
      if (!immediatePeer || !proxies.has(immediatePeer)) {
        return new PeerIdentityResult(PROVIDER, PeerIdentityStatus.UNTRUSTED_PROXY);
      }
      let raw: string | undefined;
      let verified: string | undefined;
      try {
        raw = context.header(options.certificateHeader);
        if (options.verificationHeader) verified = context.header(options.verificationHeader);
      } catch (error) {
        if (error instanceof PeerIdentityRejectedError)
          return new PeerIdentityResult(PROVIDER, PeerIdentityStatus.INVALID);
        throw error;
      }
      if (!raw) return new PeerIdentityResult(PROVIDER, PeerIdentityStatus.NO_MATCH);
      if (!ascii(raw) || byteLength(raw) > options.maxHeaderBytes) {
        return new PeerIdentityResult(PROVIDER, PeerIdentityStatus.INVALID);
      }
      if (
        options.verificationHeader &&
        (verified === undefined || byteLength(verified) > 64 || verified !== options.verificationValue)
      ) {
        return new PeerIdentityResult(PROVIDER, PeerIdentityStatus.INVALID);
      }
      try {
        const decoded = decodeURIComponent(raw);
        if (
          !ascii(decoded) ||
          byteLength(decoded) > options.maxHeaderBytes ||
          decoded.split("-----BEGIN CERTIFICATE-----").length - 1 !== 1 ||
          decoded.split("-----END CERTIFICATE-----").length - 1 !== 1 ||
          !decoded.trim().endsWith("-----END CERTIFICATE-----")
        ) {
          throw new TypeError("invalid certificate header");
        }
        const X509 = await loadX509();
        const identity = identityFromCertificate(new X509(decoded), domains, context, options.evidenceSource);
        return PeerIdentityResult.available(identity);
      } catch {
        return new PeerIdentityResult(PROVIDER, PeerIdentityStatus.INVALID);
      }
    },
  };
}

export interface SpiffeX509HeaderProviderOptions {
  readonly trustDomains: Iterable<string>;
  readonly trustedProxyAddresses: Iterable<string>;
  readonly header?: string;
  readonly chainVerifiedHeader: string;
  readonly chainVerifiedValue?: string;
  readonly maxHeaderBytes?: number;
}

export function spiffeX509HeaderProvider(options: SpiffeX509HeaderProviderOptions): PeerIdentityProvider {
  if (!options.chainVerifiedHeader) throw new TypeError("chainVerifiedHeader is required");
  return certificateProvider({
    trustDomains: options.trustDomains,
    trustedProxyAddresses: options.trustedProxyAddresses,
    certificateHeader: options.header ?? "X-SSL-Client-Cert",
    verificationHeader: options.chainVerifiedHeader,
    verificationValue: options.chainVerifiedValue ?? "true",
    maxHeaderBytes: options.maxHeaderBytes ?? 16_384,
    evidenceSource: "verified_certificate_header",
  });
}

export interface CertificateProxySpiffeOptions {
  readonly trustDomains: Iterable<string>;
  readonly trustedProxyAddresses: Iterable<string>;
  readonly certificateHeader?: string;
  readonly verificationHeader?: string;
  readonly maxHeaderBytes?: number;
}

export function nginxSpiffeProvider(options: CertificateProxySpiffeOptions): PeerIdentityProvider {
  return certificateProvider({
    trustDomains: options.trustDomains,
    trustedProxyAddresses: options.trustedProxyAddresses,
    certificateHeader: options.certificateHeader ?? "X-SSL-Client-Cert",
    verificationHeader: options.verificationHeader ?? "X-SSL-Client-Verify",
    verificationValue: "SUCCESS",
    maxHeaderBytes: options.maxHeaderBytes ?? 16_384,
    evidenceSource: "nginx_mtls",
  });
}

export function azureApplicationGatewaySpiffeProvider(options: CertificateProxySpiffeOptions): PeerIdentityProvider {
  return certificateProvider({
    trustDomains: options.trustDomains,
    trustedProxyAddresses: options.trustedProxyAddresses,
    certificateHeader: options.certificateHeader ?? "X-Client-Certificate",
    verificationHeader: options.verificationHeader ?? "X-Client-Certificate-Verification",
    verificationValue: "SUCCESS",
    maxHeaderBytes: options.maxHeaderBytes ?? 16_384,
    evidenceSource: "azure_application_gateway_mtls_strict",
  });
}

export interface AwsAlbSpiffeOptions {
  readonly trustDomains: Iterable<string>;
  readonly trustedProxyAddresses: Iterable<string>;
  readonly leafHeader?: string;
  readonly maxHeaderBytes?: number;
}

export function awsAlbSpiffeProvider(options: AwsAlbSpiffeOptions): PeerIdentityProvider {
  return certificateProvider({
    trustDomains: options.trustDomains,
    trustedProxyAddresses: options.trustedProxyAddresses,
    certificateHeader: options.leafHeader ?? "X-Amzn-Mtls-Clientcert-Leaf",
    maxHeaderBytes: options.maxHeaderBytes ?? 16_384,
    evidenceSource: "aws_alb_mtls_verify",
  });
}

export interface GcpLoadBalancerSpiffeOptions {
  readonly trustDomains: Iterable<string>;
  readonly trustedProxyAddresses: Iterable<string>;
  readonly spiffeIdHeader?: string;
  readonly presentHeader?: string;
  readonly chainVerifiedHeader?: string;
  readonly errorHeader?: string;
}

export function gcpLoadBalancerSpiffeProvider(options: GcpLoadBalancerSpiffeOptions): PeerIdentityProvider {
  const { domains, proxies } = validateDomainsAndProxies(options.trustDomains, options.trustedProxyAddresses);
  const headers = {
    spiffe: options.spiffeIdHeader ?? "X-Client-Cert-Spiffe-Id",
    present: options.presentHeader ?? "X-Client-Cert-Present",
    verified: options.chainVerifiedHeader ?? "X-Client-Cert-Chain-Verified",
    error: options.errorHeader ?? "X-Client-Cert-Error",
  };
  for (const [name, value] of Object.entries(headers)) validateHeaderName(value, name);
  if (new Set(Object.values(headers).map((header) => header.toLowerCase())).size !== 4) {
    throw new TypeError("GCP mTLS header names must be distinct");
  }
  return {
    provider: PROVIDER,
    resolve(context): PeerIdentityResult {
      const immediatePeer = normalizeIpLiteral(context.immediatePeer ?? "");
      if (!immediatePeer || !proxies.has(immediatePeer)) {
        return new PeerIdentityResult(PROVIDER, PeerIdentityStatus.UNTRUSTED_PROXY);
      }
      try {
        const present = context.header(headers.present);
        const verified = context.header(headers.verified);
        const spiffeId = context.header(headers.spiffe);
        const error = context.header(headers.error);
        if (present === "false" && (verified === undefined || verified === "false") && spiffeId === undefined) {
          return new PeerIdentityResult(PROVIDER, PeerIdentityStatus.NO_MATCH);
        }
        if (present !== "true" || verified !== "true" || (error !== undefined && error !== "") || !spiffeId) {
          return new PeerIdentityResult(PROVIDER, PeerIdentityStatus.INVALID);
        }
        const trustDomain = validateSpiffeId(spiffeId, domains);
        return PeerIdentityResult.available(
          new PeerIdentity({
            provider: PROVIDER,
            evidenceSource: "gcp_load_balancer_mtls",
            assurance: IdentityAssurance.CONFIGURED_PROXY,
            issuer: `spiffe://${trustDomain}`,
            transport: "http",
            subjectKind: PeerSubjectKind.WORKLOAD,
            subjectKey: spiffeId,
            subjectStability: SubjectStability.STABLE,
            subjectVerified: true,
            attributes: { client_certificate_present: true, client_certificate_chain_verified: true },
            sourceAddress: context.assertedPeer,
            proxyAddress: context.immediatePeer,
          }),
        );
      } catch {
        return new PeerIdentityResult(PROVIDER, PeerIdentityStatus.INVALID);
      }
    },
  };
}

function splitXfcc(value: string, delimiter: string): string[] {
  const parts: string[] = [];
  let current = "";
  let quoted = false;
  let escaped = false;
  for (const character of value) {
    if (escaped) {
      if (character !== '"' && character !== "\\") throw new TypeError("unsupported XFCC quoted escape");
      current += character;
      escaped = false;
    } else if (quoted && character === "\\") {
      escaped = true;
    } else if (character === '"') {
      quoted = !quoted;
      current += character;
    } else if (character === delimiter && !quoted) {
      parts.push(current);
      current = "";
    } else {
      current += character;
    }
  }
  if (quoted || escaped) throw new TypeError("unterminated XFCC quoted value");
  parts.push(current);
  return parts;
}

function xfccValue(value: string): string {
  if (value.startsWith('"') || value.endsWith('"')) {
    if (value.length < 2 || !value.startsWith('"') || !value.endsWith('"'))
      throw new TypeError("malformed quoted XFCC");
    return value.slice(1, -1);
  }
  if (!value || /[,;=]/.test(value)) throw new TypeError("invalid unquoted XFCC value");
  return value;
}

function strictPercentDecode(value: string): string {
  if (value.replace(PERCENT_ESCAPE, "").includes("%")) throw new TypeError("invalid XFCC percent escape");
  const decoded = decodeURIComponent(value);
  if (hasControl(decoded)) throw new TypeError("decoded XFCC value contains controls");
  return decoded;
}

function parseSingleXfcc(raw: string, maximum: number): ReadonlyMap<string, readonly string[]> {
  if (!ascii(raw) || byteLength(raw) > maximum || hasControl(raw)) throw new TypeError("invalid XFCC bytes");
  const elements = splitXfcc(raw, ",");
  if (elements.length !== 1 || !elements[0].trim()) throw new TypeError("XFCC must contain one element");
  const fields = new Map<string, string[]>();
  for (const rawPair of splitXfcc(elements[0], ";")) {
    const pair = rawPair.trim();
    const equal = pair.indexOf("=");
    if (!pair || equal < 0) throw new TypeError("malformed XFCC field");
    const rawKey = pair.slice(0, equal).trim();
    const key = rawKey.toLowerCase();
    if (!XFCC_KEY.test(rawKey) || !["by", "hash", "cert", "chain", "subject", "uri", "dns", "issuer"].includes(key)) {
      throw new TypeError("unsupported XFCC field");
    }
    let value = xfccValue(pair.slice(equal + 1).trim());
    if (["by", "uri", "cert", "chain"].includes(key)) value = strictPercentDecode(value);
    if (!["by", "uri", "dns"].includes(key) && fields.has(key)) throw new TypeError("duplicate XFCC singleton");
    const values = fields.get(key) ?? [];
    values.push(value);
    fields.set(key, values);
  }
  return fields;
}

export interface EnvoyXfccSpiffeOptions {
  readonly trustDomains: Iterable<string>;
  readonly trustedProxyAddresses: Iterable<string>;
  readonly header?: string;
  readonly maxHeaderBytes?: number;
}

export function envoyXfccSpiffeProvider(options: EnvoyXfccSpiffeOptions): PeerIdentityProvider {
  const { domains, proxies } = validateDomainsAndProxies(options.trustDomains, options.trustedProxyAddresses);
  const header = options.header ?? "X-Forwarded-Client-Cert";
  const maximum = options.maxHeaderBytes ?? 16_384;
  validateHeaderName(header, "header");
  if (!Number.isSafeInteger(maximum) || maximum <= 0) throw new TypeError("maxHeaderBytes must be positive");
  return {
    provider: PROVIDER,
    resolve(context): PeerIdentityResult {
      const immediatePeer = normalizeIpLiteral(context.immediatePeer ?? "");
      if (!immediatePeer || !proxies.has(immediatePeer)) {
        return new PeerIdentityResult(PROVIDER, PeerIdentityStatus.UNTRUSTED_PROXY);
      }
      try {
        const raw = context.header(header);
        if (raw === undefined) return new PeerIdentityResult(PROVIDER, PeerIdentityStatus.NO_MATCH);
        const fields = parseSingleXfcc(raw, maximum);
        const uris = fields.get("uri") ?? [];
        const hashes = fields.get("hash") ?? [];
        if (uris.length !== 1 || hashes.length !== 1 || !SHA256.test(hashes[0]))
          throw new TypeError("ambiguous XFCC identity");
        const trustDomain = validateSpiffeId(uris[0], domains);
        const by = fields.get("by") ?? [];
        return PeerIdentityResult.available(
          new PeerIdentity({
            provider: PROVIDER,
            evidenceSource: "envoy_xfcc_sanitize_set",
            assurance: IdentityAssurance.CONFIGURED_PROXY,
            issuer: `spiffe://${trustDomain}`,
            transport: "http",
            subjectKind: PeerSubjectKind.WORKLOAD,
            subjectKey: uris[0],
            subjectStability: SubjectStability.STABLE,
            subjectVerified: true,
            attributes: {
              certificate_sha256: hashes[0].toLowerCase(),
              ...(by.length > 0 ? { proxy_identities: by } : {}),
            },
            sourceAddress: context.assertedPeer,
            proxyAddress: context.immediatePeer,
          }),
        );
      } catch {
        return new PeerIdentityResult(PROVIDER, PeerIdentityStatus.INVALID);
      }
    },
  };
}
