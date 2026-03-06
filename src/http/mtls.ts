// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { createHash, X509Certificate } from "node:crypto";
import { AuthContext } from "../auth.js";
import type { AuthenticateFn } from "./auth.js";

// ---------------------------------------------------------------------------
// XFCC types and parser (no crypto needed)
// ---------------------------------------------------------------------------

/** A single element from an `x-forwarded-client-cert` header. */
export interface XfccElement {
  hash: string | null;
  cert: string | null;
  subject: string | null;
  uri: string | null;
  dns: readonly string[];
  by: string | null;
}

/** Receives a parsed XFCC element, returns an AuthContext on success. Must throw on failure. */
export type XfccValidateFn = (element: XfccElement) => AuthContext | Promise<AuthContext>;

/** Receives a parsed X509Certificate, returns an AuthContext on success. Must throw on failure. */
export type CertValidateFn = (cert: X509Certificate) => AuthContext | Promise<AuthContext>;

function splitRespectingQuotes(text: string, delimiter: string): string[] {
  const parts: string[] = [];
  let current: string[] = [];
  let inQuotes = false;
  let i = 0;
  while (i < text.length) {
    const ch = text[i];
    if (ch === '"') {
      inQuotes = !inQuotes;
      current.push(ch);
    } else if (ch === "\\" && inQuotes && i + 1 < text.length) {
      current.push(ch);
      current.push(text[i + 1]);
      i++;
    } else if (ch === delimiter && !inQuotes) {
      parts.push(current.join(""));
      current = [];
    } else {
      current.push(ch);
    }
    i++;
  }
  parts.push(current.join(""));
  return parts;
}

function unescapeQuoted(text: string): string {
  return text.replace(/\\(.)/g, "$1");
}

/** Extract the CN value from an RFC 4514 or similar DN string. */
function extractCn(subject: string): string {
  for (const part of subject.split(/(?<!\\),/)) {
    const trimmed = part.trim();
    if (trimmed.toUpperCase().startsWith("CN=")) {
      return trimmed.slice(3);
    }
  }
  return "";
}

/**
 * Parse an `x-forwarded-client-cert` header value.
 *
 * Handles comma-separated elements (respecting quoted values),
 * semicolon-separated key=value pairs within each element, and
 * URL-encoded Cert/URI/By fields.
 */
export function parseXfcc(headerValue: string): XfccElement[] {
  const elements: XfccElement[] = [];
  for (const rawElement of splitRespectingQuotes(headerValue, ",")) {
    const trimmed = rawElement.trim();
    if (!trimmed) continue;
    const pairs = splitRespectingQuotes(trimmed, ";");
    const fields: Record<string, string | string[]> = {};
    for (const pair of pairs) {
      const p = pair.trim();
      if (!p) continue;
      const eqIdx = p.indexOf("=");
      if (eqIdx < 0) continue;
      const key = p.slice(0, eqIdx).trim().toLowerCase();
      let value = p.slice(eqIdx + 1).trim();
      if (value.length >= 2 && value[0] === '"' && value[value.length - 1] === '"') {
        value = unescapeQuoted(value.slice(1, -1));
      }
      if (key === "cert" || key === "uri" || key === "by") {
        value = decodeURIComponent(value);
      }
      if (key === "dns") {
        const existing = fields.dns;
        if (Array.isArray(existing)) {
          existing.push(value);
        } else {
          fields.dns = [value];
        }
      } else {
        fields[key] = value;
      }
    }
    const dns = Array.isArray(fields.dns) ? fields.dns : [];
    elements.push({
      hash: typeof fields.hash === "string" ? fields.hash : null,
      cert: typeof fields.cert === "string" ? fields.cert : null,
      subject: typeof fields.subject === "string" ? fields.subject : null,
      uri: typeof fields.uri === "string" ? fields.uri : null,
      dns,
      by: typeof fields.by === "string" ? fields.by : null,
    });
  }
  return elements;
}

/**
 * Create an authenticate callback from Envoy `x-forwarded-client-cert`.
 *
 * Parses the `x-forwarded-client-cert` header and extracts client identity.
 * Does not require any crypto dependencies.
 *
 * **Warning:** The reverse proxy MUST strip client-supplied
 * `x-forwarded-client-cert` headers before forwarding.
 */
export function mtlsAuthenticateXfcc(options?: {
  validate?: XfccValidateFn;
  domain?: string;
  selectElement?: "first" | "last";
}): AuthenticateFn {
  const validate = options?.validate;
  const domain = options?.domain ?? "mtls";
  const selectElement = options?.selectElement ?? "first";

  return async function authenticate(request: Request): Promise<AuthContext> {
    const headerValue = request.headers.get("x-forwarded-client-cert");
    if (!headerValue) {
      throw new Error("Missing x-forwarded-client-cert header");
    }
    const elements = parseXfcc(headerValue);
    if (elements.length === 0) {
      throw new Error("Empty x-forwarded-client-cert header");
    }
    const element = selectElement === "first" ? elements[0] : elements[elements.length - 1];
    if (validate) {
      return validate(element);
    }
    const principal = element.subject ? extractCn(element.subject) : "";
    const claims: Record<string, any> = {};
    if (element.hash) claims.hash = element.hash;
    if (element.subject) claims.subject = element.subject;
    if (element.uri) claims.uri = element.uri;
    if (element.dns.length > 0) claims.dns = [...element.dns];
    if (element.by) claims.by = element.by;
    return new AuthContext(domain, true, principal, claims);
  };
}

// ---------------------------------------------------------------------------
// PEM-based factories (uses node:crypto X509Certificate)
// ---------------------------------------------------------------------------

function parseCertFromHeader(request: Request, header: string): X509Certificate {
  const raw = request.headers.get(header);
  if (!raw) {
    throw new Error(`Missing ${header} header`);
  }
  const pemStr = decodeURIComponent(raw);
  if (!pemStr.startsWith("-----BEGIN CERTIFICATE-----")) {
    throw new Error("Header value is not a PEM certificate");
  }
  try {
    return new X509Certificate(pemStr);
  } catch (exc) {
    throw new Error(`Failed to parse PEM certificate: ${exc}`);
  }
}

function checkCertExpiry(cert: X509Certificate): void {
  const now = new Date();
  const notBefore = new Date(cert.validFrom);
  const notAfter = new Date(cert.validTo);
  if (now < notBefore) {
    throw new Error("Certificate is not yet valid");
  }
  if (now > notAfter) {
    throw new Error("Certificate has expired");
  }
}

/**
 * Create an mTLS authenticate callback with custom certificate validation.
 *
 * Generic factory that parses the client certificate from a proxy header
 * and delegates identity extraction to a user-supplied `validate` callback.
 *
 * **Warning:** The reverse proxy MUST strip client-supplied certificate
 * headers before forwarding.
 */
export function mtlsAuthenticate(options: {
  validate: CertValidateFn;
  header?: string;
  checkExpiry?: boolean;
}): AuthenticateFn {
  const { validate, header = "X-SSL-Client-Cert", checkExpiry = false } = options;

  return async function authenticate(request: Request): Promise<AuthContext> {
    const cert = parseCertFromHeader(request, header);
    if (checkExpiry) {
      checkCertExpiry(cert);
    }
    return validate(cert);
  };
}

const SUPPORTED_ALGORITHMS = new Set(["sha256", "sha1", "sha384", "sha512"]);

/**
 * Create an mTLS authenticate callback using certificate fingerprint lookup.
 *
 * Computes the certificate fingerprint and looks it up in the provided
 * mapping. Fingerprints must be lowercase hex without colons.
 */
export function mtlsAuthenticateFingerprint(options: {
  fingerprints: ReadonlyMap<string, AuthContext> | Record<string, AuthContext>;
  header?: string;
  algorithm?: string;
  domain?: string;
  checkExpiry?: boolean;
}): AuthenticateFn {
  const { fingerprints, header, algorithm = "sha256", checkExpiry } = options;
  if (!SUPPORTED_ALGORITHMS.has(algorithm)) {
    throw new Error(`Unsupported hash algorithm: ${algorithm}`);
  }
  const entries: ReadonlyMap<string, AuthContext> =
    fingerprints instanceof Map ? fingerprints : new Map(Object.entries(fingerprints));

  function validate(cert: X509Certificate): AuthContext {
    const fp = createHash(algorithm).update(cert.raw).digest("hex");
    const ctx = entries.get(fp);
    if (!ctx) {
      throw new Error(`Unknown certificate fingerprint: ${fp}`);
    }
    return ctx;
  }

  return mtlsAuthenticate({ validate, header, checkExpiry });
}

/**
 * Create an mTLS authenticate callback using certificate subject CN.
 *
 * Extracts the Subject Common Name as `principal` and populates
 * `claims` with the full DN, serial number (hex), and `not_valid_after`.
 */
export function mtlsAuthenticateSubject(options?: {
  header?: string;
  domain?: string;
  allowedSubjects?: ReadonlySet<string> | null;
  checkExpiry?: boolean;
}): AuthenticateFn {
  const { header, domain = "mtls", allowedSubjects = null, checkExpiry } = options ?? {};

  function validate(cert: X509Certificate): AuthContext {
    // Node's cert.subject is \n-separated "KEY=value" lines
    const subjectParts = cert.subject
      .split("\n")
      .map((s) => s.trim())
      .filter(Boolean);
    const subjectDn = subjectParts.join(", ");

    let cn = "";
    for (const part of subjectParts) {
      if (part.toUpperCase().startsWith("CN=")) {
        cn = part.slice(3);
        break;
      }
    }

    if (allowedSubjects !== null && !allowedSubjects.has(cn)) {
      throw new Error(`Subject CN '${cn}' not in allowed subjects`);
    }

    const serialHex = BigInt(`0x${cert.serialNumber}`).toString(16);
    const notValidAfter = new Date(cert.validTo).toISOString();

    return new AuthContext(domain, true, cn, {
      subject_dn: subjectDn,
      serial: serialHex,
      not_valid_after: notValidAfter,
    });
  }

  return mtlsAuthenticate({ validate, header, checkExpiry });
}
