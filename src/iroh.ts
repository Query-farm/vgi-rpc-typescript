// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/** Trusted forwarding of bridge-verified Iroh EndpointIds. */

import {
  IdentityAssurance,
  PeerIdentity,
  type PeerIdentityProvider,
  PeerIdentityResult,
  PeerIdentityStatus,
  PeerSubjectKind,
  SubjectStability,
} from "./identity.js";
import { normalizeIpLiteral, normalizeTrustedProxyAddresses } from "./ip.js";

const PROVIDER = "iroh";
const CANONICAL_ENDPOINT = /^[0-9a-f]{64}$/u;

/** Sanitized header set by the trusted Iroh HTTP bridge. */
export const IROH_FORWARDED_ENDPOINT_HEADER = "VGI-Forwarded-Iroh-Endpoint";

/** Trust boundary for a bridge-forwarded Iroh EndpointId. */
export interface IrohForwardedHeaderOptions {
  /** Operator-controlled identity namespace. */
  readonly issuer: string;
  /** Exact bridge IP addresses; no CIDRs, hostnames, or implicit loopback. */
  readonly trustedProxyAddresses: Iterable<string>;
}

/** @internal Validate the operator-local namespace shared by HTTP and TCP adapters. */
export function validateIrohIssuer(issuer: string): void {
  if (typeof issuer !== "string" || !issuer) {
    throw new TypeError("Iroh issuer must be non-empty text without controls");
  }
  for (let index = 0; index < issuer.length; index++) {
    const unit = issuer.charCodeAt(index);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const next = issuer.charCodeAt(index + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) throw new TypeError("Iroh issuer contains an unpaired surrogate");
      index++;
    } else if (unit >= 0xdc00 && unit <= 0xdfff) {
      throw new TypeError("Iroh issuer contains an unpaired surrogate");
    }
  }
  if (
    Array.from(issuer).some((character) => {
      const code = character.codePointAt(0) as number;
      return code <= 0x1f || code === 0x7f;
    })
  ) {
    throw new TypeError("Iroh issuer must be non-empty text without controls");
  }
}

/** Resolve a sanitized Iroh EndpointId only from an exact trusted HTTP proxy. */
export function irohForwardedHeaderIdentityProvider(options: IrohForwardedHeaderOptions): PeerIdentityProvider {
  validateIrohIssuer(options.issuer);
  const trusted = normalizeTrustedProxyAddresses(options.trustedProxyAddresses, "Iroh trustedProxyAddresses");
  const result = (status: PeerIdentityStatus, identity?: PeerIdentity) =>
    new PeerIdentityResult(PROVIDER, status, identity ? [identity] : []);
  return {
    provider: PROVIDER,
    resolve(context) {
      const immediate = context ? normalizeIpLiteral(context.immediatePeer ?? "") : null;
      if (!immediate || !trusted.has(immediate)) return result(PeerIdentityStatus.UNTRUSTED_PROXY);
      try {
        const endpointId = context.header(IROH_FORWARDED_ENDPOINT_HEADER);
        if (endpointId === undefined) return result(PeerIdentityStatus.NO_MATCH);
        if (!CANONICAL_ENDPOINT.test(endpointId)) return result(PeerIdentityStatus.INVALID);
        return result(
          PeerIdentityStatus.AVAILABLE,
          new PeerIdentity({
            provider: PROVIDER,
            evidenceSource: "http_proxy",
            assurance: IdentityAssurance.CONFIGURED_PROXY,
            issuer: options.issuer,
            transport: "http",
            subjectKind: PeerSubjectKind.ENDPOINT,
            subjectKey: endpointId,
            subjectStability: SubjectStability.STABLE,
            subjectVerified: true,
            attributes: {
              original_assurance: IdentityAssurance.CRYPTOGRAPHIC_PEER,
            },
            sourceAddress: endpointId,
            proxyAddress: immediate,
          }),
        );
      } catch {
        return result(PeerIdentityStatus.INVALID);
      }
    },
  };
}
