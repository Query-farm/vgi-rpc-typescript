// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { observePeerIdentity, peerIdentityPrimary } from "../identity.js";
import { validateIrohIssuer } from "../iroh.js";
import type { Protocol } from "../protocol.js";
import { type ServeTcpHandle, type ServeTcpOptions, serveTcp } from "./serve-tcp.js";

/** Options for the raw loopback upstream consumed by {@code vgi-iroh-bridge}. */
export interface ServeIrohTcpUpstreamOptions
  extends Omit<
    ServeTcpOptions,
    | "host"
    | "peerIdentityProviders"
    | "peerAuthenticationPolicy"
    | "proxyProtocolV2Required"
    | "trustedProxyAddresses"
    | "irohProxyIssuer"
  > {
  /** Operator-controlled identity namespace. */
  readonly issuer: string;
  /** Loopback bind address. Defaults to 127.0.0.1. */
  readonly host?: "127.0.0.1" | "::1" | "localhost";
  /** Exact immediate bridge addresses. Defaults to IPv4 loopback. */
  readonly trustedProxyAddresses?: readonly string[];
  /** Promote the verified EndpointId to AuthContext. Defaults to true. */
  readonly authenticate?: boolean;
}

/**
 * Start a bridge-ready raw VGI worker upstream with the required PROXY-v2
 * EndpointId evidence and a loopback-only listener.
 */
export function serveIrohTcpUpstream(
  protocol: Protocol,
  options: ServeIrohTcpUpstreamOptions,
): Promise<ServeTcpHandle> {
  validateIrohIssuer(options.issuer);
  const {
    issuer,
    authenticate = true,
    trustedProxyAddresses = ["127.0.0.1"],
    host = "127.0.0.1",
    ...transport
  } = options;
  return serveTcp(protocol, {
    ...transport,
    host,
    proxyProtocolV2Required: true,
    trustedProxyAddresses,
    irohProxyIssuer: issuer,
    peerAuthenticationPolicy: authenticate ? peerIdentityPrimary("iroh") : observePeerIdentity,
  });
}
