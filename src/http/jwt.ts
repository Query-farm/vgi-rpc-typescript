// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import * as oauth from "oauth4webapi";
import { AuthContext } from "../auth.js";
import type { AuthenticateFn } from "./auth.js";

export interface JwtAuthenticateOptions {
  /** The expected `iss` claim (also used to discover AS metadata). */
  issuer: string;
  /** The expected `aud` claim. */
  audience: string;
  /** Explicit JWKS URI. If omitted, discovered from issuer metadata. */
  jwksUri?: string;
  /** JWT claim to use as the principal. Default: "sub". */
  principalClaim?: string;
  /** AuthContext domain. Default: "jwt". */
  domain?: string;
}

/**
 * Create an AuthenticateFn that validates JWT Bearer tokens using oauth4webapi.
 *
 * On first call, discovers the Authorization Server metadata from the issuer
 * to obtain the JWKS URI (unless `jwksUri` is provided directly).
 */
export function jwtAuthenticate(options: JwtAuthenticateOptions): AuthenticateFn {
  const principalClaim = options.principalClaim ?? "sub";
  const domain = options.domain ?? "jwt";
  const audience = options.audience;

  let asPromise: Promise<oauth.AuthorizationServer> | null = null;

  async function getAuthorizationServer(): Promise<oauth.AuthorizationServer> {
    if (options.jwksUri) {
      return {
        issuer: options.issuer as `https://${string}`,
        jwks_uri: options.jwksUri,
      };
    }
    const issuerUrl = new URL(options.issuer);
    const response = await oauth.discoveryRequest(issuerUrl);
    return oauth.processDiscoveryResponse(issuerUrl, response);
  }

  return async function authenticate(request: Request): Promise<AuthContext> {
    if (!asPromise) {
      asPromise = getAuthorizationServer();
    }

    let as: oauth.AuthorizationServer;
    try {
      as = await asPromise;
    } catch (error) {
      // Reset so next request retries discovery
      asPromise = null;
      throw error;
    }

    // validateJwtAccessToken throws on failure, returns claims on success
    const claims = await oauth.validateJwtAccessToken(as, request, audience);
    const principal = (claims[principalClaim] as string | undefined) ?? null;

    return new AuthContext(domain, true, principal, claims as unknown as Record<string, any>);
  };
}
