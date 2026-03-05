// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import type { AuthContext } from "../auth.js";

/** Async function that authenticates an incoming HTTP request. */
export type AuthenticateFn = (request: Request) => AuthContext | Promise<AuthContext>;

/** RFC 9728 OAuth Protected Resource Metadata. */
export interface OAuthResourceMetadata {
  resource: string;
  authorizationServers: string[];
  scopesSupported?: string[];
  bearerMethodsSupported?: string[];
  resourceName?: string;
  resourceDocumentation?: string;
  resourcePolicyUri?: string;
  resourceTosUri?: string;
}

/** Convert OAuthResourceMetadata to RFC 9728 snake_case JSON object. */
export function oauthResourceMetadataToJson(metadata: OAuthResourceMetadata): Record<string, any> {
  const json: Record<string, any> = {
    resource: metadata.resource,
    authorization_servers: metadata.authorizationServers,
  };
  if (metadata.scopesSupported) json.scopes_supported = metadata.scopesSupported;
  if (metadata.bearerMethodsSupported) json.bearer_methods_supported = metadata.bearerMethodsSupported;
  if (metadata.resourceName) json.resource_name = metadata.resourceName;
  if (metadata.resourceDocumentation) json.resource_documentation = metadata.resourceDocumentation;
  if (metadata.resourcePolicyUri) json.resource_policy_uri = metadata.resourcePolicyUri;
  if (metadata.resourceTosUri) json.resource_tos_uri = metadata.resourceTosUri;
  return json;
}

/** Compute the well-known path for OAuth Protected Resource Metadata. */
export function wellKnownPath(prefix: string): string {
  return `/.well-known/oauth-protected-resource${prefix}`;
}

/** Build a WWW-Authenticate header value with optional resource_metadata URL. */
export function buildWwwAuthenticateHeader(metadataUrl?: string): string {
  if (metadataUrl) {
    return `Bearer resource_metadata="${metadataUrl}"`;
  }
  return "Bearer";
}
