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
  /** OAuth client_id that clients should use with the authorization server. */
  clientId?: string;
  /** OAuth client_secret that clients should use with the authorization server. */
  clientSecret?: string;
  /** When true, clients should use the OIDC id_token as the Bearer token instead of access_token. */
  useIdTokenAsBearer?: boolean;
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
  if (metadata.clientId) {
    if (!/^[A-Za-z0-9\-._~]+$/.test(metadata.clientId)) {
      throw new Error(`Invalid client_id: must contain only URL-safe characters [A-Za-z0-9\\-._~]`);
    }
    json.client_id = metadata.clientId;
  }
  if (metadata.clientSecret) {
    if (!/^[A-Za-z0-9\-._~]+$/.test(metadata.clientSecret)) {
      throw new Error(`Invalid client_secret: must contain only URL-safe characters [A-Za-z0-9\\-._~]`);
    }
    json.client_secret = metadata.clientSecret;
  }
  if (metadata.useIdTokenAsBearer) {
    json.use_id_token_as_bearer = true;
  }
  return json;
}

/** Compute the well-known path for OAuth Protected Resource Metadata. */
export function wellKnownPath(prefix: string): string {
  return `/.well-known/oauth-protected-resource${prefix}`;
}

/** Build a WWW-Authenticate header value with optional resource_metadata URL, client_id, client_secret, and use_id_token_as_bearer. */
export function buildWwwAuthenticateHeader(
  metadataUrl?: string,
  clientId?: string,
  clientSecret?: string,
  useIdTokenAsBearer?: boolean,
): string {
  let header = "Bearer";
  if (metadataUrl) {
    header += ` resource_metadata="${metadataUrl}"`;
  }
  if (clientId) {
    header += `, client_id="${clientId}"`;
  }
  if (clientSecret) {
    header += `, client_secret="${clientSecret}"`;
  }
  if (useIdTokenAsBearer) {
    header += `, use_id_token_as_bearer="true"`;
  }
  return header;
}
