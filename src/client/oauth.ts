// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/** RFC 9728 OAuth Protected Resource Metadata (client-side response). */
export interface OAuthResourceMetadataResponse {
  resource: string;
  authorizationServers: string[];
  scopesSupported?: string[];
  bearerMethodsSupported?: string[];
  resourceName?: string;
  resourceDocumentation?: string;
  resourcePolicyUri?: string;
  resourceTosUri?: string;
  /** OAuth client_id advertised by the server. */
  clientId?: string;
  /** OAuth client_secret advertised by the server. */
  clientSecret?: string;
  /** When true, use the OIDC id_token as the Bearer token instead of access_token. */
  useIdTokenAsBearer?: boolean;
}

function parseMetadataJson(json: Record<string, any>): OAuthResourceMetadataResponse {
  const result: OAuthResourceMetadataResponse = {
    resource: json.resource,
    authorizationServers: json.authorization_servers,
  };
  if (json.scopes_supported) result.scopesSupported = json.scopes_supported;
  if (json.bearer_methods_supported) result.bearerMethodsSupported = json.bearer_methods_supported;
  if (json.resource_name) result.resourceName = json.resource_name;
  if (json.resource_documentation) result.resourceDocumentation = json.resource_documentation;
  if (json.resource_policy_uri) result.resourcePolicyUri = json.resource_policy_uri;
  if (json.resource_tos_uri) result.resourceTosUri = json.resource_tos_uri;
  if (json.client_id) result.clientId = json.client_id;
  if (json.client_secret) result.clientSecret = json.client_secret;
  if (json.use_id_token_as_bearer) result.useIdTokenAsBearer = json.use_id_token_as_bearer;
  return result;
}

/**
 * Discover OAuth Protected Resource Metadata (RFC 9728) from a vgi-rpc server.
 * Returns `null` if the server does not serve the well-known endpoint.
 */
export async function httpOAuthMetadata(
  baseUrl: string,
  prefix?: string,
): Promise<OAuthResourceMetadataResponse | null> {
  const effectivePrefix = (prefix ?? "/vgi").replace(/\/+$/, "");
  const metadataUrl = `${baseUrl.replace(/\/+$/, "")}/.well-known/oauth-protected-resource${effectivePrefix}`;

  try {
    return await fetchOAuthMetadata(metadataUrl);
  } catch {
    return null;
  }
}

/**
 * Fetch OAuth Protected Resource Metadata from an explicit metadata URL.
 */
export async function fetchOAuthMetadata(metadataUrl: string): Promise<OAuthResourceMetadataResponse> {
  const response = await fetch(metadataUrl);
  if (!response.ok) {
    throw new Error(`Failed to fetch OAuth metadata from ${metadataUrl}: ${response.status}`);
  }
  const json = await response.json();
  return parseMetadataJson(json);
}

/**
 * Extract the `resource_metadata` URL from a WWW-Authenticate Bearer challenge.
 * Returns `null` if no resource_metadata parameter is found.
 */
export function parseResourceMetadataUrl(wwwAuthenticate: string): string | null {
  // Parse Bearer challenge parameters per RFC 6750
  const bearerMatch = wwwAuthenticate.match(/^Bearer\s+(.*)/i);
  if (!bearerMatch) return null;

  const params = bearerMatch[1];
  const metadataMatch = params.match(/resource_metadata="([^"]+)"/);
  if (!metadataMatch) return null;

  return metadataMatch[1];
}

/**
 * Extract the `client_id` from a WWW-Authenticate Bearer challenge.
 * Returns `null` if no client_id parameter is found.
 */
export function parseClientId(wwwAuthenticate: string): string | null {
  const bearerMatch = wwwAuthenticate.match(/^Bearer\s+(.*)/i);
  if (!bearerMatch) return null;

  const params = bearerMatch[1];
  const clientIdMatch = params.match(/client_id="([^"]+)"/);
  if (!clientIdMatch) return null;

  return clientIdMatch[1];
}

/**
 * Extract the `client_secret` from a WWW-Authenticate Bearer challenge.
 * Returns `null` if no client_secret parameter is found.
 */
export function parseClientSecret(wwwAuthenticate: string): string | null {
  const bearerMatch = wwwAuthenticate.match(/^Bearer\s+(.*)/i);
  if (!bearerMatch) return null;

  const params = bearerMatch[1];
  const match = params.match(/client_secret="([^"]+)"/);
  if (!match) return null;

  return match[1];
}

/**
 * Extract the `use_id_token_as_bearer` flag from a WWW-Authenticate Bearer challenge.
 * Returns `true` if the parameter is present and set to "true", `false` otherwise.
 */
export function parseUseIdTokenAsBearer(wwwAuthenticate: string): boolean {
  const bearerMatch = wwwAuthenticate.match(/^Bearer\s+(.*)/i);
  if (!bearerMatch) return false;

  const params = bearerMatch[1];
  const match = params.match(/use_id_token_as_bearer="([^"]+)"/);
  if (!match) return false;

  return match[1] === "true";
}
