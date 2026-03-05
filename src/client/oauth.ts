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
