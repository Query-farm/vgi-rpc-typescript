/** RFC 9728 OAuth Protected Resource Metadata (client-side response). */
export interface OAuthResourceMetadataResponse {
    /** The protected resource's canonical URL (`resource`). */
    resource: string;
    /** Authorization-server issuer URLs; the first is used for OIDC discovery (`authorization_servers`). */
    authorizationServers: string[];
    /** Scopes the resource advertises (`scopes_supported`). */
    scopesSupported?: string[];
    /** Advertised bearer methods, e.g. `["header"]` (`bearer_methods_supported`). */
    bearerMethodsSupported?: string[];
    /** JWS algorithms the resource accepts (`resource_signing_alg_values_supported`). */
    resourceSigningAlgValuesSupported?: string[];
    /** Human-readable resource name (`resource_name`). */
    resourceName?: string;
    /** Documentation URL for the resource (`resource_documentation`). */
    resourceDocumentation?: string;
    /** Policy URL for the resource (`resource_policy_uri`). */
    resourcePolicyUri?: string;
    /** Terms-of-service URL for the resource (`resource_tos_uri`). */
    resourceTosUri?: string;
    /** OAuth client_id advertised by the server. */
    clientId?: string;
    /** OAuth client_secret advertised by the server. */
    clientSecret?: string;
    /** When true, use the OIDC id_token as the Bearer token instead of access_token. */
    useIdTokenAsBearer?: boolean;
    /** OAuth client_id for device code flow. */
    deviceCodeClientId?: string;
    /** OAuth client_secret for device code flow. */
    deviceCodeClientSecret?: string;
}
/**
 * Discover OAuth Protected Resource Metadata (RFC 9728) from a vgi-rpc server.
 * Returns `null` if the server does not serve the well-known endpoint.
 */
export declare function httpOAuthMetadata(baseUrl: string, prefix?: string): Promise<OAuthResourceMetadataResponse | null>;
/**
 * Fetch OAuth Protected Resource Metadata from an explicit metadata URL.
 */
export declare function fetchOAuthMetadata(metadataUrl: string): Promise<OAuthResourceMetadataResponse>;
/**
 * Extract the `resource_metadata` URL from a WWW-Authenticate Bearer challenge.
 * Returns `null` if no resource_metadata parameter is found.
 */
export declare function parseResourceMetadataUrl(wwwAuthenticate: string): string | null;
/**
 * Extract the `client_id` from a WWW-Authenticate Bearer challenge.
 * Returns `null` if no client_id parameter is found.
 */
export declare function parseClientId(wwwAuthenticate: string): string | null;
/**
 * Extract the `client_secret` from a WWW-Authenticate Bearer challenge.
 * Returns `null` if no client_secret parameter is found.
 */
export declare function parseClientSecret(wwwAuthenticate: string): string | null;
/**
 * Extract the `use_id_token_as_bearer` flag from a WWW-Authenticate Bearer challenge.
 * Returns `true` if the parameter is present and set to "true", `false` otherwise.
 */
export declare function parseUseIdTokenAsBearer(wwwAuthenticate: string): boolean;
/**
 * Extract the `device_code_client_id` from a WWW-Authenticate Bearer challenge.
 * Returns `null` if no device_code_client_id parameter is found.
 */
export declare function parseDeviceCodeClientId(wwwAuthenticate: string): string | null;
/**
 * Extract the `device_code_client_secret` from a WWW-Authenticate Bearer challenge.
 * Returns `null` if no device_code_client_secret parameter is found.
 */
export declare function parseDeviceCodeClientSecret(wwwAuthenticate: string): string | null;
//# sourceMappingURL=oauth.d.ts.map