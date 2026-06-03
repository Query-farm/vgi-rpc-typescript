import type { AuthContext } from "../auth.js";
/** Async function that authenticates an incoming HTTP request. */
export type AuthenticateFn = (request: Request) => AuthContext | Promise<AuthContext>;
/** RFC 9728 OAuth Protected Resource Metadata. */
export interface OAuthResourceMetadata {
    /** The protected resource's canonical URL. Doubles as the base for the
     *  `/_oauth/callback` redirect URI. */
    resource: string;
    /** Authorization-server issuer URLs. The PKCE flow uses
     *  `authorizationServers[0]` for OIDC discovery. */
    authorizationServers: string[];
    /** Scopes the resource advertises. When non-empty these become the PKCE
     *  authorization request's space-joined `scope`, taking precedence over
     *  {@link HttpHandlerOptions.oauthPkceScope}. */
    scopesSupported?: string[];
    /** Advertised bearer methods (e.g. `["header"]`). */
    bearerMethodsSupported?: string[];
    /** JWS algorithms the resource accepts. */
    resourceSigningAlgValuesSupported?: string[];
    /** Human-readable resource name. */
    resourceName?: string;
    /** Documentation URL for the resource. */
    resourceDocumentation?: string;
    /** Policy URL for the resource. */
    resourcePolicyUri?: string;
    /** Terms-of-service URL for the resource. */
    resourceTosUri?: string;
    /** OAuth client_id that clients should use with the authorization server. */
    clientId?: string;
    /** OAuth client_secret that clients should use with the authorization server. */
    clientSecret?: string;
    /** OAuth client_id for device code flow. */
    deviceCodeClientId?: string;
    /** OAuth client_secret for device code flow. */
    deviceCodeClientSecret?: string;
    /** When true, clients should use the OIDC id_token as the Bearer token instead of access_token. */
    useIdTokenAsBearer?: boolean;
}
/** Convert OAuthResourceMetadata to RFC 9728 snake_case JSON object. */
export declare function oauthResourceMetadataToJson(metadata: OAuthResourceMetadata): Record<string, any>;
/** Compute the well-known path for OAuth Protected Resource Metadata. */
export declare function wellKnownPath(prefix: string): string;
/** Build a WWW-Authenticate header value with optional resource_metadata URL, client_id, client_secret, device_code_client_id, device_code_client_secret, and use_id_token_as_bearer. */
export declare function buildWwwAuthenticateHeader(metadataUrl?: string, clientId?: string, clientSecret?: string, useIdTokenAsBearer?: boolean, deviceCodeClientId?: string, deviceCodeClientSecret?: string): string;
//# sourceMappingURL=auth.d.ts.map