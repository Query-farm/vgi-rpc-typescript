import { AuthContext } from "../auth.js";
import type { AuthenticateFn } from "./auth.js";
type X509Certificate = any;
/** A single element from an `x-forwarded-client-cert` header. */
export interface XfccElement {
    /** Hex SHA-256 digest of the client certificate (`Hash` key). */
    hash: string | null;
    /** URL-decoded PEM of the client certificate (`Cert` key), if the proxy
     *  forwarded it. */
    cert: string | null;
    /** Certificate Subject DN (`Subject` key). */
    subject: string | null;
    /** URL-decoded URI-type Subject Alternative Name (`URI` key). */
    uri: string | null;
    /** DNS-type Subject Alternative Names (`DNS` keys); may repeat in the header. */
    dns: readonly string[];
    /** URL-decoded URI of the proxy that presented the cert (`By` key). */
    by: string | null;
}
/** Receives a parsed XFCC element, returns an AuthContext on success. Must throw on failure. */
export type XfccValidateFn = (element: XfccElement) => AuthContext | Promise<AuthContext>;
/** Receives a parsed X509Certificate, returns an AuthContext on success. Must throw on failure. */
export type CertValidateFn = (cert: X509Certificate) => AuthContext | Promise<AuthContext>;
/**
 * Parse an `x-forwarded-client-cert` header value.
 *
 * Handles comma-separated elements (respecting quoted values),
 * semicolon-separated key=value pairs within each element, and
 * URL-encoded Cert/URI/By fields.
 */
export declare function parseXfcc(headerValue: string): XfccElement[];
/**
 * Create an authenticate callback from Envoy `x-forwarded-client-cert`.
 *
 * Parses the `x-forwarded-client-cert` header and extracts client identity.
 * Does not require any crypto dependencies.
 *
 * **Warning:** The reverse proxy MUST strip client-supplied
 * `x-forwarded-client-cert` headers before forwarding.
 */
export declare function mtlsAuthenticateXfcc(options?: {
    validate?: XfccValidateFn;
    domain?: string;
    selectElement?: "first" | "last";
}): AuthenticateFn;
/**
 * Create an mTLS authenticate callback with custom certificate validation.
 *
 * Generic factory that parses the client certificate from a proxy header
 * and delegates identity extraction to a user-supplied `validate` callback.
 *
 * **Warning:** The reverse proxy MUST strip client-supplied certificate
 * headers before forwarding.
 */
export declare function mtlsAuthenticate(options: {
    validate: CertValidateFn;
    header?: string;
    checkExpiry?: boolean;
}): AuthenticateFn;
/**
 * Create an mTLS authenticate callback using certificate fingerprint lookup.
 *
 * Computes the certificate fingerprint and looks it up in the provided
 * mapping. Fingerprints must be lowercase hex without colons.
 */
export declare function mtlsAuthenticateFingerprint(options: {
    fingerprints: ReadonlyMap<string, AuthContext> | Record<string, AuthContext>;
    header?: string;
    algorithm?: string;
    domain?: string;
    checkExpiry?: boolean;
}): AuthenticateFn;
/**
 * Create an mTLS authenticate callback using certificate subject CN.
 *
 * Extracts the Subject Common Name as `principal` and populates
 * `claims` with the full DN, serial number (hex), and `not_valid_after`.
 */
export declare function mtlsAuthenticateSubject(options?: {
    header?: string;
    domain?: string;
    allowedSubjects?: ReadonlySet<string> | null;
    checkExpiry?: boolean;
}): AuthenticateFn;
export {};
//# sourceMappingURL=mtls.d.ts.map