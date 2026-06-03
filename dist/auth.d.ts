/** Authentication context available to RPC handlers. */
export declare class AuthContext {
    /** Authentication domain/realm that vouched for the principal; empty string
     *  when anonymous. */
    readonly domain: string;
    /** True when the request carried valid credentials. */
    readonly authenticated: boolean;
    /** Authenticated principal identifier, or `null` when anonymous. */
    readonly principal: string | null;
    /** Arbitrary verified claims about the principal (e.g. decoded JWT claims). */
    readonly claims: Record<string, any>;
    constructor(domain: string, authenticated: boolean, principal: string | null, claims?: Record<string, any>);
    /** Create an unauthenticated (anonymous) context. */
    static anonymous(): AuthContext;
    /** Throw an RpcError if this context is not authenticated. */
    requireAuthenticated(): void;
}
//# sourceMappingURL=auth.d.ts.map