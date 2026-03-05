// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { RpcError } from "./errors.js";

/** Authentication context available to RPC handlers. */
export class AuthContext {
  readonly domain: string;
  readonly authenticated: boolean;
  readonly principal: string | null;
  readonly claims: Record<string, any>;

  constructor(domain: string, authenticated: boolean, principal: string | null, claims: Record<string, any> = {}) {
    this.domain = domain;
    this.authenticated = authenticated;
    this.principal = principal;
    this.claims = claims;
  }

  /** Create an unauthenticated (anonymous) context. */
  static anonymous(): AuthContext {
    return new AuthContext("", false, null);
  }

  /** Throw an RpcError if this context is not authenticated. */
  requireAuthenticated(): void {
    if (!this.authenticated) {
      throw new RpcError("AuthenticationError", "Authentication required", "");
    }
  }
}
