// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Cross-port conformance fixtures: `@query-farm/vgi-rpc/conformance`.
 *
 * What a conformance worker -- this port's, or a VGI SDK's fixture worker --
 * hosts so the reference's shared suite can assert against it:
 * `conformance.Secondary.v1` and the `vgi_rpc.Identity.v1` fixture policy.
 * Not for production servers: the identity fixture's authenticator trusts
 * request headers.
 */

export * from "./identity-fixture.js";
export * from "./secondary.js";
