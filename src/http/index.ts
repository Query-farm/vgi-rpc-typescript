// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

export type { AuthenticateFn, OAuthResourceMetadata } from "./auth.js";
export { oauthResourceMetadataToJson } from "./auth.js";
export type { BearerValidateFn } from "./bearer.js";
export { bearerAuthenticate, bearerAuthenticateStatic, chainAuthenticate } from "./bearer.js";
export {
  ARROW_CONTENT_TYPE,
  decodeContentEncoding,
  MAX_UPLOAD_URL_COUNT,
  UPLOAD_URL_METHOD,
  UPLOAD_URL_PARAMS_SCHEMA,
  UPLOAD_URL_RESPONSE_SCHEMA,
} from "./common.js";
export { createHttpHandler } from "./handler.js";
export type { Introspector, TokenIdentity, TokenResolver } from "./introspect.js";
export {
  createIntrospector,
  DEFAULT_INTROSPECT_TTL_SECONDS,
  INTROSPECT_ENABLED_HEADER,
  INTROSPECT_ENDPOINT,
  tokenDigest,
} from "./introspect.js";
export type { JwtAuthenticateOptions } from "./jwt.js";
export { jwtAuthenticate } from "./jwt.js";
export type { CertValidateFn, XfccElement, XfccValidateFn } from "./mtls.js";
export {
  mtlsAuthenticate,
  mtlsAuthenticateFingerprint,
  mtlsAuthenticateSubject,
  mtlsAuthenticateXfcc,
  parseXfcc,
} from "./mtls.js";
export { cookieAuthenticate } from "./oauth-pkce.js";
export type { ProofConfig, ProofMode, ProofSecret } from "./proof.js";
export {
  canonicalString,
  deriveProofSecret,
  mintProof,
  NonceCache,
  PROOF_HEADER,
  PROOF_REQUIRED_HEADER,
  ProofError,
  parseProofSecrets,
  requireProxyProof,
  verifyProof,
} from "./proof.js";
export { type UnpackedToken, unpackStateToken } from "./token.js";
export type { HttpHandlerOptions, LandingInfo, StateSerializer } from "./types.js";
export { jsonStateSerializer } from "./types.js";
export {
  AUTH_PROXY_REQUIRED_HEADER,
  AUTH_REASON_HEADER,
  AuthFailure,
  AuthReason,
  AuthUnavailableError,
  buildProxyHint,
  classifyAuthFailure,
  unauthorizedEnvelope,
} from "./unauthorized.js";
