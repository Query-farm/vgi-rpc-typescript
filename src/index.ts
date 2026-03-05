// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

export { AuthContext } from "./auth.js";
export * from "./client/index.js";
export {
  DESCRIBE_METHOD_NAME,
  DESCRIBE_VERSION,
  DESCRIBE_VERSION_KEY,
  LOG_EXTRA_KEY,
  LOG_LEVEL_KEY,
  LOG_MESSAGE_KEY,
  PROTOCOL_NAME_KEY,
  REQUEST_ID_KEY,
  REQUEST_VERSION,
  REQUEST_VERSION_KEY,
  RPC_METHOD_KEY,
  SERVER_ID_KEY,
  STATE_KEY,
} from "./constants.js";
export { RpcError, VersionError } from "./errors.js";
export {
  ARROW_CONTENT_TYPE,
  type AuthenticateFn,
  bearerAuthenticate,
  bearerAuthenticateStatic,
  type BearerValidateFn,
  chainAuthenticate,
  createHttpHandler,
  type HttpHandlerOptions,
  type JwtAuthenticateOptions,
  jsonStateSerializer,
  jwtAuthenticate,
  type OAuthResourceMetadata,
  oauthResourceMetadataToJson,
  type StateSerializer,
  type UnpackedToken,
  unpackStateToken,
} from "./http/index.js";
export { Protocol } from "./protocol.js";
export {
  bool,
  bytes,
  float,
  float32,
  inferParamTypes,
  int,
  int32,
  type SchemaLike,
  str,
  toSchema,
} from "./schema.js";
export { VgiRpcServer } from "./server.js";
export {
  type CallContext,
  type ExchangeFn,
  type ExchangeInit,
  type HeaderInit,
  type LogContext,
  type MethodDefinition,
  MethodType,
  OutputCollector,
  type ProducerFn,
  type ProducerInit,
  type UnaryHandler,
} from "./types.js";
