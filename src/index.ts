// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

export { VgiRpcServer } from "./server.js";
export { Protocol } from "./protocol.js";
export {
  MethodType,
  OutputCollector,
  type LogContext,
  type MethodDefinition,
  type UnaryHandler,
  type HeaderInit,
  type ProducerInit,
  type ProducerFn,
  type ExchangeInit,
  type ExchangeFn,
} from "./types.js";
export {
  type SchemaLike,
  toSchema,
  inferParamTypes,
  str,
  bytes,
  int,
  int32,
  float,
  float32,
  bool,
} from "./schema.js";
export { RpcError, VersionError } from "./errors.js";
export {
  createHttpHandler,
  ARROW_CONTENT_TYPE,
  type HttpHandlerOptions,
  type StateSerializer,
} from "./http/index.js";
export {
  RPC_METHOD_KEY,
  REQUEST_VERSION_KEY,
  REQUEST_VERSION,
  LOG_LEVEL_KEY,
  LOG_MESSAGE_KEY,
  LOG_EXTRA_KEY,
  SERVER_ID_KEY,
  REQUEST_ID_KEY,
  PROTOCOL_NAME_KEY,
  DESCRIBE_VERSION_KEY,
  DESCRIBE_VERSION,
  DESCRIBE_METHOD_NAME,
  STATE_KEY,
} from "./constants.js";
