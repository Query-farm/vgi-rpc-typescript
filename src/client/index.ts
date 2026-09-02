// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

export {
  discoverHttpCapabilities,
  type HttpServerCapabilities,
  isCapabilitySnapshotFresh,
  parseCapabilitiesFromHeaders,
} from "./capabilities.js";
export { type HttpRpcClient, httpConnect, type RpcClient } from "./connect.js";
export { httpIntrospect, type MethodInfo, parseDescribeResponse, type ServiceDescription } from "./introspect.js";
export type { OAuthResourceMetadataResponse } from "./oauth.js";
export {
  fetchOAuthMetadata,
  httpOAuthMetadata,
  parseClientId,
  parseClientSecret,
  parseDeviceCodeClientId,
  parseDeviceCodeClientSecret,
  parseResourceMetadataUrl,
  parseUseIdTokenAsBearer,
} from "./oauth.js";
export { PipeStreamSession, pipeConnect, subprocessConnect } from "./pipe.js";
export { HttpStreamSession, type RowsWithToken } from "./stream.js";
export type {
  ExchangeInput,
  HttpConnectOptions,
  LogMessage,
  PipeConnectOptions,
  Socks5hHttpConnectOptions,
  Socks5hTcpConnectOptions,
  StreamSession,
  SubprocessConnectOptions,
  TcpConnectOptions,
} from "./types.js";
