// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

export { httpConnect, type RpcClient } from "./connect.js";
export { httpIntrospect, type MethodInfo, parseDescribeResponse, type ServiceDescription } from "./introspect.js";
export { PipeStreamSession, pipeConnect, subprocessConnect } from "./pipe.js";
export { HttpStreamSession } from "./stream.js";
export type {
  HttpConnectOptions,
  LogMessage,
  PipeConnectOptions,
  StreamSession,
  SubprocessConnectOptions,
} from "./types.js";
