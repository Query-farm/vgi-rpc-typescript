// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Turn a launcher's `Protocol | VgiRpcServer` argument into the protocol host
 * it serves.
 *
 * A bare `Protocol` keeps the old single-protocol shape: the launcher builds
 * the host from its own server-level options. A `VgiRpcServer` is the caller's
 * already-built host -- carrying every additional protocol and identity -- so
 * the same server object, and therefore the same hosted set, reaches every
 * transport (WIRE_PROTOCOL.md §3.1). Server-level options alongside a built
 * host are refused rather than silently ignored: they belong on the server.
 */

import type { ExternalLocationConfig } from "../external.js";
import type { Protocol } from "../protocol.js";
import { VgiRpcServer } from "../server.js";
import type { DispatchHook, ServeStartHook } from "../types.js";

/** The server-level options every raw launcher accepts for a bare `Protocol`. */
export interface LauncherServerOptions {
  protocolVersion?: string;
  serverId?: string;
  enableDescribe?: boolean;
  dispatchHook?: DispatchHook;
  externalLocation?: ExternalLocationConfig;
  onServeStart?: ServeStartHook;
}

const SERVER_LEVEL_KEYS = [
  "protocolVersion",
  "serverId",
  "enableDescribe",
  "dispatchHook",
  "externalLocation",
  "onServeStart",
] as const satisfies readonly (keyof LauncherServerOptions)[];

/** Resolve the host a launcher serves. */
export function launcherHost(
  target: Protocol | VgiRpcServer,
  options: LauncherServerOptions,
  launcher: string,
): VgiRpcServer {
  if (target instanceof VgiRpcServer) {
    const given = SERVER_LEVEL_KEYS.filter((key) => options[key] !== undefined);
    if (given.length > 0) {
      throw new TypeError(
        `${launcher} was given a VgiRpcServer together with server-level options [${given.join(", ")}]. ` +
          "Pass them to the VgiRpcServer constructor instead, so every transport serving it agrees.",
      );
    }
    return target;
  }
  return new VgiRpcServer(target, {
    serverId: options.serverId,
    protocolVersion: options.protocolVersion,
    enableDescribe: options.enableDescribe ?? true,
    dispatchHook: options.dispatchHook,
    externalLocation: options.externalLocation,
    onServeStart: options.onServeStart,
  });
}
