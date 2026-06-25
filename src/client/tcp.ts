// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Raw-TCP RPC client.
 *
 * Node/Bun only — statically imports `node:net`, so this module is exported
 * exclusively from the node barrel (`src/index.ts`), never from the
 * runtime-agnostic core (which is re-exported into browser/workerd bundles).
 *
 * SECURITY: raw TCP carries no authentication or TLS — connect only to
 * trusted endpoints. Use `httpConnect` for untrusted networks.
 */

import { connect } from "node:net";
import type { RpcClient } from "./connect.js";
import { pipeConnect } from "./pipe.js";
import type { TcpConnectOptions } from "./types.js";

/**
 * Connect to a vgi-rpc server over a raw TCP socket and wrap it with
 * {@link pipeConnect}. The network analog of `subprocessConnect`: identical
 * lockstep raw Arrow-IPC framing, only the transport differs. Nagle's
 * algorithm is disabled (`setNoDelay(true)`) so lockstep requests are not
 * delayed coalescing writes. The returned client's {@link RpcClient.close}
 * also destroys the socket.
 *
 * SECURITY: raw TCP carries **no authentication or TLS** — only connect to
 * trusted endpoints. Use `httpConnect` for untrusted networks.
 *
 * @param host Hostname or IP address of the TCP server.
 * @param port TCP port of the server.
 * @param options Optional log/external-location configuration.
 */
export function tcpConnect(host: string, port: number, options?: TcpConnectOptions): RpcClient {
  const socket = connect({ host, port });
  socket.setNoDelay(true);
  // Swallow socket errors so an EPIPE/ECONNRESET doesn't crash the process;
  // the in-flight call surfaces the failure as an EOF on the reader instead.
  socket.on("error", () => {});

  const readable = socket as unknown as ReadableStream<Uint8Array>;
  const writable = {
    write(data: Uint8Array): void {
      socket.write(data);
    },
    end(): void {
      socket.end();
    },
  };

  const client = pipeConnect(readable, writable, {
    onLog: options?.onLog,
    externalLocation: options?.externalLocation,
  });

  const originalClose = client.close;
  client.close = () => {
    originalClose.call(client);
    try {
      socket.destroy();
    } catch {
      // Socket may already be closed
    }
  };

  return client;
}
