// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { randomBytes } from "node:crypto";
import type { RpcClient } from "./connect.js";
import { pipeConnect } from "./pipe.js";
import type { PipeConnectOptions } from "./types.js";

export const IROH_ARROW_MUX_ALPN = "vgi-rpc/arrow-mux/1";
export const IROH_HTTP_ALPN = "iroh-http/2";
const processEphemeralSecretKey = randomBytes(32);

export type IrohErrorStage =
  | "parse"
  | "bind"
  | "resolve"
  | "connect"
  | "alpn"
  | "open_stream"
  | "write"
  | "read"
  | "cancel"
  | "close"
  | "internal";
export type IrohErrorCategory =
  | "invalid_input"
  | "unsupported"
  | "unavailable"
  | "timeout"
  | "protocol"
  | "connection_reset"
  | "cancelled"
  | "authentication"
  | "resource_exhausted"
  | "internal";
export type IrohDispatchCertainty = "not_sent" | "unknown" | "sent";

/** Portable failure dimensions shared by every VGI Iroh transport. */
export class IrohTransportError extends Error {
  readonly stage: IrohErrorStage;
  readonly category: IrohErrorCategory;
  readonly dispatchCertainty: IrohDispatchCertainty;

  constructor(
    message: string,
    stage: IrohErrorStage,
    category: IrohErrorCategory,
    dispatchCertainty: IrohDispatchCertainty,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "IrohTransportError";
    this.stage = stage;
    this.category = category;
    this.dispatchCertainty = dispatchCertainty;
  }
}

export class IrohUriError extends IrohTransportError {
  constructor(message: string) {
    super(message, "parse", "invalid_input", "not_sent");
    this.name = "IrohUriError";
  }
}

function transportError(
  error: unknown,
  stage: IrohErrorStage,
  category: IrohErrorCategory,
  dispatchCertainty: IrohDispatchCertainty,
): IrohTransportError {
  if (error instanceof IrohTransportError) return error;
  const aborted = error instanceof DOMException && error.name === "AbortError";
  return new IrohTransportError(
    error instanceof Error ? error.message : String(error),
    aborted ? "cancel" : stage,
    aborted ? "cancelled" : category,
    dispatchCertainty,
    { cause: error },
  );
}

export interface IrohEndpoint {
  readonly scheme: "iroh" | "httpi";
  readonly endpointId: string;
  readonly endpointIdBytes: Uint8Array;
  readonly basePath: string;
  readonly alpn: typeof IROH_ARROW_MUX_ALPN | typeof IROH_HTTP_ALPN;
}

function decodeEndpointId(value: string): Uint8Array {
  const bytes = new Uint8Array(32);
  for (let i = 0; i < bytes.length; i++) bytes[i] = Number.parseInt(value.slice(i * 2, i * 2 + 2), 16);
  return bytes;
}

/** Parse the canonical VGI Iroh URI without URL-parser hostname normalization. */
export function parseIrohEndpoint(raw: string): IrohEndpoint {
  if (
    typeof raw !== "string" ||
    raw.length === 0 ||
    raw.includes("\\") ||
    raw.includes("?") ||
    raw.includes("#") ||
    [...raw].some((value) => value.charCodeAt(0) <= 0x20 || value.charCodeAt(0) === 0x7f)
  ) {
    throw new IrohUriError("invalid VGI Iroh endpoint URI");
  }
  const match = /^(iroh|httpi):\/\/([0-9a-f]{64})(\/.*)?$/.exec(raw);
  if (!match) throw new IrohUriError("Iroh endpoint ID must be exactly 64 lowercase hexadecimal characters");

  const scheme = match[1] as "iroh" | "httpi";
  const path = match[3] ?? "";
  if (scheme === "iroh" && path !== "") throw new IrohUriError("iroh:// endpoints cannot contain a path");
  if (path.length > 1 && path.endsWith("/")) {
    throw new IrohUriError("httpi:// base paths cannot have a trailing empty segment");
  }
  if (path.includes("//") || path.split("/").some((part) => part === "." || part === "..")) {
    throw new IrohUriError("httpi:// base paths must be canonical and cannot contain empty or dot segments");
  }
  for (let i = 0; i < path.length; i++) {
    if (path[i] === "%" && !/^[0-9A-Fa-f]{2}$/.test(path.slice(i + 1, i + 3))) {
      throw new IrohUriError("httpi:// base path contains an invalid percent escape");
    }
    if (path[i] === "%") {
      const decoded = Number.parseInt(path.slice(i + 1, i + 3), 16);
      if (decoded === 0x2e || decoded === 0x2f || decoded === 0x5c || decoded <= 0x20 || decoded === 0x7f) {
        throw new IrohUriError("httpi:// base path contains an encoded dot, separator, or control");
      }
      i += 2;
    }
  }

  return {
    scheme,
    endpointId: match[2],
    endpointIdBytes: decodeEndpointId(match[2]),
    basePath: path === "/" ? "" : path,
    alpn: scheme === "iroh" ? IROH_ARROW_MUX_ALPN : IROH_HTTP_ALPN,
  };
}

interface NativeRecvStream {
  read(limit: number): Promise<number[]>;
  stop(errorCode: bigint): Promise<void>;
}
interface NativeSendStream {
  writeAll(bytes: number[]): Promise<void>;
  finish(): Promise<void>;
  reset(errorCode: bigint): Promise<void>;
}
interface NativeConnection {
  openBi(): Promise<{ recv: NativeRecvStream; send: NativeSendStream }>;
  close(errorCode: bigint, reason: number[]): void;
}
interface NativeEndpoint {
  connect(address: unknown, alpn: number[]): Promise<NativeConnection>;
  close(): Promise<void>;
}
interface NativeEndpointBuilder {
  applyN0(): void;
  applyN0DisableRelay(): void;
  secretKey(bytes: number[]): void;
  relayMode(mode: unknown): void;
  bind(): Promise<NativeEndpoint>;
}

/** Minimal surface implemented by the official `@number0/iroh` Node binding. */
export interface IrohNativeBinding {
  Endpoint: { builder(): NativeEndpointBuilder };
  EndpointId: { fromBytes(bytes: number[]): unknown };
  EndpointAddr: new (id: unknown, relayUrl?: string | null, addresses?: string[] | null) => unknown;
  RelayMode: { customFromUrls(urls: string[]): unknown };
}

export interface IrohConnectOptions extends PipeConnectOptions {
  /** Optional 32-byte Ed25519 secret. Omit for a process-local ephemeral identity. */
  secretKey?: Uint8Array;
  /** Custom relay URLs. Mutually exclusive with `noRelay`. */
  relayUrls?: readonly string[];
  /** Disable relay use. Direct discovery/addressing must then succeed. */
  noRelay?: boolean;
  /** Total endpoint bind, connection, and stream-open deadline. Default: 30000 ms. */
  connectTimeoutMs?: number;
  /** Deadline for each active native read or write. Default: 300000 ms. */
  ioTimeoutMs?: number;
  /** Optional relay hint for the remote endpoint. */
  remoteRelayUrl?: string;
  /** Optional direct socket-address hints for the remote endpoint. */
  directAddresses?: readonly string[];
  /** Cancels connection setup and later closes the active native connection. */
  signal?: AbortSignal;
  /** Dependency-injection seam for testing or an application-pinned binding. */
  binding?: IrohNativeBinding;
}

async function loadBinding(): Promise<IrohNativeBinding> {
  try {
    const packageName = "@number0/iroh";
    const loaded = (await import(packageName)) as unknown as { default?: IrohNativeBinding } & IrohNativeBinding;
    return loaded.default ?? loaded;
  } catch (error) {
    throw new IrohTransportError(
      "iroh:// requires the optional @number0/iroh native package; install a supported platform build or pass options.binding",
      "bind",
      "unsupported",
      "not_sent",
      { cause: error },
    );
  }
}

/**
 * Connect the ordinary VGI raw client over a native Iroh bidirectional stream.
 * HTTP-over-Iroh is deliberately not routed through this function: it needs an
 * iroh-http/2 codec, not raw Arrow-mux framing. The httpi scheme is therefore
 * parsed for the shared endpoint contract but explicitly unsupported here.
 */
export async function irohConnect(rawEndpoint: string, options: IrohConnectOptions = {}): Promise<RpcClient> {
  const target = parseIrohEndpoint(rawEndpoint);
  if (target.scheme !== "iroh") {
    throw new IrohTransportError(
      "irohConnect only accepts iroh:// endpoints; httpi:// requires an iroh-http/2 client",
      "bind",
      "unsupported",
      "not_sent",
    );
  }
  if (options.noRelay && options.relayUrls && options.relayUrls.length !== 0) {
    throw new IrohTransportError("noRelay and relayUrls are mutually exclusive", "parse", "invalid_input", "not_sent");
  }
  if (options.secretKey && options.secretKey.byteLength !== 32) {
    throw new IrohTransportError("Iroh secretKey must contain exactly 32 bytes", "parse", "invalid_input", "not_sent");
  }
  const connectTimeoutMs = options.connectTimeoutMs ?? 30_000;
  if (!Number.isFinite(connectTimeoutMs) || connectTimeoutMs <= 0) {
    throw new IrohTransportError("connectTimeoutMs must be positive and finite", "parse", "invalid_input", "not_sent");
  }
  const ioTimeoutMs = options.ioTimeoutMs ?? 300_000;
  if (!Number.isFinite(ioTimeoutMs) || ioTimeoutMs <= 0) {
    throw new IrohTransportError("ioTimeoutMs must be positive and finite", "parse", "invalid_input", "not_sent");
  }
  if (options.signal?.aborted) {
    throw new IrohTransportError("Iroh connection aborted", "cancel", "cancelled", "not_sent", {
      cause: options.signal.reason,
    });
  }

  const native = options.binding ?? (await loadBinding());
  const builder = native.Endpoint.builder();
  if (options.noRelay) builder.applyN0DisableRelay();
  else builder.applyN0();
  builder.secretKey(Array.from(options.secretKey ?? processEphemeralSecretKey));
  if (options.relayUrls) builder.relayMode(native.RelayMode.customFromUrls([...options.relayUrls]));

  let setupTimer: ReturnType<typeof setTimeout> | undefined;
  let rejectSetup!: (error: Error) => void;
  let setupStage: IrohErrorStage = "bind";
  const setupCancelled = new Promise<never>((_, reject) => {
    rejectSetup = reject;
    setupTimer = setTimeout(
      () =>
        reject(
          new IrohTransportError(
            `Iroh connection timed out after ${connectTimeoutMs} ms`,
            setupStage,
            "timeout",
            "not_sent",
          ),
        ),
      connectTimeoutMs,
    );
  });
  const onSetupAbort = () =>
    rejectSetup(
      new IrohTransportError("Iroh connection aborted", "cancel", "cancelled", "not_sent", {
        cause: options.signal?.reason,
      }),
    );
  options.signal?.addEventListener("abort", onSetupAbort, { once: true });
  let endpoint: NativeEndpoint | undefined;
  let connection: NativeConnection | undefined;
  let recv: NativeRecvStream | undefined;
  let send: NativeSendStream | undefined;
  try {
    const binding = builder.bind();
    try {
      endpoint = await Promise.race([binding, setupCancelled]);
    } catch (error) {
      // A timed-out native bind cannot be cancelled through the Node API. If
      // it resolves later, close it immediately instead of leaking an endpoint.
      void binding.then((lateEndpoint) => lateEndpoint.close()).catch(() => {});
      throw transportError(error, "bind", "unavailable", "not_sent");
    }
    const id = native.EndpointId.fromBytes(Array.from(target.endpointIdBytes));
    setupStage = "connect";
    try {
      connection = await Promise.race([
        endpoint.connect(
          new native.EndpointAddr(
            id,
            options.remoteRelayUrl ?? null,
            options.directAddresses ? [...options.directAddresses] : null,
          ),
          Array.from(new TextEncoder().encode(target.alpn)),
        ),
        setupCancelled,
      ]);
    } catch (error) {
      throw transportError(error, "connect", "unavailable", "not_sent");
    }
    setupStage = "open_stream";
    try {
      ({ recv, send } = await Promise.race([connection.openBi(), setupCancelled]));
    } catch (error) {
      throw transportError(error, "open_stream", "unavailable", "not_sent");
    }
  } catch (error) {
    await endpoint?.close().catch(() => {});
    throw error;
  } finally {
    if (setupTimer) clearTimeout(setupTimer);
    options.signal?.removeEventListener("abort", onSetupAbort);
  }
  if (!endpoint || !connection || !recv || !send) throw new Error("Iroh connection setup did not produce a stream");

  const onActiveAbort = () => {
    void recv.stop(0n).catch(() => {});
    connection.close(0n, []);
    void endpoint.close();
  };
  options.signal?.addEventListener("abort", onActiveAbort, { once: true });

  async function activeIo<T>(
    operation: Promise<T>,
    stage: "read" | "write",
    certainty: "unknown" | "sent",
    cancel: () => void,
  ): Promise<T> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        operation,
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => {
            cancel();
            reject(
              new IrohTransportError(`Iroh ${stage} timed out after ${ioTimeoutMs} ms`, stage, "timeout", certainty),
            );
          }, ioTimeoutMs);
        }),
      ]);
    } catch (error) {
      if (options.signal?.aborted) {
        throw new IrohTransportError("Iroh operation cancelled", "cancel", "cancelled", certainty, { cause: error });
      }
      throw transportError(error, stage, "connection_reset", certainty);
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  let writeQueue = Promise.resolve();
  let firstWriteResolve!: () => void;
  const firstWrite = new Promise<void>((resolve) => {
    firstWriteResolve = resolve;
  });
  const readable = new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        // ReadableStream is allowed to pull eagerly at construction. Do not
        // enter the native receive call until pipeConnect has queued its first
        // request, and surface a failed native write before waiting for bytes
        // the peer can never produce.
        await firstWrite;
        await writeQueue;
        const chunk = await activeIo(recv.read(64 * 1024 * 1024), "read", "sent", () => {
          void recv.stop(0n).catch(() => {});
        });
        if (chunk.length === 0) controller.close();
        else controller.enqueue(Uint8Array.from(chunk));
      } catch (error) {
        controller.error(error);
      }
    },
    async cancel() {
      await recv.stop(0n).catch(() => {});
    },
  });
  const writable = {
    write(bytes: Uint8Array): void {
      const owned = Array.from(bytes);
      writeQueue = writeQueue.then(() =>
        activeIo(send.writeAll(owned), "write", "unknown", () => {
          void send.reset(0n).catch(() => {});
        }),
      );
      firstWriteResolve();
    },
    end(): void {
      writeQueue = writeQueue.then(() =>
        activeIo(send.finish(), "write", "unknown", () => {
          void send.reset(0n).catch(() => {});
        }),
      );
      firstWriteResolve();
    },
  };
  const client = pipeConnect(readable, writable, options);
  const close = client.close;
  client.close = () => {
    options.signal?.removeEventListener("abort", onActiveAbort);
    close.call(client);
    void writeQueue.then(
      () => {
        connection.close(0n, []);
        void endpoint.close();
      },
      () => {
        connection.close(0n, []);
        void endpoint.close();
      },
    );
  };
  return client;
}
