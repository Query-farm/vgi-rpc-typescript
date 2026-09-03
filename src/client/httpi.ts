// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { DEFAULT_ACCEPTED_MAX_RESPONSE_BYTES } from "#vgi-rpc-client-response-budget";
import { optionalResponseBudget } from "../http/response-budget.js";
import { type HttpRpcClient, httpConnect } from "./connect.js";
import { IrohTransportError, parseIrohEndpoint } from "./iroh.js";
import type { HttpConnectOptions } from "./types.js";

/** Native endpoint options accepted by the official iroh-http Node adapter. */
export type IrohHttpNodeOptions = import("@momics/iroh-http-node").NodeOptions;

/** Per-request controls understood by an iroh-http/2 Fetch implementation. */
export interface IrohHttpFetchInit extends RequestInit {
  /** Known direct QUIC addresses for the remote endpoint. */
  directAddrs?: string[];
  /** Known home-relay URL for the remote endpoint. */
  relayUrl?: string;
  /** Complete iroh-http request deadline in milliseconds. */
  requestTimeout?: number;
  /** Preserve wire Content-Encoding for VGI's HTTP codec. */
  decompress?: boolean;
  /** Maximum response bytes buffered/read by the native adapter. */
  maxResponseBodyBytes?: number;
}

/** Minimal typed surface shared by native iroh-http nodes and test adapters. */
export interface IrohHttpNode {
  /** Execute one Fetch-compatible request over iroh-http/2. */
  fetch(input: string | URL, init?: IrohHttpFetchInit): Promise<Response>;
  /** Close this Iroh endpoint and release its native resources. */
  close(options?: { force?: boolean }): Promise<void>;
}

/** Dependency-injection surface implemented by `@momics/iroh-http-node`. */
export interface IrohHttpBinding {
  /** Bind a native Iroh endpoint for outgoing HTTP-over-Iroh requests. */
  createNode(options?: IrohHttpNodeOptions): Promise<IrohHttpNode>;
}

/** Options for {@link httpiConnect}, the typed HTTP-over-Iroh VGI client. */
export interface HttpiConnectOptions extends Omit<HttpConnectOptions, "fetch"> {
  /** Application-owned Iroh node. Omit to create one with the native binding. */
  node?: IrohHttpNode;
  /** Options used only when this function creates the native Iroh node. */
  nodeOptions?: IrohHttpNodeOptions;
  /** Test/application-pinned native binding. Defaults to `@momics/iroh-http-node`. */
  binding?: IrohHttpBinding;
  /** Known direct socket addresses for the remote endpoint. */
  directAddresses?: readonly string[];
  /** Known relay URL for the remote endpoint. */
  remoteRelayUrl?: string;
  /** Complete deadline for each iroh-http request. */
  requestTimeoutMs?: number;
  /** Cancels every subsequent request made by this client. */
  signal?: AbortSignal;
  /** Fetch used for non-httpi external-location URLs. Defaults to global fetch. */
  externalFetch?: typeof globalThis.fetch;
  /** Close an application-owned node when the RPC client closes. Defaults to false. */
  closeNode?: boolean;
}

const BASE32_ALPHABET = "abcdefghijklmnopqrstuvwxyz234567";
const IROH_HTTP_MAX_RESPONSE_BYTES = 256 * 1024 * 1024;

function encodeIrohNodeId(bytes: Uint8Array): string {
  let accumulator = 0;
  let bits = 0;
  let encoded = "";
  for (const byte of bytes) {
    accumulator = (accumulator << 8) | byte;
    bits += 8;
    while (bits >= 5) {
      bits -= 5;
      encoded += BASE32_ALPHABET[(accumulator >>> bits) & 31];
      accumulator &= (1 << bits) - 1;
    }
  }
  if (bits !== 0) encoded += BASE32_ALPHABET[(accumulator << (5 - bits)) & 31];
  return encoded;
}

function nativeHttpiUrl(raw: string): string {
  const parsed = new URL(raw);
  const endpoint = parseIrohEndpoint(`httpi://${parsed.hostname}`);
  parsed.hostname = encodeIrohNodeId(endpoint.endpointIdBytes);
  return parsed.href;
}

function positiveMilliseconds(value: number | undefined, name: string): void {
  if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) {
    throw new IrohTransportError(`${name} must be a positive safe integer`, "parse", "invalid_input", "not_sent");
  }
}

async function loadHttpBinding(): Promise<IrohHttpBinding> {
  try {
    const packageName = "@momics/iroh-http-node";
    return (await import(packageName)) as unknown as IrohHttpBinding;
  } catch (error) {
    throw new IrohTransportError(
      "httpi:// requires the optional @momics/iroh-http-node package; install a supported native build or pass options.node",
      "bind",
      "unsupported",
      "not_sent",
      { cause: error },
    );
  }
}

/**
 * Connect the ordinary VGI HTTP client over native `iroh-http/2`.
 *
 * The adapter converts VGI's canonical 64-hex EndpointId URI to iroh-http's
 * base-32 hostname internally. HTTP capability discovery, response budgets,
 * compression, authorization, continuation calls, and external locations all
 * remain owned by {@link httpConnect}.
 */
export async function httpiConnect(rawBaseUrl: string, options: HttpiConnectOptions = {}): Promise<HttpRpcClient> {
  const target = parseIrohEndpoint(rawBaseUrl);
  if (target.scheme !== "httpi") {
    throw new IrohTransportError("httpiConnect only accepts httpi:// endpoints", "parse", "invalid_input", "not_sent");
  }
  if (options.node && options.nodeOptions) {
    throw new IrohTransportError(
      "nodeOptions cannot be used with an application-owned Iroh node",
      "parse",
      "invalid_input",
      "not_sent",
    );
  }
  positiveMilliseconds(options.requestTimeoutMs, "requestTimeoutMs");
  const acceptedMaxResponseBytes = options.acceptedMaxResponseBytes ?? DEFAULT_ACCEPTED_MAX_RESPONSE_BYTES;
  optionalResponseBudget(acceptedMaxResponseBytes, "acceptedMaxResponseBytes");
  if (acceptedMaxResponseBytes > IROH_HTTP_MAX_RESPONSE_BYTES) {
    throw new IrohTransportError(
      `acceptedMaxResponseBytes must not exceed the iroh-http limit of ${IROH_HTTP_MAX_RESPONSE_BYTES}`,
      "parse",
      "invalid_input",
      "not_sent",
    );
  }
  if (options.signal?.aborted) {
    throw new IrohTransportError("Iroh connection aborted", "cancel", "cancelled", "not_sent", {
      cause: options.signal.reason,
    });
  }

  const ownsNode = options.node === undefined;
  let node: IrohHttpNode;
  try {
    node = options.node ?? (await (options.binding ?? (await loadHttpBinding())).createNode(options.nodeOptions));
  } catch (error) {
    if (error instanceof IrohTransportError) throw error;
    throw new IrohTransportError(
      error instanceof Error ? error.message : String(error),
      "bind",
      "unavailable",
      "not_sent",
      { cause: error },
    );
  }

  const externalFetch = options.externalFetch ?? globalThis.fetch;
  const routeFetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const raw = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (!raw.startsWith("httpi://")) return externalFetch(input, init);
    try {
      return await node.fetch(nativeHttpiUrl(raw), {
        ...init,
        directAddrs: options.directAddresses ? [...options.directAddresses] : undefined,
        relayUrl: options.remoteRelayUrl,
        requestTimeout: options.requestTimeoutMs,
        // VGI owns Content-Encoding negotiation and bounded decompression.
        decompress: false,
        maxResponseBodyBytes: acceptedMaxResponseBytes,
        signal: init?.signal ?? options.signal,
      });
    } catch (error) {
      const cancelled = options.signal?.aborted || (error instanceof DOMException && error.name === "AbortError");
      throw new IrohTransportError(
        error instanceof Error ? error.message : String(error),
        cancelled ? "cancel" : "read",
        cancelled ? "cancelled" : "connection_reset",
        "unknown",
        { cause: error },
      );
    }
  }) as typeof globalThis.fetch;

  let client: HttpRpcClient;
  try {
    const {
      node: _node,
      nodeOptions: _nodeOptions,
      binding: _binding,
      directAddresses: _directAddresses,
      remoteRelayUrl: _remoteRelayUrl,
      requestTimeoutMs: _requestTimeoutMs,
      signal: _signal,
      externalFetch: _externalFetch,
      closeNode: _closeNode,
      ...httpOptions
    } = options;
    client = httpConnect(rawBaseUrl, {
      ...httpOptions,
      acceptedMaxResponseBytes,
      fetch: routeFetch,
    });
  } catch (error) {
    if (ownsNode) await node.close({ force: true }).catch(() => {});
    throw error;
  }

  const closeHttp = client.close;
  let closed = false;
  client.close = () => {
    if (closed) return;
    closed = true;
    closeHttp.call(client);
    if (ownsNode || options.closeNode === true) void node.close();
  };
  return client;
}
