// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/** Configuration options for createHttpHandler(). */
export interface HttpHandlerOptions {
  /** URL path prefix for all endpoints. Default: "/vgi" */
  prefix?: string;
  /** HMAC-SHA256 signing key for state tokens. Random 32 bytes if omitted. */
  signingKey?: Uint8Array;
  /** State token time-to-live in seconds. Default: 3600 (1 hour). 0 disables TTL checks. */
  tokenTtl?: number;
  /** CORS allowed origins. If set, CORS headers are added to all responses. */
  corsOrigins?: string;
  /** Maximum request body size in bytes. Advertised via VGI-Max-Request-Bytes header. */
  maxRequestBytes?: number;
  /** Maximum bytes before a producer stream emits a continuation token. */
  maxStreamResponseBytes?: number;
  /** Server ID included in response metadata. Random if omitted. */
  serverId?: string;
  /** Custom state serializer for stream state objects. Default: JSON with BigInt support. */
  stateSerializer?: StateSerializer;
}

/** Serializer for stream state objects stored in state tokens. */
export interface StateSerializer {
  serialize(state: any): Uint8Array;
  deserialize(bytes: Uint8Array): any;
}

/** Default state serializer using JSON (with BigInt support). */
export const jsonStateSerializer: StateSerializer = {
  serialize(state: any): Uint8Array {
    return new TextEncoder().encode(
      JSON.stringify(state, (_key, value) =>
        typeof value === "bigint" ? `__bigint__:${value}` : value,
      ),
    );
  },
  deserialize(bytes: Uint8Array): any {
    return JSON.parse(new TextDecoder().decode(bytes), (_key, value) =>
      typeof value === "string" && value.startsWith("__bigint__:")
        ? BigInt(value.slice(11))
        : value,
    );
  },
};
