import { type ProtocolHost } from "../binding.js";
import type { Protocol } from "../protocol.js";
import { type HttpHandlerOptions } from "./types.js";
/**
 * Create a fetch-compatible HTTP handler for a vgi-rpc Protocol, or for a
 * whole `VgiRpcServer` and every protocol it hosts.
 *
 * Compatible with Bun.serve(), Deno.serve(), Cloudflare Workers, and any
 * Web API runtime that uses the standard Request/Response types.
 *
 * RPC routes are namespaced by protocol -- `{prefix}/{protocol}/{method}`,
 * plus `/init` and `/exchange` for streams -- so a server hosting
 * `vgi_rpc.Reflection.v1` and `vgi_rpc.Identity.v1` alongside its application
 * protocol reaches all three over HTTP. Pass the server, not its primary
 * protocol, for the co-hosted ones to be routable.
 *
 * The protocol rides twice on HTTP: in the request's `vgi_rpc.protocol`
 * metadata and as that path segment. **The metadata is canonical** -- it is
 * the only carrier on the stdio, unix and named-pipe transports -- and the
 * path is a required faithful projection of it, present so an edge device can
 * act on the protocol without an Arrow parser. This handler therefore
 * requires the metadata on every unary call and stream `/init` and rejects a
 * request whose two carriers disagree; unchecked, edge policy would be applied
 * to one protocol while the worker dispatched another.
 *
 * @example
 * ```typescript
 * const handler = createHttpHandler(server);
 * Bun.serve({ port: 8080, fetch: handler });
 * ```
 */
export declare function createHttpHandler(target: Protocol | ProtocolHost, options?: HttpHandlerOptions): (request: Request) => Response | Promise<Response>;
//# sourceMappingURL=handler.d.ts.map