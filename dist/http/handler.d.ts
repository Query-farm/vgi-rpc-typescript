import type { Protocol } from "../protocol.js";
import { type HttpHandlerOptions } from "./types.js";
/**
 * Create a fetch-compatible HTTP handler for a vgi-rpc Protocol.
 *
 * Compatible with Bun.serve(), Deno.serve(), Cloudflare Workers, and any
 * Web API runtime that uses the standard Request/Response types.
 *
 * @example
 * ```typescript
 * const handler = createHttpHandler(protocol);
 * Bun.serve({ port: 8080, fetch: handler });
 * ```
 */
export declare function createHttpHandler(protocol: Protocol, options?: HttpHandlerOptions): (request: Request) => Response | Promise<Response>;
//# sourceMappingURL=handler.d.ts.map