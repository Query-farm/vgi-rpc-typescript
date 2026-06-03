/**
 * OpenTelemetry instrumentation for vgi-rpc TypeScript servers.
 *
 * Implements {@link DispatchHook} to add distributed tracing (spans) and
 * metrics (request counter, duration histogram) to RPC dispatch.
 *
 * Requires `@opentelemetry/api` as a peer dependency.
 *
 * @example
 * ```typescript
 * import { createOtelHook } from "vgi-rpc/otel";
 * import { createHttpHandler } from "vgi-rpc";
 *
 * const handler = createHttpHandler(protocol, {
 *   dispatchHook: createOtelHook(),
 * });
 * ```
 */
import { type Meter, type Tracer } from "@opentelemetry/api";
import type { DispatchHook } from "./types.js";
/** Configuration for OpenTelemetry instrumentation. */
export interface OtelConfig {
    /** Custom TracerProvider; uses the global provider when omitted. */
    tracerProvider?: {
        getTracer(name: string): Tracer;
    };
    /** Custom MeterProvider; uses the global provider when omitted. */
    meterProvider?: {
        getMeter(name: string): Meter;
    };
    /** Enable span creation. Default: true. */
    enableTracing?: boolean;
    /** Enable counter/histogram recording. Default: true. */
    enableMetrics?: boolean;
    /** Record exceptions on error spans. Default: true. */
    recordExceptions?: boolean;
    /** Service name for the rpc.service attribute. Default: "TypeScriptRpcServer". */
    serviceName?: string;
}
/**
 * Create a {@link DispatchHook} that instruments RPC calls with OpenTelemetry.
 *
 * Creates a span for each RPC call with method attributes, and records
 * request count and duration metrics.
 */
export declare function createOtelHook(config?: OtelConfig): DispatchHook;
//# sourceMappingURL=otel.d.ts.map