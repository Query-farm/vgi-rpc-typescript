// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

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

import {
  type Attributes,
  type Counter,
  type Histogram,
  type Meter,
  metrics,
  type Span,
  SpanKind,
  SpanStatusCode,
  type Tracer,
  trace,
} from "@opentelemetry/api";
import type { CallStatistics, DispatchHook, DispatchInfo, HookToken } from "./types.js";

const INSTRUMENTATION_NAME = "vgi_rpc";

/** Configuration for OpenTelemetry instrumentation. */
export interface OtelConfig {
  /** Custom TracerProvider; uses the global provider when omitted. */
  tracerProvider?: { getTracer(name: string): Tracer };
  /** Custom MeterProvider; uses the global provider when omitted. */
  meterProvider?: { getMeter(name: string): Meter };
  /** Enable span creation. Default: true. */
  enableTracing?: boolean;
  /** Enable counter/histogram recording. Default: true. */
  enableMetrics?: boolean;
  /** Record exceptions on error spans. Default: true. */
  recordExceptions?: boolean;
  /** Service name for the rpc.service attribute. Default: "TypeScriptRpcServer". */
  serviceName?: string;
}

interface OtelHookToken {
  span: Span | null;
  startTime: number;
}

/**
 * Create a {@link DispatchHook} that instruments RPC calls with OpenTelemetry.
 *
 * Creates a span for each RPC call with method attributes, and records
 * request count and duration metrics.
 */
export function createOtelHook(config?: OtelConfig): DispatchHook {
  const enableTracing = config?.enableTracing ?? true;
  const enableMetrics = config?.enableMetrics ?? true;
  const recordExceptions = config?.recordExceptions ?? true;
  const serviceName = config?.serviceName ?? "TypeScriptRpcServer";

  const tracer = (config?.tracerProvider ?? trace).getTracer(INSTRUMENTATION_NAME);

  let requestCounter: Counter | null = null;
  let durationHistogram: Histogram | null = null;

  if (enableMetrics) {
    const meter = (config?.meterProvider ?? metrics).getMeter(INSTRUMENTATION_NAME);
    requestCounter = meter.createCounter("rpc.server.requests", {
      unit: "{request}",
      description: "Number of RPC requests handled",
    });
    durationHistogram = meter.createHistogram("rpc.server.duration", {
      unit: "s",
      description: "Duration of RPC requests",
    });
  }

  return {
    onDispatchStart(info: DispatchInfo): HookToken {
      const startTime = performance.now();

      if (!enableTracing) {
        return { span: null, startTime } satisfies OtelHookToken;
      }

      const spanName = `vgi_rpc/${info.method}`;
      const attrs: Attributes = {
        "rpc.system": "vgi_rpc",
        "rpc.service": serviceName,
        "rpc.method": info.method,
        "rpc.vgi_rpc.method_type": info.methodType,
        "rpc.vgi_rpc.server_id": info.serverId,
      };
      if (info.requestId) {
        attrs["rpc.vgi_rpc.request_id"] = info.requestId;
      }

      const span = tracer.startSpan(spanName, {
        kind: SpanKind.SERVER,
        attributes: attrs,
      });

      return { span, startTime } satisfies OtelHookToken;
    },

    onDispatchEnd(token: HookToken, info: DispatchInfo, stats: CallStatistics, error?: Error): void {
      const t = token as OtelHookToken;
      const durationS = (performance.now() - t.startTime) / 1000;
      const status = error ? "error" : "ok";

      // Finalize span
      if (t.span) {
        if (stats) {
          t.span.setAttributes({
            "rpc.vgi_rpc.input_batches": stats.inputBatches,
            "rpc.vgi_rpc.output_batches": stats.outputBatches,
            "rpc.vgi_rpc.input_rows": stats.inputRows,
            "rpc.vgi_rpc.output_rows": stats.outputRows,
            "rpc.vgi_rpc.input_bytes": stats.inputBytes,
            "rpc.vgi_rpc.output_bytes": stats.outputBytes,
          });
        }

        if (error) {
          t.span.setStatus({ code: SpanStatusCode.ERROR, message: error.message });
          t.span.setAttribute("rpc.vgi_rpc.error_type", error.constructor.name);
          if (recordExceptions) {
            t.span.recordException(error);
          }
        } else {
          t.span.setStatus({ code: SpanStatusCode.OK });
        }
        t.span.end();
      }

      // Record metrics
      if (enableMetrics) {
        const metricAttrs: Attributes = {
          "rpc.system": "vgi_rpc",
          "rpc.service": serviceName,
          "rpc.method": info.method,
          "rpc.vgi_rpc.method_type": info.methodType,
          status,
        };
        requestCounter?.add(1, metricAttrs);
        durationHistogram?.record(durationS, metricAttrs);
      }
    },
  };
}
