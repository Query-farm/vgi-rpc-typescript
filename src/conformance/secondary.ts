// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * `conformance.Secondary.v1` -- the second application protocol every
 * conformance worker (and every VGI SDK fixture worker) hosts.
 *
 * Normative in the reference's `tools/cross-port/specs/MULTI_PROTOCOL_HOSTING.md`
 * §2; the constants here are what the shared suite asserts.
 *
 * - **Routing by pair.** `echo_string` repeats the name and signature of
 *   `ConformanceService.echo_string`, and prefixes its reply, so a server that
 *   dispatched on the bare method name answers with a wrong value.
 * - **A per-binding version gate.** It declares *no* `protocolVersion`, so a
 *   server gating every call against the primary's version refuses it.
 * - **The error model.** `fail` raises whatever code and kind it is asked for
 *   with a fixed detail set (one catalog type, `RetryInfo` when asked, one type
 *   no client knows); `fail_oversized` raises details over the 4 KiB cap.
 */

import { badRequest, type ErrorDetailJson, errorInfo, isErrorCode, retryInfo, StatusError } from "../error-model.js";
import { Protocol } from "../protocol.js";
import { float, str } from "../schema.js";

/** Routing key of the fixture protocol. */
export const SECONDARY_PROTOCOL_NAME = "conformance.Secondary.v1";

/** Pinned digest of the secondary's description, read back off a running
 *  worker by the suite. */
export const SECONDARY_PROTOCOL_HASH = "58557cf1611546ad22d1c379bc3ce1b04166082f78375e9fc959f0086347eab6";

/** What `echo_string` prepends. */
export const SECONDARY_ECHO_PREFIX = "secondary:";

/** The `ErrorInfo` every `fail` carries. */
export const FAIL_ERROR_INFO: ErrorDetailJson = {
  "@type": "vgi_rpc.ErrorInfo",
  metadata: { fixture: SECONDARY_PROTOCOL_NAME },
};

/** A detail type no client knows, legitimately named under this protocol. */
export const PROBE_DETAIL: ErrorDetailJson = {
  "@type": `${SECONDARY_PROTOCOL_NAME}.Probe`,
  note: "clients ignore detail types they do not know",
};

/** Kind `fail` answers with when asked for a code outside the closed set. */
export const INVALID_CODE_KIND = "invalid_code";
/** Kind `fail_oversized` raises. */
export const OVERSIZED_KIND = "details_oversized";
/** Size of `fail_oversized`'s padding -- over the cap on its own. */
export const OVERSIZED_PADDING_BYTES = 5000;

/** The detail array `fail` sends, in wire order: `ErrorInfo`, `RetryInfo`
 *  only when the delay is positive, then the probe type. */
export function expectedFailDetails(retryDelaySeconds: number): ErrorDetailJson[] {
  const details: ErrorDetailJson[] = [FAIL_ERROR_INFO];
  if (retryDelaySeconds > 0) details.push(retryInfo(retryDelaySeconds) as unknown as ErrorDetailJson);
  details.push(PROBE_DETAIL);
  return details;
}

/**
 * Build `conformance.Secondary.v1`, handlers included.
 *
 * A {@link Protocol} carries its implementation, so this is the whole
 * `(protocol, implementation)` pair: pass it to `VgiRpcServer`'s `protocols`
 * option (or an SDK's hosting hook). A fresh instance per call.
 */
export function buildSecondaryProtocol(): Protocol {
  const p = new Protocol(SECONDARY_PROTOCOL_NAME);
  p.unary("echo_string", {
    params: { value: str },
    result: { result: str },
    doc: "Return 'secondary:' + value; collides with the primary's echo_string.",
    handler: (params) => ({ result: SECONDARY_ECHO_PREFIX + String(params.value) }),
  });
  p.unary("fail", {
    params: { code: str, kind: str, retry_delay_seconds: float },
    result: {},
    doc: "Raise an error with code, kind (absent when empty) and the fixed details.",
    handler: (params) => {
      const code = String(params.code);
      const kind = String(params.kind);
      const delay = Number(params.retry_delay_seconds);
      if (!isErrorCode(code)) {
        throw new StatusError(`'${code}' is not a canonical error code`, {
          code: "INVALID_ARGUMENT",
          kind: INVALID_CODE_KIND,
          details: [badRequest([{ field: "code", description: "must be a canonical code name" }])],
        });
      }
      throw new StatusError(`conformance.Secondary.v1 fail: ${code} ${kind}`.trimEnd(), {
        code,
        kind: kind || undefined,
        details: expectedFailDetails(delay),
      });
    },
  });
  p.unary("fail_oversized", {
    params: {},
    result: {},
    doc: "Raise an error whose details exceed the 4 KiB cap.",
    handler: () => {
      // RetryInfo first and small: a server dropping only the element that
      // does not fit keeps it, and the error then reads as retryable.
      throw new StatusError("conformance.Secondary.v1 fail_oversized: details exceed 4 KiB", {
        code: "RESOURCE_EXHAUSTED",
        kind: OVERSIZED_KIND,
        details: [retryInfo(1), errorInfo({ padding: "x".repeat(OVERSIZED_PADDING_BYTES) })],
      });
    },
  });
  return p;
}
