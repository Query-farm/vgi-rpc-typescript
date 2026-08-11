// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Runtime detection.
 *
 * Kept separate from the capability probes in `zstd.ts` / `gzip.ts`: those ask
 * "can this runtime do X", which is the right question almost everywhere. This
 * one names the runtime itself, which is only ever the right question when the
 * *host* does something to our responses that we have to stay out of the way
 * of — see `isWorkerd`.
 */

/**
 * True on Cloudflare Workers (workerd), including `wrangler dev`.
 *
 * `navigator.userAgent` is the identification Cloudflare documents for this;
 * it is absent on Node (until `navigator` was added) and differs on Bun/Deno,
 * so the exact-string compare is the reliable form. Guarded because
 * `navigator` is not defined at all on older Node.
 */
export function isWorkerd(): boolean {
  return typeof navigator !== "undefined" && navigator.userAgent === "Cloudflare-Workers";
}
