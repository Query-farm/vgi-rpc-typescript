// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Small constructors shared by the two transports' raw (batch-level) call
 * paths. Internal: nothing here is part of the published surface.
 *
 * @internal
 */

import type { RecordBatch } from "@query-farm/apache-arrow";
import { withMetadata } from "./outbound.js";
import type { RawBatch } from "./raw.js";

/**
 * Pair a decoded batch with its own custom metadata.
 *
 * @internal
 */
export function rawBatchOf(batch: RecordBatch): RawBatch {
  return { batch, metadata: new Map(batch.metadata ?? []) };
}

/**
 * Rebuild `input`'s batch so the bytes written carry exactly `input.metadata`.
 *
 * The caller's metadata map is authoritative — it is what the peer must see —
 * and a batch decoded from one stream and rewritten to another would
 * otherwise carry whatever metadata its own message happened to hold.
 *
 * @internal
 */
export function rawInputBatch(input: RawBatch, extra?: ReadonlyMap<string, string>): RecordBatch {
  const metadata = new Map(input.metadata);
  if (extra) for (const [key, value] of extra) metadata.set(key, value);
  // Either backend's batch, rebuilt in the active one (see outbound.ts).
  return withMetadata(input.batch, metadata);
}
