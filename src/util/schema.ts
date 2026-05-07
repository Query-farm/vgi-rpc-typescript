// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { type VgiSchema, serializeSchema as facadeSerializeSchema } from "../arrow/index.js";

/**
 * Serialize a Schema to the Arrow IPC Schema message format.
 * This produces bytes compatible with Python's `pa.ipc.read_schema()`.
 * Equivalent to writing an empty-batch IPC stream — schema message + EOS marker.
 */
export function serializeSchema(schema: VgiSchema): Uint8Array {
  return facadeSerializeSchema(schema);
}
