// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { DataType } from "@query-farm/apache-arrow";

/**
 * Arrow types whose `.get(0)` / `vectorFromArray` round-trips are unreliable
 * in arrow-js. For these we extract and re-emit the underlying `Data` object
 * directly (passthrough), like we already do for Map_.
 *
 * Covers Date/Time/Timestamp/Duration/Decimal/LargeUtf8/LargeBinary/
 * FixedSizeBinary/Dictionary.
 */
export function isOpaquePassthroughType(type: DataType): boolean {
  return (
    DataType.isDate(type) ||
    DataType.isTime(type) ||
    DataType.isTimestamp(type) ||
    DataType.isDuration(type) ||
    DataType.isDecimal(type) ||
    DataType.isLargeUtf8(type) ||
    DataType.isLargeBinary(type) ||
    DataType.isFixedSizeBinary(type) ||
    DataType.isDictionary(type)
  );
}
