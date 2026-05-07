// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import {
  type VgiDataType,
  isDate,
  isTime,
  isTimestamp,
  isDuration,
  isDecimal,
  isLargeUtf8,
  isLargeBinary,
  isFixedSizeBinary,
  isDictionary,
} from "../arrow/index.js";

/**
 * Arrow types whose `.get(0)` / `vectorFromArray` round-trips are unreliable
 * in arrow-js. For these we extract and re-emit the underlying `Data` object
 * directly (passthrough), like we already do for Map_.
 *
 * Covers Date/Time/Timestamp/Duration/Decimal/LargeUtf8/LargeBinary/
 * FixedSizeBinary/Dictionary.
 */
export function isOpaquePassthroughType(type: VgiDataType): boolean {
  return (
    isDate(type) ||
    isTime(type) ||
    isTimestamp(type) ||
    isDuration(type) ||
    isDecimal(type) ||
    isLargeUtf8(type) ||
    isLargeBinary(type) ||
    isFixedSizeBinary(type) ||
    isDictionary(type)
  );
}
