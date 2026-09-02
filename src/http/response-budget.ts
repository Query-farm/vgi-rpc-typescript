// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

export const ACCEPT_MAX_RESPONSE_BYTES_HEADER = "VGI-Accept-Max-Response-Bytes";
export const ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER = "VGI-Accept-Max-Response-Bytes-Support";
export const MAX_SAFE_RESPONSE_BYTES = Number.MAX_SAFE_INTEGER;
export const MIN_RESPONSE_BYTES = 64 * 1024;

/** Parse the cross-SDK positive-decimal grammar without whitespace, signs,
 * leading zeroes, exponent notation, or values above 2^53-1. */
export function parsePositiveSafeDecimal(raw: string): number {
  if (!/^[1-9][0-9]*$/.test(raw)) {
    throw new TypeError("must be a positive decimal integer");
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value > MAX_SAFE_RESPONSE_BYTES) {
    throw new TypeError(`must not exceed ${MAX_SAFE_RESPONSE_BYTES}`);
  }
  return value;
}

export function optionalPositiveSafeInteger(value: number | undefined, name: string): number | undefined {
  if (value === undefined) return undefined;
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
  return value;
}

/** Validate a configured or advertised response budget. The wire grammar is
 * positive decimal, but deployable response limits smaller than 64 KiB are
 * deliberately rejected across every SDK. */
export function parseResponseBudgetDecimal(raw: string): number {
  const value = parsePositiveSafeDecimal(raw);
  if (value < MIN_RESPONSE_BYTES) throw new TypeError(`must be at least ${MIN_RESPONSE_BYTES}`);
  return value;
}

export function optionalResponseBudget(value: number | undefined, name: string): number | undefined {
  const parsed = optionalPositiveSafeInteger(value, name);
  if (parsed !== undefined && parsed < MIN_RESPONSE_BYTES) {
    throw new TypeError(`${name} must be at least ${MIN_RESPONSE_BYTES}`);
  }
  return parsed;
}

export function minPositive(...values: Array<number | undefined>): number | undefined {
  let result: number | undefined;
  for (const value of values) {
    if (value !== undefined && value > 0 && (result === undefined || value < result)) result = value;
  }
  return result;
}
