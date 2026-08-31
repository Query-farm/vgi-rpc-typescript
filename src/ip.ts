// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/** Parse and canonicalize an exact IPv4 or IPv6 literal without DNS. */
export function normalizeIpLiteral(value: string): string | null {
  const ipv4 = parseIpv4(value);
  if (ipv4) return ipv4.join(".");
  if (!value.includes(":") || value.includes("%") || value.includes("[") || value.includes("]")) return null;

  const halves = value.toLowerCase().split("::");
  if (halves.length > 2) return null;
  const left = parseIpv6Words(halves[0], halves.length === 1);
  const right = parseIpv6Words(halves.length === 2 ? halves[1] : "", true);
  if (!left || !right) return null;
  let words: number[];
  if (halves.length === 1) {
    if (left.length !== 8) return null;
    words = left;
  } else {
    const omitted = 8 - left.length - right.length;
    if (omitted < 1) return null;
    words = [...left, ...Array<number>(omitted).fill(0), ...right];
  }
  if (words.length !== 8) return null;
  if (words.slice(0, 5).every((word) => word === 0) && words[5] === 0xffff) {
    return `${words[6] >>> 8}.${words[6] & 255}.${words[7] >>> 8}.${words[7] & 255}`;
  }

  let bestStart = -1;
  let bestLength = 0;
  for (let index = 0; index < words.length; ) {
    if (words[index] !== 0) {
      index++;
      continue;
    }
    let end = index;
    while (end < words.length && words[end] === 0) end++;
    if (end - index > bestLength && end - index >= 2) {
      bestStart = index;
      bestLength = end - index;
    }
    index = end;
  }
  if (bestStart < 0) return words.map((word) => word.toString(16)).join(":");
  const before = words
    .slice(0, bestStart)
    .map((word) => word.toString(16))
    .join(":");
  const after = words
    .slice(bestStart + bestLength)
    .map((word) => word.toString(16))
    .join(":");
  return `${before}::${after}`;
}

export function normalizeTrustedProxyAddresses(values: Iterable<string>, label: string): ReadonlySet<string> {
  const normalized = new Set<string>();
  let count = 0;
  for (const value of values) {
    count++;
    const address = normalizeIpLiteral(value);
    if (!address) throw new TypeError(`${label} must contain exact IP literals`);
    if (normalized.has(address)) throw new TypeError(`${label} contains a duplicate normalized address`);
    normalized.add(address);
  }
  if (count === 0) throw new TypeError(`${label} must not be empty`);
  return normalized;
}

function parseIpv4(value: string): number[] | null {
  const parts = value.split(".");
  if (parts.length !== 4) return null;
  const bytes: number[] = [];
  for (const part of parts) {
    if (!/^(?:0|[1-9][0-9]{0,2})$/u.test(part)) return null;
    const byte = Number(part);
    if (byte > 255) return null;
    bytes.push(byte);
  }
  return bytes;
}

function parseIpv6Words(value: string, allowIpv4: boolean): number[] | null {
  if (value === "") return [];
  const parts = value.split(":");
  const words: number[] = [];
  for (let index = 0; index < parts.length; index++) {
    const part = parts[index];
    if (part.includes(".")) {
      if (!allowIpv4 || index !== parts.length - 1) return null;
      const bytes = parseIpv4(part);
      if (!bytes) return null;
      words.push((bytes[0] << 8) | bytes[1], (bytes[2] << 8) | bytes[3]);
    } else {
      if (!/^[0-9a-f]{1,4}$/u.test(part)) return null;
      words.push(Number.parseInt(part, 16));
    }
  }
  return words;
}
