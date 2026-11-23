// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * The per-process object cache (`STATE_REF_V1.md` §11).
 *
 * Stored {@link StateRef} values and argument references both name an object
 * in storage by its `object_id`. Resolving one fetches the object, verifies
 * and opens it; this cache keeps the result so the next turn -- or a parallel
 * scan of the same query -- does not fetch again.
 *
 * The rules, all required:
 *
 * 1. **Keyed by `object_id`, never by a digest.** Grants mint objects for
 *    claimed hashes without seeing the bytes, so a digest-keyed cache would
 *    hand one principal's value to another who merely knows its hash.
 * 2. **Single-flight.** Concurrent resolves of one `object_id` share one fetch.
 * 3. **Byte-bounded LRU.** `VGI_RPC_OBJECT_CACHE_BYTES`, default
 *    `min(1 GiB, 40% of the cgroup memory limit)` (or of physical memory). A
 *    value larger than the bound is returned but not retained, with a warning.
 * 4. **Memos hold the `object_id`, never the value**, so resident bytes stay
 *    bounded by this cache alone.
 * 5. **`fetchedAt`.** A hit on an entry fetched more than one day ago is a
 *    miss: the bucket lifecycle may delete the object after a day.
 *
 * Nothing here is visible on the wire.
 */

import { emitStateRefEvent } from "./state-ref-events.js";

/** Environment variable overriding the cache's byte bound. */
export const OBJECT_CACHE_BYTES_ENV = "VGI_RPC_OBJECT_CACHE_BYTES";
/** An entry fetched longer ago than this is treated as a miss (one day). */
export const OBJECT_CACHE_MAX_AGE_SECONDS = 86400;

const ONE_GIB = 1024 * 1024 * 1024;

type EnvLike = Record<string, string | undefined>;

function processEnv(): EnvLike {
  return (globalThis as { process?: { env?: EnvLike } }).process?.env ?? {};
}

function nodeRequire(): ((id: string) => any) | null {
  return (import.meta as any).require ?? (globalThis as any).require ?? null;
}

/** The cgroup (v2, then v1) memory limit in bytes, or `null` when unlimited/unknown. */
function cgroupMemoryLimit(): number | null {
  const req = nodeRequire();
  if (!req) return null;
  let fs: any;
  try {
    fs = req("node:fs");
  } catch {
    return null;
  }
  for (const path of ["/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"]) {
    let raw: string;
    try {
      raw = String(fs.readFileSync(path, "utf8")).trim();
    } catch {
      continue;
    }
    if (!raw || raw === "max") return null;
    const value = Number(raw);
    if (!Number.isFinite(value)) continue;
    // cgroup v1 reports "unlimited" as a huge page-aligned number.
    if (value <= 0 || value >= 2 ** 60) return null;
    return value;
  }
  return null;
}

function physicalMemory(): number | null {
  const req = nodeRequire();
  if (!req) return null;
  try {
    const total = Number(req("node:os").totalmem());
    return Number.isFinite(total) && total > 0 ? total : null;
  } catch {
    return null;
  }
}

/**
 * The cache bound: `VGI_RPC_OBJECT_CACHE_BYTES`, else `min(1 GiB, 40% of memory)`.
 *
 * @throws Error when the variable is set to anything but a non-negative integer.
 */
export function defaultObjectCacheBytes(env?: EnvLike): number {
  const source = env ?? processEnv();
  const raw = source[OBJECT_CACHE_BYTES_ENV];
  if (raw !== undefined && raw.trim() !== "") {
    const text = raw.trim();
    const value = Number(text);
    if (!/^\d+$/.test(text) || !Number.isSafeInteger(value)) {
      throw new Error(`${OBJECT_CACHE_BYTES_ENV} must be a non-negative integer, got ${JSON.stringify(raw)}`);
    }
    return value;
  }
  const memory = cgroupMemoryLimit() ?? physicalMemory();
  if (memory === null) return ONE_GIB;
  return Math.min(ONE_GIB, Math.floor((memory * 40) / 100));
}

/** A snapshot of the cache's counters. */
export interface ObjectCacheStats {
  /** Bytes of values currently retained. */
  residentBytes: number;
  /** Number of retained values. */
  entries: number;
  /** The byte bound. */
  maxBytes: number;
  /** Lookups answered from the cache. */
  hits: number;
  /** Lookups that fetched (including aged-out entries and single-flight joins). */
  misses: number;
  /** Entries dropped to stay under the bound or aged out. */
  evictions: number;
  /** Fetches actually performed (single-flight joins excluded). */
  fetches: number;
}

/** The result of {@link ObjectCache.getOrFetch}. */
export interface ObjectCacheLookup {
  /** The verified value. */
  value: Uint8Array;
  /** True when the value came from the cache rather than a fetch. */
  hit: boolean;
}

/** Options for {@link ObjectCache}. */
export interface ObjectCacheOptions {
  /** Entries fetched longer ago than this (seconds) are misses. Default one day. */
  maxAgeSeconds?: number;
  /** Wall clock in seconds, injectable for tests. Default `Date.now() / 1000`. */
  clock?: () => number;
}

/**
 * Byte-bounded, single-flight LRU of verified object values keyed by `object_id`.
 *
 * Values are the verified, opened, decompressed bytes the caller's `fetch`
 * returns; the cache never inspects them.
 */
export class ObjectCache {
  private readonly _maxBytes: number;
  private readonly maxAge: number;
  private readonly clock: () => number;
  /** Insertion order is LRU order: a hit re-inserts at the end. */
  private readonly entries = new Map<string, { value: Uint8Array; fetchedAt: number }>();
  private readonly flights = new Map<string, Promise<Uint8Array>>();
  private resident = 0;
  private hits = 0;
  private misses = 0;
  private evictions = 0;
  private fetches = 0;

  /**
   * @param maxBytes Byte bound on retained values. `0` retains nothing (every
   *   lookup fetches, still single-flight).
   */
  constructor(maxBytes: number, options: ObjectCacheOptions = {}) {
    if (!Number.isSafeInteger(maxBytes) || maxBytes < 0) {
      throw new Error(`object cache bound must be a non-negative integer, got ${maxBytes}`);
    }
    this._maxBytes = maxBytes;
    this.maxAge = options.maxAgeSeconds ?? OBJECT_CACHE_MAX_AGE_SECONDS;
    this.clock = options.clock ?? (() => Date.now() / 1000);
  }

  /** The byte bound. */
  get maxBytes(): number {
    return this._maxBytes;
  }

  /** A snapshot of the counters. */
  stats(): ObjectCacheStats {
    return {
      residentBytes: this.resident,
      entries: this.entries.size,
      maxBytes: this._maxBytes,
      hits: this.hits,
      misses: this.misses,
      evictions: this.evictions,
      fetches: this.fetches,
    };
  }

  /** Drop every entry (tests, and explicit operator reset). */
  clear(): void {
    this.entries.clear();
    this.resident = 0;
  }

  /**
   * Shift every entry's `fetchedAt` back by `seconds` (test hook): proves the
   * one-day age rule without waiting a day. Returns how many entries moved.
   */
  ageEntries(seconds: number): number {
    for (const entry of this.entries.values()) entry.fetchedAt -= seconds;
    return this.entries.size;
  }

  /** Whether a fresh entry for `objectId` is retained (no counters change). */
  contains(objectId: string): boolean {
    const entry = this.entries.get(objectId);
    return entry !== undefined && this.clock() - entry.fetchedAt <= this.maxAge;
  }

  /**
   * Return the value for `objectId`, fetching it at most once across
   * concurrent callers.
   *
   * @param fetch Called on a miss to produce the verified value. Exactly one
   *   caller runs it per miss; concurrent callers share its result or error.
   * @returns `{ value, hit }`; `hit` is true when the value came from the cache.
   */
  async getOrFetch(objectId: string, fetch: () => Promise<Uint8Array>): Promise<ObjectCacheLookup> {
    const entry = this.entries.get(objectId);
    if (entry !== undefined) {
      if (this.clock() - entry.fetchedAt <= this.maxAge) {
        this.entries.delete(objectId);
        this.entries.set(objectId, entry);
        this.hits += 1;
        emitStateRefEvent("object_cache", { result: "hit", resident_bytes: this.resident });
        return { value: entry.value, hit: true };
      }
      // Aged out: the object may already be gone; never serve it.
      this.entries.delete(objectId);
      this.resident -= entry.value.byteLength;
      this.evictions += 1;
    }
    this.misses += 1;
    const pending = this.flights.get(objectId);
    if (pending !== undefined) {
      return { value: await pending, hit: false };
    }
    this.fetches += 1;
    emitStateRefEvent("object_cache", { result: "miss", resident_bytes: this.resident });
    const flight = (async () => {
      try {
        const value = await fetch();
        this.insert(objectId, value);
        return value;
      } finally {
        this.flights.delete(objectId);
      }
    })();
    this.flights.set(objectId, flight);
    return { value: await flight, hit: false };
  }

  private insert(objectId: string, value: Uint8Array): void {
    const size = value.byteLength;
    if (size > this._maxBytes) {
      emitStateRefEvent(
        "object_cache",
        {
          result: "not_retained",
          bytes: size,
          max_bytes: this._maxBytes,
          message: `object_cache: a ${size}-byte value exceeds the cache bound (${this._maxBytes} bytes, ${OBJECT_CACHE_BYTES_ENV}); resolved but not retained`,
        },
        "warn",
      );
      return;
    }
    const previous = this.entries.get(objectId);
    if (previous !== undefined) {
      this.entries.delete(objectId);
      this.resident -= previous.value.byteLength;
    }
    this.entries.set(objectId, { value, fetchedAt: this.clock() });
    this.resident += size;
    while (this.resident > this._maxBytes && this.entries.size > 0) {
      const [oldestId, oldest] = this.entries.entries().next().value as [string, { value: Uint8Array }];
      this.entries.delete(oldestId);
      this.resident -= oldest.value.byteLength;
      this.evictions += 1;
      emitStateRefEvent("object_cache", { result: "evict", resident_bytes: this.resident });
    }
  }
}

let defaultCache: ObjectCache | null = null;

/**
 * The process-wide object cache, created on first use with its bound read
 * from `VGI_RPC_OBJECT_CACHE_BYTES` (or the memory default).
 */
export function defaultObjectCache(): ObjectCache {
  if (defaultCache === null) defaultCache = new ObjectCache(defaultObjectCacheBytes());
  return defaultCache;
}
