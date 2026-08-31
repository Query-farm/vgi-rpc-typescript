// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import { makeDrainHandle, SessionRegistry, sessionPrincipalKey, startSessionReaper } from "../../src/http/sticky.js";

describe("sessionPrincipalKey", () => {
  test("anonymous collapses to a single NUL-prefixed sentinel", () => {
    expect(sessionPrincipalKey(false, null, null)).toBe("\u0000anonymous");
    // Domain/principal are ignored when not authenticated.
    expect(sessionPrincipalKey(false, "d", "p")).toBe("\u0000anonymous");
  });

  test("authenticated key is NUL-separated so adjacent fields can't collide", () => {
    // {domain:"a", principal:"b "} vs {domain:"a ", principal:"b"} must differ.
    expect(sessionPrincipalKey(true, "a", "b ")).not.toBe(sessionPrincipalKey(true, "a ", "b"));
  });

  test("dispatch and DELETE paths derive identical keys (regression)", () => {
    // Both call sites in handler.ts share this helper; an open on one path
    // must be found on the other. A space-vs-NUL mismatch previously leaked.
    const fromOpen = sessionPrincipalKey(true, "corp", "alice");
    const fromDelete = sessionPrincipalKey(true, "corp", "alice");
    expect(fromOpen).toBe(fromDelete);
    expect(fromOpen).toBe("corp\u0000alice");
  });

  test("peer evidence partitions otherwise identical principals", () => {
    expect(sessionPrincipalKey(true, "corp", "alice", "binding-a")).not.toBe(
      sessionPrincipalKey(true, "corp", "alice", "binding-b"),
    );
  });
});

describe("SessionRegistry reaper", () => {
  test("makeDrainHandle.shutdown() stops the reaper interval (no leak)", async () => {
    const registry = new SessionRegistry(300);
    let ticks = 0;
    // Stub the work the reaper performs so we can observe it stopping.
    const originalDrainExpired = registry.drainExpired.bind(registry);
    registry.drainExpired = ((now?: number) => {
      ticks++;
      return originalDrainExpired(now);
    }) as typeof registry.drainExpired;

    const stop = startSessionReaper(registry, 5);
    const handle = makeDrainHandle(registry, stop);

    await Bun.sleep(25);
    const ticksBeforeShutdown = ticks;
    expect(ticksBeforeShutdown).toBeGreaterThan(0);

    handle.shutdown();
    const ticksAtShutdown = ticks;

    await Bun.sleep(25);
    // No further ticks after shutdown cleared the interval.
    expect(ticks).toBe(ticksAtShutdown);
    expect(ticksAtShutdown).toBeGreaterThanOrEqual(ticksBeforeShutdown);
  });

  test("shutdown() closes live session state", () => {
    const registry = new SessionRegistry(300);
    let closed = false;
    registry.open({ close: () => (closed = true) }, undefined, sessionPrincipalKey(false, null, null));
    const handle = makeDrainHandle(registry);
    handle.shutdown();
    expect(closed).toBe(true);
    expect(registry.size).toBe(0);
  });
});
