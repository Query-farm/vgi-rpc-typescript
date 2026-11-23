// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Observability events for `StateRef`, argument references and the object
 * cache (`STATE_REF_V1.md` §14): `state_ref.store`, `state_ref.resolve`,
 * `arguments_ref.mint`, `arguments_ref.resolve`, `object_cache`,
 * `cursor_large` and `cursor_too_large`.
 *
 * Fields are sizes, forms and outcomes only: never tag bytes, values, digests
 * of client data, or object ids beside principals.
 *
 * The default sink prints warnings with `console.warn` and drops the
 * debug-level events; install your own with {@link setStateRefEventSink} to
 * forward them to a logger or metrics.
 */

/** Severity of a {@link StateRefEvent}. */
export type StateRefEventLevel = "debug" | "warn";

/** One observability event. */
export interface StateRefEvent {
  /** Event name, e.g. `state_ref.store` or `cursor_large`. */
  event: string;
  /** Severity. */
  level: StateRefEventLevel;
  /** Structured fields (sizes, forms, outcomes; a `message` for warnings). */
  fields: Readonly<Record<string, string | number | boolean>>;
}

/** Receives every {@link StateRefEvent}. Must not throw. */
export type StateRefEventSink = (event: StateRefEvent) => void;

const defaultSink: StateRefEventSink = (event) => {
  if (event.level !== "warn") return;
  const message = typeof event.fields.message === "string" ? event.fields.message : event.event;
  console.warn(`[vgi-rpc] ${message}`);
};

let sink: StateRefEventSink = defaultSink;

/** Install the sink for `StateRef` / object-cache events; `null` restores the default. */
export function setStateRefEventSink(next: StateRefEventSink | null): void {
  sink = next ?? defaultSink;
}

/** @internal Emit one event; observability never fails the caller. */
export function emitStateRefEvent(
  event: string,
  fields: Record<string, string | number | boolean>,
  level: StateRefEventLevel = "debug",
): void {
  try {
    sink({ event, level, fields });
  } catch {
    // best-effort
  }
}
