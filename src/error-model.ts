// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * The error model: a canonical code, an open reason, and typed details.
 *
 * Every EXCEPTION batch carries three layers, adopted from gRPC's
 * `google.rpc.Status` (WIRE_PROTOCOL.md §8):
 *
 * | Layer   | Wire key                | Set |
 * |---------|-------------------------|-----|
 * | Code    | `vgi_rpc.error_code`    | **Closed**: gRPC's sixteen codes minus `OK`, sent by *name* |
 * | Reason  | `vgi_rpc.error_kind`    | Open, unique within the raising protocol |
 * | Details | `vgi_rpc.error_details` | A JSON array of typed objects from a fixed catalog |
 *
 * The details array is capped at 4 KiB of UTF-8 and dropped *whole* when over,
 * never truncated: a client cannot tell a partial list from a complete one.
 *
 * Servers throw a {@link StatusError} (or any error carrying `errorCode` /
 * `errorKind` / `errorDetails`); clients read `RpcError.errorCode` and
 * friends, plus the typed accessors.
 *
 * Runtime-agnostic: no node-only imports, so it is safe in the workerd bundle.
 */

/** Cap on the serialized `vgi_rpc.error_details` value, in UTF-8 bytes. */
export const MAX_ERROR_DETAILS_BYTES = 4096;

/** The closed set of canonical codes, by name. gRPC's sixteen, minus `OK`. */
export const ERROR_CODES = [
  "CANCELLED",
  "UNKNOWN",
  "INVALID_ARGUMENT",
  "DEADLINE_EXCEEDED",
  "NOT_FOUND",
  "ALREADY_EXISTS",
  "PERMISSION_DENIED",
  "RESOURCE_EXHAUSTED",
  "FAILED_PRECONDITION",
  "ABORTED",
  "OUT_OF_RANGE",
  "UNIMPLEMENTED",
  "INTERNAL",
  "UNAVAILABLE",
  "DATA_LOSS",
  "UNAUTHENTICATED",
] as const;

/** One canonical code name. The wire value is the name, never a number. */
export type ErrorCode = (typeof ERROR_CODES)[number];

const CODE_SET: ReadonlySet<string> = new Set(ERROR_CODES);

/** Whether `value` is one of the sixteen canonical code names. */
export function isErrorCode(value: unknown): value is ErrorCode {
  return typeof value === "string" && CODE_SET.has(value);
}

/** Read a wire value as a code; anything unrecognised reads as `UNKNOWN`. */
export function parseErrorCode(value: unknown): ErrorCode {
  return isErrorCode(value) ? value : "UNKNOWN";
}

// ---------------------------------------------------------------------------
// The detail catalog
// ---------------------------------------------------------------------------

/** A detail as it travels: a JSON object naming its type in `@type`. */
export type ErrorDetailJson = {
  /** The detail's type name: a catalog `vgi_rpc.*` name, or one under a protocol's own name. */
  "@type": string;
  [key: string]: unknown;
};

/** `vgi_rpc.ErrorInfo` -- extra context for the reason. */
export interface ErrorInfo {
  /** The detail's type name on the wire. */
  readonly "@type": "vgi_rpc.ErrorInfo";
  /** String-to-string context. Never credentials or user data. */
  readonly metadata: Readonly<Record<string, string>>;
}
/** `vgi_rpc.RetryInfo` -- how long to wait before retrying. */
export interface RetryInfo {
  /** The detail's type name on the wire. */
  readonly "@type": "vgi_rpc.RetryInfo";
  /** Seconds to wait; a retry waits at least this long. Finite, non-negative. */
  readonly retry_delay_seconds: number;
}
/** One wrong input. */
export interface FieldViolation {
  /** The input that was wrong. */
  readonly field: string;
  /** What went wrong, in developer-facing English. */
  readonly description: string;
}
/** `vgi_rpc.BadRequest` -- which inputs were wrong. */
export interface BadRequest {
  /** The detail's type name on the wire. */
  readonly "@type": "vgi_rpc.BadRequest";
  /** One entry per wrong input. */
  readonly field_violations: readonly FieldViolation[];
}
/** One unmet precondition. */
export interface PreconditionViolation {
  /** The kind of precondition, e.g. `"protocol_version"`. */
  readonly type: string;
  /** What the violation concerns, e.g. a protocol or quota name. */
  readonly subject: string;
  /** What went wrong, in developer-facing English. */
  readonly description: string;
}
/** `vgi_rpc.PreconditionFailure` -- what state must change first. */
export interface PreconditionFailure {
  /** The detail's type name on the wire. */
  readonly "@type": "vgi_rpc.PreconditionFailure";
  /** One entry per violation. */
  readonly violations: readonly PreconditionViolation[];
}
/** One exhausted limit. */
export interface QuotaViolation {
  /** What the violation concerns, e.g. a protocol or quota name. */
  readonly subject: string;
  /** What went wrong, in developer-facing English. */
  readonly description: string;
}
/** `vgi_rpc.QuotaFailure` -- which limit was hit. */
export interface QuotaFailure {
  /** The detail's type name on the wire. */
  readonly "@type": "vgi_rpc.QuotaFailure";
  /** One entry per violation. */
  readonly violations: readonly QuotaViolation[];
}
/** `vgi_rpc.ResourceInfo` -- which object the error concerns. */
export interface ResourceInfo {
  /** The detail's type name on the wire. */
  readonly "@type": "vgi_rpc.ResourceInfo";
  /** The kind of resource, e.g. `"report"`. */
  readonly resource_type: string;
  /** Its name or identifier. */
  readonly resource_name: string;
  /** Its owner, when meaningful. */
  readonly owner: string;
  /** What went wrong, in developer-facing English. */
  readonly description: string;
}
/** One documentation pointer. */
export interface HelpLink {
  /** What went wrong, in developer-facing English. */
  readonly description: string;
  /** Where to read more. */
  readonly url: string;
}
/** `vgi_rpc.Help` -- where to read more. */
export interface Help {
  /** The detail's type name on the wire. */
  readonly "@type": "vgi_rpc.Help";
  /** Documentation pointers. */
  readonly links: readonly HelpLink[];
}
/** `vgi_rpc.LocalizedMessage` -- text safe to show an end user. */
export interface LocalizedMessage {
  /** The detail's type name on the wire. */
  readonly "@type": "vgi_rpc.LocalizedMessage";
  /** BCP 47 tag, e.g. `"en-US"`. */
  readonly locale: string;
  /** Text safe to show an end user. */
  readonly message: string;
}

/** Any member of the fixed detail catalog. */
export type ErrorDetail =
  | ErrorInfo
  | RetryInfo
  | BadRequest
  | PreconditionFailure
  | QuotaFailure
  | ResourceInfo
  | Help
  | LocalizedMessage;

/** Build a `vgi_rpc.RetryInfo`. */
export function retryInfo(retryDelaySeconds: number): RetryInfo {
  return { "@type": "vgi_rpc.RetryInfo", retry_delay_seconds: retryDelaySeconds };
}
/** Build a `vgi_rpc.ErrorInfo`. */
export function errorInfo(metadata: Record<string, string>): ErrorInfo {
  return { "@type": "vgi_rpc.ErrorInfo", metadata: { ...metadata } };
}
/** Build a `vgi_rpc.BadRequest`. */
export function badRequest(fieldViolations: readonly FieldViolation[]): BadRequest {
  return { "@type": "vgi_rpc.BadRequest", field_violations: fieldViolations.map((v) => ({ ...v })) };
}
/** Build a `vgi_rpc.PreconditionFailure`. */
export function preconditionFailure(violations: readonly PreconditionViolation[]): PreconditionFailure {
  return { "@type": "vgi_rpc.PreconditionFailure", violations: violations.map((v) => ({ ...v })) };
}

const RESERVED_DETAIL_PREFIX = "vgi_rpc.";

class DetailShapeError extends Error {}

function str(obj: Record<string, unknown>, key: string): string {
  const value = obj[key] ?? "";
  if (typeof value !== "string") throw new DetailShapeError(`'${key}' must be a string`);
  return value;
}

function objects(obj: Record<string, unknown>, key: string): Record<string, unknown>[] {
  const value = obj[key] ?? [];
  if (!Array.isArray(value) || !value.every(isPlainObject)) {
    throw new DetailShapeError(`'${key}' must be an array of objects`);
  }
  return value as Record<string, unknown>[];
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Parsers for the catalog. Each throws `DetailShapeError` on a malformed field. */
const CATALOG: Readonly<Record<string, (obj: Record<string, unknown>) => ErrorDetail>> = {
  "vgi_rpc.ErrorInfo": (obj) => {
    const raw = obj.metadata ?? {};
    if (!isPlainObject(raw) || !Object.values(raw).every((v) => typeof v === "string")) {
      throw new DetailShapeError("'metadata' must be an object of strings");
    }
    return { "@type": "vgi_rpc.ErrorInfo", metadata: { ...(raw as Record<string, string>) } };
  },
  "vgi_rpc.RetryInfo": (obj) => {
    const raw = obj.retry_delay_seconds;
    if (typeof raw !== "number" || !Number.isFinite(raw) || raw < 0) {
      throw new DetailShapeError("'retry_delay_seconds' must be a finite, non-negative number");
    }
    return retryInfo(raw);
  },
  "vgi_rpc.BadRequest": (obj) => ({
    "@type": "vgi_rpc.BadRequest",
    field_violations: objects(obj, "field_violations").map((v) => ({
      field: str(v, "field"),
      description: str(v, "description"),
    })),
  }),
  "vgi_rpc.PreconditionFailure": (obj) => ({
    "@type": "vgi_rpc.PreconditionFailure",
    violations: objects(obj, "violations").map((v) => ({
      type: str(v, "type"),
      subject: str(v, "subject"),
      description: str(v, "description"),
    })),
  }),
  "vgi_rpc.QuotaFailure": (obj) => ({
    "@type": "vgi_rpc.QuotaFailure",
    violations: objects(obj, "violations").map((v) => ({
      subject: str(v, "subject"),
      description: str(v, "description"),
    })),
  }),
  "vgi_rpc.ResourceInfo": (obj) => ({
    "@type": "vgi_rpc.ResourceInfo",
    resource_type: str(obj, "resource_type"),
    resource_name: str(obj, "resource_name"),
    owner: str(obj, "owner"),
    description: str(obj, "description"),
  }),
  "vgi_rpc.Help": (obj) => ({
    "@type": "vgi_rpc.Help",
    links: objects(obj, "links").map((v) => ({ description: str(v, "description"), url: str(v, "url") })),
  }),
  "vgi_rpc.LocalizedMessage": (obj) => ({
    "@type": "vgi_rpc.LocalizedMessage",
    locale: str(obj, "locale"),
    message: str(obj, "message"),
  }),
};

/**
 * Decode one detail object, or `null` when it is unknown or malformed.
 *
 * Clients ignore types they do not know, and a malformed known type is
 * treated as absent rather than failing the error it rides on: the error is
 * the news, the detail is commentary.
 */
export function parseErrorDetail(obj: unknown): ErrorDetail | null {
  if (!isPlainObject(obj)) return null;
  const type = obj["@type"];
  if (typeof type !== "string" || !Object.hasOwn(CATALOG, type)) return null;
  try {
    return CATALOG[type](obj);
  } catch (e) {
    if (e instanceof DetailShapeError) return null;
    throw e;
  }
}

/** Throw unless `objs` obeys the catalog rules: typed, unique, qualified,
 *  and never an invented `vgi_rpc.*` type. */
function validateDetails(objs: readonly Record<string, unknown>[]): void {
  const seen = new Set<string>();
  for (const obj of objs) {
    const type = obj["@type"];
    if (typeof type !== "string" || type === "") {
      throw new DetailShapeError("every error detail must name its type in '@type'");
    }
    if (seen.has(type)) throw new DetailShapeError(`error detail type '${type}' appears more than once`);
    seen.add(type);
    if (type.startsWith(RESERVED_DETAIL_PREFIX) && !Object.hasOwn(CATALOG, type)) {
      throw new DetailShapeError(
        `'${type}' claims the reserved 'vgi_rpc.' prefix but is not in the catalog. ` +
          "A protocol-defined detail type must live under its own protocol's name.",
      );
    }
    if (!type.includes(".")) {
      throw new DetailShapeError(`'${type}' is not qualified; protocol-defined types live under the protocol's name`);
    }
  }
}

/**
 * Serialize a detail list for `vgi_rpc.error_details`, enforcing the rules.
 *
 * Returns `null` -- meaning *omit the key* -- for an empty list, for a list
 * breaking a catalog rule, and for one whose serialized form exceeds
 * {@link MAX_ERROR_DETAILS_BYTES}. Dropped whole, never trimmed.
 */
export function encodeErrorDetails(details: readonly unknown[]): string | null {
  if (details.length === 0) return null;
  if (!details.every(isPlainObject)) return null;
  const objs = details as Record<string, unknown>[];
  let text: string;
  try {
    validateDetails(objs);
    // JSON.stringify writes NaN/Infinity as `null` rather than failing; a
    // detail carrying one is malformed, so it is refused rather than sent as a
    // value its author did not write.
    if (!allFinite(objs)) return null;
    text = JSON.stringify(objs);
  } catch {
    return null;
  }
  if (new TextEncoder().encode(text).length > MAX_ERROR_DETAILS_BYTES) return null;
  return text;
}

function allFinite(value: unknown): boolean {
  if (typeof value === "number") return Number.isFinite(value);
  if (Array.isArray(value)) return value.every(allFinite);
  if (isPlainObject(value)) return Object.values(value).every(allFinite);
  return true;
}

/**
 * Decode a `vgi_rpc.error_details` value into its JSON objects.
 *
 * Tolerant: anything that is not a JSON array decodes as empty and
 * non-object elements are skipped. Unknown `@type` values are **kept** --
 * filtering to the catalog is what the typed accessors do.
 */
export function decodeErrorDetails(raw: string | null | undefined): ErrorDetailJson[] {
  if (raw == null) return [];
  let decoded: unknown;
  try {
    decoded = JSON.parse(raw);
  } catch {
    return [];
  }
  return detailObjects(decoded);
}

/** Keep the object elements of an already-decoded array (e.g. the `log_extra` mirror). */
export function detailObjects(value: unknown): ErrorDetailJson[] {
  if (!Array.isArray(value)) return [];
  return value.filter(isPlainObject) as ErrorDetailJson[];
}

/**
 * Whether WIRE_PROTOCOL.md §8 calls an error retryable.
 *
 * `UNAVAILABLE` always; `RESOURCE_EXHAUSTED` only when it carries `RetryInfo`.
 * Everything else is final -- `ABORTED` included, which means "retry the whole
 * operation at a higher level". A classification, not a policy: nothing in
 * this package retries an RPC error automatically.
 */
export function isRetryable(code: string, details: readonly unknown[] = []): boolean {
  const parsed = isErrorCode(code) ? code : "UNKNOWN";
  if (parsed === "UNAVAILABLE") return true;
  if (parsed === "RESOURCE_EXHAUSTED") {
    return details.some((d) => parseErrorDetail(d)?.["@type"] === "vgi_rpc.RetryInfo");
  }
  return false;
}

// ---------------------------------------------------------------------------
// Reading the model off a thrown value
// ---------------------------------------------------------------------------

function attribute(error: unknown, key: string): unknown {
  if (error === null || (typeof error !== "object" && typeof error !== "function")) return undefined;
  const own = (error as Record<string, unknown>)[key];
  if (own !== undefined) return own;
  return (error as { constructor?: Record<string, unknown> }).constructor?.[key];
}

/** The canonical code an error declares (`errorCode`, instance or static), or `UNKNOWN`. */
export function errorCodeOf(error: unknown): ErrorCode {
  return parseErrorCode(attribute(error, "errorCode"));
}

/** The non-empty `errorKind` an error declares (instance or static), or `null`. */
export function errorKindOf(error: unknown): string | null {
  const kind = attribute(error, "errorKind");
  return typeof kind === "string" && kind.length > 0 ? kind : null;
}

/** The detail objects an error declares (`errorDetails`). Never throws. */
export function errorDetailsOf(error: unknown): Record<string, unknown>[] {
  try {
    const raw = attribute(error, "errorDetails");
    if (!Array.isArray(raw)) return [];
    return raw.filter(isPlainObject).map((d) => ({ ...d }));
  } catch {
    return [];
  }
}

/** Options for {@link StatusError}. */
export interface StatusErrorOptions {
  /** One of the sixteen canonical codes. */
  code: ErrorCode;
  /** The reason a client branches on, unique within the raising protocol. */
  kind?: string;
  /** Catalog details, at most one of each type. Protocol-defined types live
   *  under the protocol's own name. */
  details?: readonly (ErrorDetail | ErrorDetailJson)[];
}

/**
 * An application error carrying the full error model.
 *
 * ```ts
 * throw new StatusError("report is being rebuilt", {
 *   code: "UNAVAILABLE",
 *   kind: "report_rebuilding",
 *   details: [retryInfo(30)],
 * });
 * ```
 *
 * Any error class may instead declare `errorCode` / `errorKind` /
 * `errorDetails`; this is the convenience for when a dedicated class would add
 * nothing. Details are validated eagerly, so a rule violation fails where it
 * was made rather than being silently dropped on the way out.
 */
export class StatusError extends Error {
  /** Canonical code hoisted as `vgi_rpc.error_code` (WIRE_PROTOCOL.md §8). */
  readonly errorCode: ErrorCode;
  /** The reason hoisted as `vgi_rpc.error_kind`, when set. */
  readonly errorKind: string | undefined;
  /** Catalog details, emitted as `vgi_rpc.error_details` when within the 4 KiB cap. */
  readonly errorDetails: readonly Record<string, unknown>[];

  constructor(message: string, options: StatusErrorOptions) {
    super(message);
    this.name = "StatusError";
    if (!isErrorCode(options.code)) {
      throw new TypeError(`'${String(options.code)}' is not a canonical error code`);
    }
    this.errorCode = options.code;
    this.errorKind = options.kind || undefined;
    const objs = (options.details ?? []).map((d) => ({ ...(d as Record<string, unknown>) }));
    try {
      validateDetails(objs);
    } catch (e) {
      throw new TypeError((e as Error).message);
    }
    this.errorDetails = objs;
  }
}
