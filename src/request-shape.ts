// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Describe a request batch for the access log without exposing a value.
 *
 * The access log used to carry the whole request as base64 Arrow IPC
 * (`request_data`) at DEBUG. The framework cannot know which parameters are
 * secret -- a VGI `catalog_attach` carries API keys and passwords in its
 * options -- so any payload in a log is a credential leak waiting for someone
 * to turn DEBUG on. A record now says only what the request *looked like*:
 * parameter names, Arrow types and the row count (`request_fields`,
 * `request_rows`); its size is already `request_bytes`.
 *
 * Deliberately no digest of the bytes either: a hash of a payload whose other
 * fields are known is a brute-force oracle for a short secret.
 */

import type { VgiSchema } from "./arrow/types.js";
import { typeToken } from "./type-tokens.js";
import type { AccessLogRequestField } from "./types.js";

/** Names and type tokens of `schema`'s fields, plus `rows`. */
export function requestShape(
  schema: VgiSchema,
  rows: number,
): { requestFields: AccessLogRequestField[]; requestRows: number } {
  return {
    requestFields: schema.fields.map((f) => ({ name: f.name, type: typeName(f.type) })),
    requestRows: rows,
  };
}

function typeName(type: VgiSchema["fields"][number]["type"]): string {
  try {
    return typeToken(type);
  } catch {
    // A type the canonical token table does not spell. The record still
    // needs a non-empty type, and the backend's own rendering never carries
    // a value.
    const s = String(type);
    return s && s !== "[object Object]" ? s : "unknown";
  }
}
