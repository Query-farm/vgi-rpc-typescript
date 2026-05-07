// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import {
  binary,
  bool as boolType,
  float32 as float32Type,
  float64 as float64Type,
  int8 as int8Type,
  int16 as int16Type,
  int32 as int32Type,
  int64 as int64Type,
  isBinary,
  isBool,
  isFloat,
  isInt,
  isUtf8,
  field as makeField,
  schema as makeSchema,
  uint8 as uint8Type,
  uint16 as uint16Type,
  uint32 as uint32Type,
  uint64 as uint64Type,
  utf8,
  type VgiDataType,
  type VgiField,
  type VgiSchema,
} from "./arrow/index.js";

// ---------------------------------------------------------------------------
// Convenient DataType singletons — re-export so users avoid arrow imports
// ---------------------------------------------------------------------------

/** Apache Arrow Utf8 type. Use as schema shorthand: `{ name: str }` */
export const str = utf8();
/** Apache Arrow Binary type. Use as schema shorthand: `{ data: bytes }` */
export const bytes = binary();
/** Apache Arrow Int64 type. Use as schema shorthand: `{ count: int }` */
export const int = int64Type();
/** Apache Arrow Int32 type. Use as schema shorthand: `{ count: int32 }` */
export const int32 = int32Type();
/** Apache Arrow Int16 type. Use as schema shorthand: `{ count: int16 }` */
export const int16 = int16Type();
/** Apache Arrow Int8 type. Use as schema shorthand: `{ count: int8 }` */
export const int8 = int8Type();
/** Apache Arrow Uint8 type. Use as schema shorthand: `{ count: uint8 }` */
export const uint8 = uint8Type();
/** Apache Arrow Uint16 type. Use as schema shorthand: `{ count: uint16 }` */
export const uint16 = uint16Type();
/** Apache Arrow Uint32 type. Use as schema shorthand: `{ count: uint32 }` */
export const uint32 = uint32Type();
/** Apache Arrow Uint64 type. Use as schema shorthand: `{ count: uint64 }` */
export const uint64 = uint64Type();
/** Apache Arrow Float64 type. Use as schema shorthand: `{ value: float }` */
export const float = float64Type();
/** Apache Arrow Float32 type. Use as schema shorthand: `{ value: float32 }` */
export const float32 = float32Type();
/** Apache Arrow Bool type. Use as schema shorthand: `{ flag: bool }` */
export const bool = boolType();

// ---------------------------------------------------------------------------
// SchemaLike — shorthand for declaring schemas
// ---------------------------------------------------------------------------

/**
 * Structural minimum that any backend's Schema must satisfy. arrow-js's
 * `Schema`, vgi-typescript's `VgiSchema`, and flechette's `Schema` all match
 * this shape. Used so vgi-rpc consumers don't have to know which Arrow
 * library is on the other side of the wire.
 *
 * Kept exported for backwards compatibility — equivalent to `VgiSchema`.
 */
export interface SchemaShape {
  readonly fields: ReadonlyArray<{
    readonly name: string;
    readonly type: { readonly typeId: number };
    readonly nullable?: boolean;
    readonly metadata?: Map<string, string>;
  }>;
  readonly metadata?: Map<string, string> | null;
}

/**
 * A schema specification that accepts:
 * - A real `VgiSchema` (passed through)
 * - Anything structurally `SchemaShape`
 * - A record mapping field names to `VgiDataType` instances or `VgiField` instances
 * - An empty `{}` for an empty schema
 */
export type SchemaLike = VgiSchema | SchemaShape | Record<string, VgiDataType | VgiField>;

function isField(x: unknown): x is VgiField {
  return (
    x != null &&
    typeof (x as any).name === "string" &&
    (x as any).type != null &&
    typeof (x as any).nullable === "boolean"
  );
}

function isDataType(x: unknown): x is VgiDataType {
  return x != null && typeof (x as any).typeId === "number";
}

/**
 * Convert a SchemaLike spec into a real `VgiSchema`.
 */
export function toSchema(spec: SchemaLike): VgiSchema {
  // VgiSchema / SchemaShape branch: anything with a `fields` array.
  const maybeFields = (spec as { fields?: unknown }).fields;
  if (Array.isArray(maybeFields)) {
    const out: VgiField[] = [];
    for (const f of maybeFields as ReadonlyArray<any>) {
      if (isField(f)) {
        out.push(f);
      } else {
        out.push(makeField(f.name, f.type, f.nullable ?? true, f.metadata));
      }
    }
    return makeSchema(out);
  }

  const fields: VgiField[] = [];
  for (const [name, value] of Object.entries(spec)) {
    if (isField(value)) {
      fields.push(value);
    } else if (isDataType(value)) {
      fields.push(makeField(name, value, false));
    } else {
      throw new TypeError(`Invalid schema value for "${name}": expected DataType or Field, got ${typeof value}`);
    }
  }
  return makeSchema(fields);
}

// ---------------------------------------------------------------------------
// inferParamTypes — derive paramTypes from a schema spec
// ---------------------------------------------------------------------------

/**
 * Derive a `paramTypes` record from a SchemaLike spec.
 * Maps common Arrow scalar types to Python-style type strings.
 * Returns `undefined` if any field has a complex type (List, Map_, Dictionary, etc.).
 */
export function inferParamTypes(spec: SchemaLike): Record<string, string> | undefined {
  const sch = toSchema(spec);
  if (sch.fields.length === 0) return undefined;

  const result: Record<string, string> = {};
  for (const f of sch.fields) {
    let mapped: string | undefined;
    if (isUtf8(f.type)) mapped = "str";
    else if (isBinary(f.type)) mapped = "bytes";
    else if (isBool(f.type)) mapped = "bool";
    else if (isFloat(f.type)) mapped = "float";
    else if (isInt(f.type)) mapped = "int";
    if (!mapped) return undefined;
    result[f.name] = mapped;
  }
  return result;
}
