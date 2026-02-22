// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import {
  Schema,
  Field,
  DataType,
  Utf8,
  Binary,
  Int64,
  Int32,
  Int16,
  Float64,
  Float32,
  Bool,
} from "apache-arrow";

// ---------------------------------------------------------------------------
// Convenient DataType singletons — re-export so users avoid arrow imports
// ---------------------------------------------------------------------------

/** Apache Arrow Utf8 type. Use as schema shorthand: `{ name: str }` */
export const str = new Utf8();
/** Apache Arrow Binary type. Use as schema shorthand: `{ data: bytes }` */
export const bytes = new Binary();
/** Apache Arrow Int64 type. Use as schema shorthand: `{ count: int }` */
export const int = new Int64();
/** Apache Arrow Int32 type. Use as schema shorthand: `{ count: int32 }` */
export const int32 = new Int32();
/** Apache Arrow Float64 type. Use as schema shorthand: `{ value: float }` */
export const float = new Float64();
/** Apache Arrow Float32 type. Use as schema shorthand: `{ value: float32 }` */
export const float32 = new Float32();
/** Apache Arrow Bool type. Use as schema shorthand: `{ flag: bool }` */
export const bool = new Bool();

// ---------------------------------------------------------------------------
// SchemaLike — shorthand for declaring schemas
// ---------------------------------------------------------------------------

/**
 * A schema specification that accepts:
 * - A real `Schema` (passed through)
 * - A record mapping field names to `DataType` instances or `Field` instances
 * - An empty `{}` for an empty schema
 */
export type SchemaLike = Schema | Record<string, DataType | Field>;

/**
 * Convert a SchemaLike spec into a real `Schema`.
 *
 * - `Schema` → returned as-is
 * - `Record<string, DataType>` → each entry becomes `new Field(name, type, false)`
 * - `Record<string, Field>` → each entry is passed through
 * - `{}` → `new Schema([])`
 */
export function toSchema(spec: SchemaLike): Schema {
  if (spec instanceof Schema) return spec;

  const fields: Field[] = [];
  for (const [name, value] of Object.entries(spec)) {
    if (value instanceof Field) {
      fields.push(value);
    } else if (value instanceof DataType) {
      fields.push(new Field(name, value, false));
    } else {
      throw new TypeError(
        `Invalid schema value for "${name}": expected DataType or Field, got ${typeof value}`,
      );
    }
  }
  return new Schema(fields);
}

// ---------------------------------------------------------------------------
// inferParamTypes — derive paramTypes from a schema spec
// ---------------------------------------------------------------------------

const TYPE_MAP: [new (...args: any[]) => DataType, string][] = [
  [Utf8, "str"],
  [Binary, "bytes"],
  [Bool, "bool"],
  [Float64, "float"],
  [Float32, "float"],
  [Int64, "int"],
  [Int32, "int"],
  [Int16, "int"],
];

/**
 * Derive a `paramTypes` record from a SchemaLike spec.
 * Maps common Arrow scalar types to Python-style type strings.
 * Returns `undefined` if any field has a complex type (List, Map_, Dictionary, etc.).
 */
export function inferParamTypes(
  spec: SchemaLike,
): Record<string, string> | undefined {
  const schema = toSchema(spec);
  if (schema.fields.length === 0) return undefined;

  const result: Record<string, string> = {};
  for (const field of schema.fields) {
    let mapped: string | undefined;
    for (const [ctor, name] of TYPE_MAP) {
      if (field.type instanceof ctor) {
        mapped = name;
        break;
      }
    }
    if (!mapped) return undefined;
    result[field.name] = mapped;
  }
  return result;
}
