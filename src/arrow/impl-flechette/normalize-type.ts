// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Normalize a (possibly foreign) Arrow DataType into a flechette-native type.
//
// App/worker code routinely builds schemas with concrete arrow-js DataType
// instances (e.g. `new Int64()` from @query-farm/apache-arrow) rather than the
// facade constructors. flechette's `columnFromArray` *tolerates* such foreign
// type objects when reading values, but the resulting `Column.type` is the
// foreign object — and flechette's IPC writer then serializes it incorrectly
// (e.g. Utf8 round-trips to ["", "xy\0\0…"], Int64 throws BigInt→number on a
// stale build). Reconstructing the type from its structural fields via
// flechette's own constructors yields a writer-safe native type.
//
// Both arrow-js and flechette encode `typeId` with the same Arrow Type enum, so
// we switch on it and read the shared structural props (with the one rename
// flechette differs on: Int uses `signed`, arrow-js uses `isSigned`).

import {
  Type,
  field as f_field,
  nullType, bool, int, float, utf8, binary, fixedSizeBinary,
  decimal, date, time, timestamp, duration, interval,
  list, largeList, fixedSizeList, struct, map, dictionary, union,
} from "@uwdata/flechette";

function fField(f: any): any {
  return f_field(f.name, toFlechetteType(f.type), f.nullable ?? true, f.metadata ?? null);
}

/**
 * Rebuild `type` as a flechette-native DataType. Idempotent for types that are
 * already native (reconstructed from the same structural props). Unknown
 * typeIds pass through unchanged as a safety net.
 */
export function toFlechetteType(type: any): any {
  if (type == null) return nullType();
  switch (type.typeId) {
    case Type.Null:
      return nullType();
    case Type.Bool:
      return bool();
    case Type.Int:
      return int(type.bitWidth, type.isSigned ?? type.signed ?? true);
    case Type.Float:
      return float(type.precision);
    case Type.Binary:
    case Type.LargeBinary:
      return binary();
    case Type.Utf8:
    case Type.LargeUtf8:
      return utf8();
    case Type.FixedSizeBinary:
      return fixedSizeBinary(type.byteWidth ?? type.stride);
    case Type.Decimal:
      return decimal(type.precision, type.scale, type.bitWidth ?? 128);
    case Type.Date:
      return date(type.unit);
    case Type.Time:
      return time(type.unit);
    case Type.Timestamp:
      return timestamp(type.unit, type.timezone ?? null);
    case Type.Duration:
      return duration(type.unit);
    case Type.Interval:
      return interval(type.unit);
    case Type.List:
    case Type.LargeList:
      return (type.typeId === Type.LargeList ? largeList : list)(fField(type.children[0]));
    case Type.FixedSizeList:
      return fixedSizeList(fField(type.children[0]), type.listSize);
    case Type.Struct:
      return struct(type.children.map(fField));
    case Type.Map: {
      // children[0] is the "entries" struct field holding [key, value].
      const entries = type.children[0];
      const [k, v] = entries.type.children;
      return map(fField(k), fField(v), type.keysSorted ?? false);
    }
    case Type.Dictionary:
      return dictionary(
        toFlechetteType(type.dictionary),
        toFlechetteType(type.indices),
        type.ordered ?? type.isOrdered ?? false,
        type.id,
      );
    case Type.Union:
      return union(type.mode, type.children.map(fField), type.typeIds);
    default:
      return type;
  }
}

/**
 * Normalize every field's type in a schema-like object to flechette-native,
 * preserving field name / nullable / metadata.
 */
export function normalizeSchemaFields(fields: readonly any[]): any[] {
  return fields.map(fField);
}
