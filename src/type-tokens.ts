// Canonical text tokens for Arrow types, for the protocol hash preimage.
//
// The protocol hash is taken over what Arrow *decodes to*, not over what an
// encoder emits: each language's Arrow implementation may legitimately produce
// different bytes for the same logical schema, so a hash over serialized IPC is
// not a cross-language contract. The preimage is canonical JSON (RFC 8785) of
// the decoded description, and these tokens are how a type appears inside it.
//
// JSON solves framing, escaping and key ordering. It does not solve spelling --
// two ports can agree on every JCS rule and still disagree on whether a
// microsecond timestamp is `timestamp[us]` or `timestamp(us)`, which is a
// silent hash divergence. So the vocabulary is enumerated exhaustively and
// {@link typeToken} is total: an unrecognised type throws rather than falling
// back to the backend's own `toString`, whose output is an implementation
// detail that differs between arrow-js, flechette, and every other port.
//
// Grammar: a token is lowercase ASCII. Parameters go in parentheses, children
// in angle brackets. A child is `name:token` when non-nullable and
// `name?:token` when nullable -- child nullability is part of the type in
// Arrow, and two schemas differing only there are different schemas. Numeric
// parameters are folded into the token (`decimal128(38,9)`) so the preimage
// contains no JSON numbers and RFC 8785's hardest rule, number
// canonicalisation, never applies. Keep it that way.
//
// What is normalised: Arrow's own type equality ignores the *name* of a list's
// child field and of a map's key/value fields. Keeping them would give two
// ports different hashes for a protocol Arrow itself calls identical.
// Everything Arrow does treat as part of the type is kept: child nullability,
// struct field names, union child names and type codes, dictionary index/value
// types and orderedness, and map keysSorted.

import { TypeId } from "./arrow/predicates.js";
import type { VgiDataType, VgiField } from "./arrow/types.js";

/** Thrown when an Arrow type has no canonical token.
 *
 *  Thrown rather than falling back to the backend's `toString`: a port that
 *  silently spelled an unknown type its own way would produce a protocol hash
 *  that disagrees with every other port, and the disagreement would surface as
 *  an unexplained mismatch at a client rather than as an error here. */
export class UnsupportedArrowTypeError extends Error {
  constructor(type: unknown) {
    super(
      `Arrow type ${String(type)} has no canonical token. Add one to ` +
        `src/type-tokens.ts and to every other port at the same time: a one-sided ` +
        `addition changes only this port's protocol hash.`,
    );
    this.name = "UnsupportedArrowTypeError";
  }
}

/** Arrow's own unit spellings, indexed by the numeric unit the backends use. */
const TIME_UNITS = ["s", "ms", "us", "ns"] as const;

function unitToken(unit: unknown): string {
  if (typeof unit === "number" && unit >= 0 && unit < TIME_UNITS.length) return TIME_UNITS[unit]!;
  if (typeof unit === "string") {
    const lower = unit.toLowerCase();
    if (lower === "second") return "s";
    if (lower === "millisecond") return "ms";
    if (lower === "microsecond") return "us";
    if (lower === "nanosecond") return "ns";
    if ((TIME_UNITS as readonly string[]).includes(lower)) return lower;
  }
  throw new UnsupportedArrowTypeError(`time unit ${String(unit)}`);
}

/** Spell a child whose name Arrow does not consider part of the type.
 *
 *  A list's child is named `item` by arrow-js, `element` by some Parquet
 *  producers, and whatever the caller passed by anyone constructing the type by
 *  hand -- and Arrow's own type equality ignores all of it. Normalising to a
 *  fixed name is what keeps two ports that default differently from hashing the
 *  same protocol differently. Nullability *is* part of the type, so it is
 *  kept. */
function anonChild(field: VgiField, name: string): string {
  return `${name}${field.nullable ? "?" : ""}:${typeToken(field.type)}`;
}

/** Spell a child field whose name is part of the type. */
function child(field: VgiField): string {
  return anonChild(field, field.name);
}

/** Return the canonical token for `type`. */
export function typeToken(type: VgiDataType): string {
  const t = type as VgiDataType & Record<string, unknown>;
  switch (type.typeId) {
    case TypeId.Null:
      return "null";
    case TypeId.Bool:
      return "bool";
    case TypeId.Int: {
      const bits = Number(t.bitWidth ?? 64);
      const signed = t.isSigned !== false;
      return `${signed ? "int" : "uint"}${bits}`;
    }
    case TypeId.Float: {
      // arrow-js spells precision 0/1/2 for half/single/double.
      const precision = Number(t.precision ?? 2);
      return ["float16", "float32", "float64"][precision] ?? "float64";
    }
    case TypeId.Utf8:
      return "utf8";
    case TypeId.LargeUtf8:
      return "large_utf8";
    case TypeId.Binary:
      return "binary";
    case TypeId.LargeBinary:
      return "large_binary";
    case TypeId.FixedSizeBinary:
      return `fixed_size_binary(${Number(t.byteWidth)})`;
    case TypeId.Date:
      // arrow-js spells unit 0 for DAY (date32) and 1 for MILLISECOND (date64).
      return Number(t.unit ?? 0) === 0 ? "date32" : "date64";
    case TypeId.Time: {
      const bits = Number(t.bitWidth ?? 64);
      return `time${bits}(${unitToken(t.unit)})`;
    }
    case TypeId.Timestamp: {
      // The zone is carried verbatim: "UTC" and "+00:00" are distinct Arrow
      // types and must not collapse to one token.
      const tz = t.timezone ?? t.timeZone ?? null;
      return tz ? `timestamp(${unitToken(t.unit)},tz=${String(tz)})` : `timestamp(${unitToken(t.unit)})`;
    }
    case TypeId.Duration:
      return `duration(${unitToken(t.unit)})`;
    case TypeId.Decimal: {
      const bits = Number(t.bitWidth ?? 128);
      return `decimal${bits}(${Number(t.precision)},${Number(t.scale)})`;
    }
    case TypeId.List:
      return `list<${anonChild(listChild(t), "item")}>`;
    case TypeId.FixedSizeList:
      return `fixed_size_list(${Number(t.listSize)})<${anonChild(listChild(t), "item")}>`;
    case TypeId.Struct:
      return `struct<${structChildren(t).map(child).join(",")}>`;
    case TypeId.Map: {
      // A map's child is a struct of the key and value fields.
      const entries = structChildren(listChild(t).type as VgiDataType & Record<string, unknown>);
      if (entries.length !== 2) throw new UnsupportedArrowTypeError(type);
      const token = `map<${anonChild(entries[0]!, "key")},${anonChild(entries[1]!, "value")}>`;
      // keysSorted is part of the type in Arrow, so it is part of the token.
      return t.keysSorted ? `${token},keys_sorted` : token;
    }
    case TypeId.Dictionary: {
      const indexType = t.indices as VgiDataType | undefined;
      const valueType = t.dictionary as VgiDataType | undefined;
      if (!indexType || !valueType) throw new UnsupportedArrowTypeError(type);
      const token = `dictionary<index:${typeToken(indexType)},value:${typeToken(valueType)}>`;
      return t.isOrdered ? `${token},ordered` : token;
    }
    case TypeId.Union: {
      // Type codes need not be 0..n-1, so they are spelled rather than implied
      // by position.
      const codes = (t.typeIds ?? []) as readonly number[];
      const parts = structChildren(t).map((f, i) => `${codes[i] ?? i}=${child(f)}`);
      const kind = Number(t.mode ?? 0) === 0 ? "sparse_union" : "dense_union";
      return `${kind}<${parts.join(",")}>`;
    }
    default:
      throw new UnsupportedArrowTypeError(type);
  }
}

/** The single child of a list-like type, across backend spellings. */
function listChild(t: Record<string, unknown>): VgiField {
  const children = (t.children ?? []) as readonly VgiField[];
  if (children.length !== 1) throw new UnsupportedArrowTypeError(t);
  return children[0]!;
}

/** The children of a struct-like type, across backend spellings. */
function structChildren(t: Record<string, unknown>): readonly VgiField[] {
  return (t.children ?? []) as readonly VgiField[];
}

/** One top-level schema field as it appears in the hash preimage.
 *
 *  Strings and booleans only, so the preimage carries no JSON numbers. */
export interface FieldToken {
  name: string;
  nullable: boolean;
  type: string;
}

/** Describe a schema's fields in declaration order, which is significant. */
export function schemaTokens(fields: readonly VgiField[] | undefined | null): FieldToken[] {
  if (!fields) return [];
  return fields.map((f) => ({ name: f.name, nullable: f.nullable, type: typeToken(f.type) }));
}
