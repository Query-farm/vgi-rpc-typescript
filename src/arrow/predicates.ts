// Backend-agnostic Arrow type predicates. typeId values match the Arrow Type
// enum and agree across arrow-js and flechette.

import type { VgiDataType } from "./types.js";

export const TypeId = {
  Null: 1,
  Int: 2,
  Float: 3,
  Binary: 4,
  Utf8: 5,
  Bool: 6,
  Decimal: 7,
  Date: 8,
  Time: 9,
  Timestamp: 10,
  Interval: 11,
  List: 12,
  Struct: 13,
  Union: 14,
  FixedSizeBinary: 15,
  FixedSizeList: 16,
  Map: 17,
  Duration: 18,
  LargeBinary: 19,
  LargeUtf8: 20,
  Dictionary: -1,
} as const;

export const isNull = (t: VgiDataType): boolean => t.typeId === TypeId.Null;
export const isInt = (t: VgiDataType): boolean => t.typeId === TypeId.Int;
export const isFloat = (t: VgiDataType): boolean => t.typeId === TypeId.Float;
export const isBinary = (t: VgiDataType): boolean => t.typeId === TypeId.Binary || t.typeId === TypeId.LargeBinary;
export const isUtf8 = (t: VgiDataType): boolean => t.typeId === TypeId.Utf8 || t.typeId === TypeId.LargeUtf8;
export const isLargeUtf8 = (t: VgiDataType): boolean => t.typeId === TypeId.LargeUtf8;
export const isLargeBinary = (t: VgiDataType): boolean => t.typeId === TypeId.LargeBinary;
export const isBool = (t: VgiDataType): boolean => t.typeId === TypeId.Bool;
export const isDecimal = (t: VgiDataType): boolean => t.typeId === TypeId.Decimal;
export const isDate = (t: VgiDataType): boolean => t.typeId === TypeId.Date;
export const isTime = (t: VgiDataType): boolean => t.typeId === TypeId.Time;
export const isTimestamp = (t: VgiDataType): boolean => t.typeId === TypeId.Timestamp;
export const isDuration = (t: VgiDataType): boolean => t.typeId === TypeId.Duration;
export const isList = (t: VgiDataType): boolean => t.typeId === TypeId.List;
export const isStruct = (t: VgiDataType): boolean => t.typeId === TypeId.Struct;
export const isMap = (t: VgiDataType): boolean => t.typeId === TypeId.Map;
export const isFixedSizeBinary = (t: VgiDataType): boolean => t.typeId === TypeId.FixedSizeBinary;
export const isDictionary = (t: VgiDataType): boolean => t.typeId === TypeId.Dictionary;

export function isBatch(x: unknown): x is import("./types.js").VgiBatch {
  return (
    x != null &&
    typeof (x as any).numRows === "number" &&
    (x as any).schema != null &&
    Array.isArray((x as any).schema.fields)
  );
}
