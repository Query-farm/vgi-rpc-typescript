import { type VgiDataType, type VgiField, type VgiSchema } from "./arrow/index.js";
/** Apache Arrow Utf8 type. Use as schema shorthand: `{ name: str }` */
export declare const str: VgiDataType;
/** Apache Arrow Binary type. Use as schema shorthand: `{ data: bytes }` */
export declare const bytes: VgiDataType;
/** Apache Arrow Int64 type. Use as schema shorthand: `{ count: int }` */
export declare const int: VgiDataType;
/** Apache Arrow Int32 type. Use as schema shorthand: `{ count: int32 }` */
export declare const int32: VgiDataType;
/** Apache Arrow Int16 type. Use as schema shorthand: `{ count: int16 }` */
export declare const int16: VgiDataType;
/** Apache Arrow Int8 type. Use as schema shorthand: `{ count: int8 }` */
export declare const int8: VgiDataType;
/** Apache Arrow Uint8 type. Use as schema shorthand: `{ count: uint8 }` */
export declare const uint8: VgiDataType;
/** Apache Arrow Uint16 type. Use as schema shorthand: `{ count: uint16 }` */
export declare const uint16: VgiDataType;
/** Apache Arrow Uint32 type. Use as schema shorthand: `{ count: uint32 }` */
export declare const uint32: VgiDataType;
/** Apache Arrow Uint64 type. Use as schema shorthand: `{ count: uint64 }` */
export declare const uint64: VgiDataType;
/** Apache Arrow Float64 type. Use as schema shorthand: `{ value: float }` */
export declare const float: VgiDataType;
/** Apache Arrow Float32 type. Use as schema shorthand: `{ value: float32 }` */
export declare const float32: VgiDataType;
/** Apache Arrow Bool type. Use as schema shorthand: `{ flag: bool }` */
export declare const bool: VgiDataType;
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
        readonly type: {
            readonly typeId: number;
        };
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
/**
 * Convert a SchemaLike spec into a real `VgiSchema`.
 */
export declare function toSchema(spec: SchemaLike): VgiSchema;
/**
 * Derive a `paramTypes` record from a SchemaLike spec.
 * Maps common Arrow scalar types to Python-style type strings.
 * Returns `undefined` if any field has a complex type (List, Map_, Dictionary, etc.).
 */
export declare function inferParamTypes(spec: SchemaLike): Record<string, string> | undefined;
//# sourceMappingURL=schema.d.ts.map