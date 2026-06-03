import { type VgiDataType } from "../arrow/index.js";
/**
 * Arrow types whose `.get(0)` / `vectorFromArray` round-trips are unreliable
 * in arrow-js. For these we extract and re-emit the underlying `Data` object
 * directly (passthrough), like we already do for Map_.
 *
 * Covers Date/Time/Timestamp/Duration/Decimal/LargeUtf8/LargeBinary/
 * FixedSizeBinary/Dictionary.
 */
export declare function isOpaquePassthroughType(type: VgiDataType): boolean;
//# sourceMappingURL=opaque.d.ts.map