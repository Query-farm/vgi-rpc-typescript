/**
 * Rebuild `type` as a flechette-native DataType. Idempotent for types that are
 * already native (reconstructed from the same structural props). Unknown
 * typeIds pass through unchanged as a safety net.
 */
export declare function toFlechetteType(type: any): any;
/**
 * Normalize every field's type in a schema-like object to flechette-native,
 * preserving field name / nullable / metadata.
 */
export declare function normalizeSchemaFields(fields: readonly any[]): any[];
//# sourceMappingURL=normalize-type.d.ts.map