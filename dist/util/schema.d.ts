import { type VgiSchema } from "../arrow/index.js";
/**
 * Serialize a Schema to the Arrow IPC Schema message format.
 * This produces bytes compatible with Python's `pa.ipc.read_schema()`.
 * Equivalent to writing an empty-batch IPC stream — schema message + EOS marker.
 */
export declare function serializeSchema(schema: VgiSchema): Uint8Array;
//# sourceMappingURL=schema.d.ts.map