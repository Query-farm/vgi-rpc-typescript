import { type VgiBatch } from "../arrow/index.js";
import type { MethodDefinition } from "../types.js";
/**
 * Slim DESCRIBE_VERSION 4 schema. Python-flavoured fields (doc,
 * param_types_json, param_defaults_json, param_docs_json) are not on the
 * wire — Arrow IPC schema bytes are the authoritative type information;
 * everything else is source-level metadata that callers should consult the
 * Protocol class for.
 */
export declare const DESCRIBE_SCHEMA: import("../arrow/types.js").VgiSchema;
/**
 * Build the __describe__ response batch and metadata.
 */
export declare function buildDescribeBatch(protocolName: string, methods: Map<string, MethodDefinition>, serverId: string, protocolVersion?: string): Promise<{
    batch: VgiBatch;
    metadata: Map<string, string>;
}>;
//# sourceMappingURL=describe.d.ts.map