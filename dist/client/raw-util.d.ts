/**
 * Small constructors shared by the two transports' raw (batch-level) call
 * paths. Internal: nothing here is part of the published surface.
 *
 * @internal
 */
import type { RecordBatch } from "@query-farm/apache-arrow";
import type { RawBatch } from "./raw.js";
/**
 * Pair a decoded batch with its own custom metadata.
 *
 * @internal
 */
export declare function rawBatchOf(batch: RecordBatch): RawBatch;
/**
 * Rebuild `input`'s batch so the bytes written carry exactly `input.metadata`.
 *
 * The caller's metadata map is authoritative — it is what the peer must see —
 * and a batch decoded from one stream and rewritten to another would
 * otherwise carry whatever metadata its own message happened to hold.
 *
 * @internal
 */
export declare function rawInputBatch(input: RawBatch, extra?: ReadonlyMap<string, string>): RecordBatch;
//# sourceMappingURL=raw-util.d.ts.map