/**
 * Decoding of the framework's out-of-band log and error batches.
 *
 * A zero-row batch whose custom metadata carries `vgi_rpc.log_level` is not
 * data: it is a log record the peer emitted during the call, or — at
 * `EXCEPTION` — the error that ended it. Lives here rather than beside the
 * client because an externalized payload carries the same batches, and
 * resolving one is shared with the server.
 */
import type { RecordBatch } from "@query-farm/apache-arrow";
import type { LogMessage } from "./client/types.js";
/**
 * Check if a zero-row batch carries log/error metadata.
 * If EXCEPTION → throw RpcError.
 * If other level → call onLog.
 * Returns true if the batch was consumed as a log/error.
 */
export declare function dispatchLogOrError(batch: RecordBatch, onLog?: (msg: LogMessage) => void): boolean;
//# sourceMappingURL=log-batch.d.ts.map