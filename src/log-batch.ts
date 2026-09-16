// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

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
import { LOG_EXTRA_KEY, LOG_LEVEL_KEY, LOG_MESSAGE_KEY } from "./constants.js";
import { RpcError } from "./errors.js";

/**
 * Check if a zero-row batch carries log/error metadata.
 * If EXCEPTION → throw RpcError.
 * If other level → call onLog.
 * Returns true if the batch was consumed as a log/error.
 */
export function dispatchLogOrError(batch: RecordBatch, onLog?: (msg: LogMessage) => void): boolean {
  const meta = batch.metadata;
  if (!meta) return false;

  const level = meta.get(LOG_LEVEL_KEY);
  if (!level) return false;

  const message = meta.get(LOG_MESSAGE_KEY) ?? "";

  if (level === "EXCEPTION") {
    const extraStr = meta.get(LOG_EXTRA_KEY);
    let errorType = "RpcError";
    let errorMessage = message;
    let traceback = "";
    if (extraStr) {
      try {
        const extra = JSON.parse(extraStr);
        errorType = extra.exception_type ?? "RpcError";
        errorMessage = extra.exception_message ?? message;
        traceback = extra.traceback ?? "";
      } catch {}
    }
    throw new RpcError(errorType, errorMessage, traceback);
  }

  if (onLog) {
    const extraStr = meta.get(LOG_EXTRA_KEY);
    let extra: Record<string, any> | undefined;
    if (extraStr) {
      try {
        extra = JSON.parse(extraStr);
      } catch {}
    }
    onLog({ level, message, extra });
  }

  return true;
}
