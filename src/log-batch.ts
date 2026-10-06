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
import {
  ERROR_CODE_KEY,
  ERROR_DETAILS_KEY,
  ERROR_KIND_KEY,
  LOG_EXTRA_KEY,
  LOG_LEVEL_KEY,
  LOG_MESSAGE_KEY,
  REQUEST_ID_KEY,
} from "./constants.js";
import { decodeErrorDetails, detailObjects } from "./error-model.js";
import { RpcError, type RpcErrorModel } from "./errors.js";

/**
 * Read the error model's three layers off an EXCEPTION batch.
 *
 * Top-level keys are canonical and read first; the `log_extra` mirror is the
 * fallback, so a server -- or an intermediary that rebuilt the batch -- that
 * set only one of the two still classifies. Every client decode path (pipe,
 * HTTP unary/stream/exchange, an externalized error batch) funnels through
 * {@link dispatchLogOrError}, which is why this is the only place they are
 * read: three clients dropped `error_kind` by reading it on some paths only.
 */
export function errorModelFields(
  meta: ReadonlyMap<string, string>,
  extra: Record<string, unknown> | undefined,
): RpcErrorModel {
  const text = (key: string, fallback: string): string => {
    const top = meta.get(key);
    if (top !== undefined) return top;
    const mirrored = extra?.[fallback];
    return typeof mirrored === "string" ? mirrored : "";
  };
  const rawDetails = meta.get(ERROR_DETAILS_KEY);
  return {
    errorCode: text(ERROR_CODE_KEY, "error_code"),
    errorKind: text(ERROR_KIND_KEY, "error_kind"),
    errorDetails: rawDetails !== undefined ? decodeErrorDetails(rawDetails) : detailObjects(extra?.error_details),
    requestId: meta.get(REQUEST_ID_KEY) ?? "",
  };
}

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
    let extra: Record<string, unknown> | undefined;
    if (extraStr) {
      try {
        const parsed = JSON.parse(extraStr);
        if (parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)) {
          extra = parsed as Record<string, unknown>;
          errorType = typeof extra.exception_type === "string" ? extra.exception_type : "RpcError";
          errorMessage = typeof extra.exception_message === "string" ? extra.exception_message : message;
          traceback = typeof extra.traceback === "string" ? extra.traceback : "";
        }
      } catch {}
    }
    throw new RpcError(errorType, errorMessage, traceback, errorModelFields(meta, extra));
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
