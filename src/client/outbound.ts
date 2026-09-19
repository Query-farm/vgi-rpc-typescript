// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * The batches the client writes, on whichever Arrow backend this process runs.
 *
 * The facade's writers -- `serializeIpcStream`, the lockstep encoder -- are
 * arrow-js's or flechette's by process (workerd and `--conditions=flechette`
 * select flechette), and each can only write its own implementation's batches:
 * flechette's writer cannot read an arrow-js `RecordBatch`, and an arrow-js
 * `RecordBatch` cannot wrap a flechette table. The client meets both kinds. It
 * builds its own batches (ticks, cancels, rows, empty turns) through the
 * facade, as the unary path does, so those are the active backend's. But its
 * reader returns arrow-js batches and its API types a caller's batches as
 * arrow-js `RecordBatch`, so under flechette a caller -- a relay, a conformance
 * driver, anyone re-sending a batch the client handed back -- may well hold one.
 *
 * So every outbound batch is written by the implementation that owns it: a
 * caller's arrow-js batch by arrow-js, verbatim (schema, buffers, and the row
 * count of a zero-column batch, which flechette's writer cannot express), and
 * everything else by the active backend. Where one IPC stream must carry both
 * -- a lockstep stream whose later turns the client builds itself -- a batch
 * crosses to the stream's implementation as Arrow IPC.
 *
 * Under arrow-js every batch is the active backend's and nothing here changes a
 * byte.
 *
 * @internal
 */

import {
  backend,
  createIncrementalEncoder,
  deserializeBatch,
  serializeBatch,
  withBatchMetadata as withNativeBatchMetadata,
} from "#vgi-rpc-arrow";
import * as arrowjs from "../arrow/impl-arrowjs/index.js";
import type { IncrementalEncoder, VgiBatch, VgiSchema } from "../arrow/types.js";
import { serializeIpcStream } from "../http/common.js";

/** Which implementation a batch belongs to: arrow-js in a process whose
 *  backend is not arrow-js, or the active backend. */
type Owner = "arrow-js" | "active";

/**
 * Structural rather than `instanceof`, which a second copy of apache-arrow in
 * the module graph defeats: an arrow-js batch keeps its columns under `data`,
 * a flechette table under `children`.
 */
function ownerOf(batch: unknown): Owner {
  if (backend.name === "arrow-js") return "active";
  const b = batch as { data?: unknown; children?: unknown } | null;
  return b != null && typeof b.data === "object" && b.data !== null && !Array.isArray(b.children)
    ? "arrow-js"
    : "active";
}

/** `batch` as `owner`'s type, crossing as Arrow IPC (schema and custom metadata carried) when it is the other's. */
function asOwner(batch: VgiBatch, owner: Owner): VgiBatch {
  if (ownerOf(batch) === owner) return batch;
  return owner === "active"
    ? deserializeBatch(arrowjs.serializeBatch(batch))
    : arrowjs.deserializeBatch(serializeBatch(batch));
}

/** `batch` as the active backend's own type. */
export function toActiveBackend<B>(batch: B): B {
  return asOwner(batch as unknown as VgiBatch, "active") as unknown as B;
}

/**
 * `batch` carrying exactly `metadata`, in the implementation it came in.
 *
 * The map replaces whatever the batch carried: the caller's (or the
 * transport's) metadata is what the peer must see.
 */
export function withMetadata<B>(batch: B, metadata: Map<string, string>): B {
  const b = batch as unknown as VgiBatch;
  return (ownerOf(b) === "arrow-js"
    ? arrowjs.withBatchMetadata(b, metadata)
    : withNativeBatchMetadata(b, metadata)) as unknown as B;
}

/**
 * One request body: `batches` as a single IPC stream under `schema`.
 *
 * A body of a caller's arrow-js batches is written by arrow-js under their own
 * schema; anything else by the active backend.
 */
export function serializeRequest(schema: unknown, batches: readonly unknown[]): Uint8Array {
  const all = batches as VgiBatch[];
  if (all.length > 0 && all.every((b) => ownerOf(b) === "arrow-js")) {
    return arrowjs.serializeBatches(all[0].schema, all);
  }
  return serializeIpcStream(
    schema as VgiSchema,
    all.map((b) => asOwner(b, "active")),
  );
}

/**
 * An incremental encoder for one lockstep request stream.
 *
 * The stream's implementation is its first batch's (arrow-js for a caller's
 * arrow-js batch, else the active backend), and every later batch crosses to
 * it. Nothing is emitted until the first batch or `finish()` -- arrow-js would
 * emit the schema from `start()`, but the implementation is not known yet; the
 * bytes are the same, in the same order.
 */
export function createRequestEncoder(schema: unknown): {
  writeBatch(batch: unknown): Uint8Array;
  finish(): Uint8Array;
} {
  let owner: Owner | null = null;
  let encoder: IncrementalEncoder | null = null;
  const open = (as: Owner, s: VgiSchema): Uint8Array => {
    owner = as;
    encoder = as === "arrow-js" ? arrowjs.createIncrementalEncoder(s) : createIncrementalEncoder(s);
    return encoder.start();
  };
  return {
    writeBatch(batch: unknown): Uint8Array {
      const b = batch as VgiBatch;
      let head: Uint8Array = new Uint8Array(0);
      if (encoder === null) {
        const as = ownerOf(b);
        head = open(as, as === "arrow-js" ? b.schema : (schema as VgiSchema));
      }
      const body = (encoder as unknown as IncrementalEncoder).writeBatch(asOwner(b, owner as unknown as Owner));
      return concat(head, body);
    },
    finish(): Uint8Array {
      const head = encoder === null ? open("active", schema as VgiSchema) : new Uint8Array(0);
      return concat(head, (encoder as unknown as IncrementalEncoder).finish());
    },
  };
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  if (a.length === 0) return b;
  if (b.length === 0) return a;
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
}
