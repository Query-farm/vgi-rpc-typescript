// Backend-agnostic Arrow type surface used inside vgi-rpc-typescript.
// Mirrors vgi-typescript's facade so structurally compatible values flow
// freely between the two packages.

export type VgiTypeId = number;

export interface VgiDataType {
  readonly typeId: VgiTypeId;
}

export interface VgiField {
  readonly name: string;
  readonly type: VgiDataType;
  readonly nullable: boolean;
  readonly metadata: Map<string, string>;
}

export interface VgiSchema {
  readonly fields: readonly VgiField[];
  readonly metadata: Map<string, string>;
}

export interface VgiColumn {
  readonly type: VgiDataType;
  readonly length: number;
  get(index: number): unknown;
  [Symbol.iterator](): Iterator<unknown>;
}

export interface VgiBatch {
  readonly schema: VgiSchema;
  readonly numRows: number;
  readonly metadata?: Map<string, string> | null;
  getChild(name: string): VgiColumn | null;
  getChildAt(index: number): VgiColumn | null;
}

export interface VgiBackendInfo {
  readonly name: "arrow-js" | "flechette";
}

export type VgiColumnData = unknown;

/**
 * Incremental IPC stream encoder for the lockstep stdio transport.
 *
 * The stdio exchange protocol is lockstep — the client reads each response
 * batch (and its framing bytes) before sending the next input — so we
 * cannot buffer-then-emit at close. Each call returns the wire bytes to
 * flush immediately. Only the stdio server uses this; HTTP serializes
 * whole responses via {@link serializeBatches}.
 *
 * The arrow-js backend implements this over `RecordBatchStreamWriter`.
 * The flechette backend has no incremental-writer surface and its
 * factory throws — keeping arrow-js out of the flechette (workerd/
 * browser) bundle, which is HTTP-only anyway.
 */
export interface IncrementalEncoder {
  /** Bytes for the schema preamble (continuation + schema message). */
  start(): Uint8Array;
  /** Bytes for one record batch message. */
  writeBatch(batch: VgiBatch): Uint8Array;
  /** Bytes for the end-of-stream marker. */
  finish(): Uint8Array;
}
