// Minimal Arrow Message-envelope reader.
//
// flechette's IPC parser ignores the Message-level `custom_metadata` field
// (vtable offset 12 in the Message FlatBuffer) and only surfaces row count
// through column children — both of which are needed to round-trip vgi
// "params" batches that carry zero-field schemas with auth/state tokens
// in batch metadata. arrow-js exposes both via `RecordBatch.metadata` and
// `RecordBatch.numRows`.
//
// This file is a self-contained, dependency-free reader that walks an IPC
// stream and pulls out, for the first RecordBatch message it sees:
//   * `numRows`   — from RecordBatch.length (Message header_value, RB vtable offset 4)
//   * `metadata`  — Message.custom_metadata (Message vtable offset 12)
//
// Schema fields are 0/1/2/3/4; same for RecordBatch — see Arrow's
// `format/Message.fbs` and `format/Schema.fbs`.

const TYPE_SCHEMA = 1;
const TYPE_RECORD_BATCH = 3;

function readU32LE(b: Uint8Array, p: number): number {
  return ((b[p] | (b[p + 1] << 8) | (b[p + 2] << 16) | (b[p + 3] << 24)) >>> 0);
}
function readI32LE(b: Uint8Array, p: number): number {
  return readU32LE(b, p) | 0;
}
function readI16LE(b: Uint8Array, p: number): number {
  const v = b[p] | (b[p + 1] << 8);
  return v < 0x8000 ? v : v - 0x10000;
}
function readI64LE(b: Uint8Array, p: number): bigint {
  const lo = BigInt(readU32LE(b, p));
  const hi = BigInt(readU32LE(b, p + 4));
  // Sign-extend if hi's top bit is set.
  const top = hi & 0x80000000n;
  const combined = (hi << 32n) | lo;
  return top ? combined - (1n << 64n) : combined;
}

// FlatBuffer table accessor. Holds the absolute byte offset of the table
// (root or sub-table) plus the parsed vtable field offsets.
interface FbTable {
  pos: number;
  fields: Int16Array;
}

function readTable(buf: Uint8Array, tablePos: number): FbTable {
  // First field at tablePos is the (signed) offset back to the vtable.
  const vtableRel = readI32LE(buf, tablePos);
  const vtablePos = tablePos - vtableRel;
  const vtableSize = readI16LE(buf, vtablePos);
  const fieldCount = (vtableSize - 4) >> 1;
  const fields = new Int16Array(fieldCount);
  for (let i = 0; i < fieldCount; i++) {
    fields[i] = readI16LE(buf, vtablePos + 4 + i * 2);
  }
  return { pos: tablePos, fields };
}

/** Position of a field in the table buffer, or null if absent (offset 0). */
function fieldAt(t: FbTable, fieldIndex: number): number | null {
  if (fieldIndex >= t.fields.length) return null;
  const off = t.fields[fieldIndex];
  return off === 0 ? null : t.pos + off;
}

/** Follow a uoffset_t at `ptrPos` to the absolute target byte position. */
function follow(buf: Uint8Array, ptrPos: number): number {
  return ptrPos + readU32LE(buf, ptrPos);
}

function readString(buf: Uint8Array, strPos: number): string {
  const len = readU32LE(buf, strPos);
  return new TextDecoder().decode(buf.subarray(strPos + 4, strPos + 4 + len));
}

interface ParsedMessage {
  /** Header type tag — 1 = Schema, 2 = DictionaryBatch, 3 = RecordBatch. */
  headerType: number;
  /** RecordBatch.length (rows). Zero if not a RecordBatch or absent. */
  numRows: number;
  /** Message-level body length (offset 10). Used to skip past the body. */
  bodyLength: number;
  /** Message.custom_metadata, parsed into a Map. Empty if absent. */
  metadata: Map<string, string>;
}

function readMessageEnvelope(head: Uint8Array): ParsedMessage {
  const rootOffset = readU32LE(head, 0);
  const msg = readTable(head, rootOffset);

  // Message field indices (Message.fbs):
  //   0: version (int16)            vtable offset 4
  //   1: headerType (uint8)         vtable offset 6
  //   2: header   (table)           vtable offset 8
  //   3: bodyLength (int64)         vtable offset 10
  //   4: custom_metadata ([KeyVal]) vtable offset 12
  const headerTypePos = fieldAt(msg, 1);
  const headerType = headerTypePos === null ? 0 : head[headerTypePos];

  const bodyLengthPos = fieldAt(msg, 3);
  const bodyLengthBig = bodyLengthPos === null ? 0n : readI64LE(head, bodyLengthPos);
  // Bodies are bounded by maxDecompressedRequestBytes upstream — safe to coerce.
  const bodyLength = Number(bodyLengthBig);

  let numRows = 0;
  if (headerType === TYPE_RECORD_BATCH) {
    const headerValuePos = fieldAt(msg, 2);
    if (headerValuePos !== null) {
      const rbPos = follow(head, headerValuePos);
      const rb = readTable(head, rbPos);
      // RecordBatch.length is field 0 (vtable offset 4), int64.
      const lenPos = fieldAt(rb, 0);
      numRows = lenPos === null ? 0 : Number(readI64LE(head, lenPos));
    }
  }

  const metadata = new Map<string, string>();
  const metaVecPtr = fieldAt(msg, 4);
  if (metaVecPtr !== null) {
    const vecPos = follow(head, metaVecPtr);
    const vecLen = readU32LE(head, vecPos);
    for (let i = 0; i < vecLen; i++) {
      // Vector of tables: each entry is a uoffset_t (4 bytes) pointing to the table.
      const itemPtr = vecPos + 4 + i * 4;
      const kvPos = follow(head, itemPtr);
      const kv = readTable(head, kvPos);
      const keyPtr = fieldAt(kv, 0);
      const valPtr = fieldAt(kv, 1);
      const key = keyPtr === null ? "" : readString(head, follow(head, keyPtr));
      const val = valPtr === null ? "" : readString(head, follow(head, valPtr));
      metadata.set(key, val);
    }
  }

  return { headerType, numRows, bodyLength, metadata };
}

/**
 * Walk an Arrow IPC stream and return the first RecordBatch message's
 * (numRows, metadata) — the bits flechette drops on the floor.
 *
 * Returns null if the stream contains no RecordBatch.
 */
export function readFirstRecordBatchMeta(
  stream: Uint8Array,
): { numRows: number; metadata: Map<string, string> } | null {
  let pos = 0;
  while (pos + 4 <= stream.length) {
    let metaLen = readI32LE(stream, pos);
    pos += 4;
    // Continuation-marker form (post-Arrow 0.15): the real length follows.
    if (metaLen === -1) {
      if (pos + 4 > stream.length) return null;
      metaLen = readI32LE(stream, pos);
      pos += 4;
    }
    if (metaLen === 0) return null; // EOS
    if (pos + metaLen > stream.length) return null;
    const head = stream.subarray(pos, pos + metaLen);
    pos += metaLen;
    const parsed = readMessageEnvelope(head);
    if (parsed.headerType === TYPE_RECORD_BATCH) {
      return { numRows: parsed.numRows, metadata: parsed.metadata };
    }
    // Skip the body (Schema messages have bodyLength = 0).
    pos += parsed.bodyLength;
    if (parsed.headerType !== TYPE_SCHEMA && parsed.headerType !== 2 /* Dict */) {
      // Unknown header — stop walking.
      return null;
    }
  }
  return null;
}
