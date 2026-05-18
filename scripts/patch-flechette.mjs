#!/usr/bin/env bun
// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Apply local patches to `node_modules/@uwdata/flechette` that add
 * per-record-batch `custom_metadata` support (Arrow IPC Message field 4).
 * The vgi-rpc wire protocol puts log_level / log_message / server_id /
 * request_id on RecordBatch messages — without these patches, flechette
 * silently drops them on encode and ignores them on decode.
 *
 * Idempotent: running twice is a no-op. Re-applies after every
 * `bun install` via the package.json postinstall script.
 *
 * These changes belong upstream in the Query-farm/flechette fork
 * eventually; this script is the in-tree transition.
 */

import { readFileSync, writeFileSync, existsSync } from "node:fs";
import { resolve } from "node:path";

const ROOT = resolve(import.meta.dirname, "..", "node_modules", "@uwdata", "flechette");
if (!existsSync(ROOT)) {
  // Flechette not installed (e.g., fresh clone before bun install). Skip
  // silently — the user's bun install will run postinstall again.
  process.exit(0);
}

const PATCHES = [
  // ---------------------------------------------------------------------
  // 1. package.json — prefer src/index.js so our patches take effect.
  //    Bun resolves `main` over `module` in some default paths.
  // ---------------------------------------------------------------------
  {
    file: "package.json",
    edits: [
      {
        find: '"main": "./dist/flechette.cjs",\n  "module": "./src/index.js",',
        replace: '"main": "./src/index.js",\n  "module": "./src/index.js",',
      },
    ],
  },

  // ---------------------------------------------------------------------
  // 2. encode/message.js — accept a per-message metadata FlatBuffer
  //    offset and emit it at field 4 of the Message table.
  // ---------------------------------------------------------------------
  {
    file: "src/encode/message.js",
    edits: [
      {
        find: "export function writeMessage(builder, headerType, headerOffset, bodyLength, blocks) {",
        replace:
          "export function writeMessage(builder, headerType, headerOffset, bodyLength, blocks, metadataOffset = 0) {",
      },
      {
        find: "      b.addInt64(3, bodyLength, 0);\n      // NOT SUPPORTED: 4, message-level metadata\n    })",
        replace: "      b.addInt64(3, bodyLength, 0);\n      b.addOffset(4, metadataOffset, 0);\n    })",
      },
    ],
  },

  // ---------------------------------------------------------------------
  // 3. encode/encode-ipc.js — encode each record batch's metadata via
  //    `encodeMetadata` before emitting the Message header.
  // ---------------------------------------------------------------------
  {
    file: "src/encode/encode-ipc.js",
    edits: [
      {
        find: "import { encodeRecordBatch } from './record-batch.js';",
        replace:
          "import { encodeMetadata } from './metadata.js';\nimport { encodeRecordBatch } from './record-batch.js';",
      },
      {
        find: `  // write record batch messages
  for (const batch of records) {
    writeMessage(
      builder,
      MessageHeader.RecordBatch,
      encodeRecordBatch(builder, batch, compression),
      batch.byteLength,
      recordBlocks
    );
    writeBuffers(builder, batch.buffers);
  }`,
        replace: `  // write record batch messages
  for (const batch of records) {
    // Per-record-batch custom_metadata — Arrow IPC spec field 4 of the
    // Message table. Encode the metadata BEFORE the record-batch header
    // so writeMessage can finalize the message with both offsets in
    // FlatBuffer-required tail-first ordering.
    const batchMetadataOffset = batch?.metadata
      ? encodeMetadata(builder, batch.metadata)
      : 0;
    writeMessage(
      builder,
      MessageHeader.RecordBatch,
      encodeRecordBatch(builder, batch, compression),
      batch.byteLength,
      recordBlocks,
      batchMetadataOffset
    );
    writeBuffers(builder, batch.buffers);
  }`,
      },
    ],
  },

  // ---------------------------------------------------------------------
  // 4. encode/record-batch.js — derive RecordBatch.length from an
  //    explicit `batch.length` when nodes is empty (zero-field schema
  //    with metadata-only batches).
  // ---------------------------------------------------------------------
  {
    file: "src/encode/record-batch.js",
    edits: [
      {
        find: `  return builder.addObject(5, b => {
    b.addInt64(0, nodes[0].length, 0);
    b.addOffset(1, nodeVector, 0);`,
        replace: `  // RecordBatch.length is the batch's row count. Normally derived from
  // the first FieldNode's length, but a zero-field schema has no nodes —
  // fall back to an explicit \`batch.length\` (vgi-rpc's metadata-only
  // empty batch uses 0) so the FlatBuffer doesn't trip on \`nodes[0]\`.
  const rowCount = nodes.length > 0 ? nodes[0].length : (batch.length ?? 0);
  return builder.addObject(5, b => {
    b.addInt64(0, rowCount, 0);
    b.addOffset(1, nodeVector, 0);`,
      },
    ],
  },

  // ---------------------------------------------------------------------
  // 5. encode/tables-to-ipc.js — thread per-table custom_metadata into
  //    encode and pass it through both single- and multi-table paths.
  // ---------------------------------------------------------------------
  {
    file: "src/encode/tables-to-ipc.js",
    edits: [
      {
        find: `export function tablesToIPC(tables, options) {
  if (!tables || tables.length === 0) {
    throw new Error('tablesToIPC requires at least one table');
  }
  if (tables.length === 1) {
    return tableToIPC(tables[0], options);
  }
  return tableToIPC(concatTables(tables), options);
}`,
        replace: `export function tablesToIPC(tables, options) {
  if (!tables || tables.length === 0) {
    throw new Error('tablesToIPC requires at least one table');
  }
  // Pull per-table custom_metadata from each input (vgi-rpc's common
  // shape: single-batch tables carrying log_level / log_message /
  // server_id / request_id). The caller can override entirely by
  // passing \`options.batchMetadata\`. Zero-batch tables (no rows + no
  // columns OR empty column data) synthesise an empty record-batch
  // entry inside \`tableToIPC\` so the message is still visible on the
  // wire.
  let opts = options;
  if (!opts?.batchMetadata) {
    const md = [];
    for (const t of tables) {
      const tm = t?._vgiRecordMetadata;
      const actualBatches = t?.children?.[0]?.data?.length ?? 0;
      // Force at least one slot when the table carries metadata, so
      // even zero-batch tables (vgi-rpc's metadata-only EXCEPTION / log
      // batches) emit a RecordBatch message on the wire.
      const nBatches = Math.max(actualBatches, tm ? 1 : 0);
      for (let i = 0; i < nBatches; i++) md.push(i === 0 ? tm : undefined);
    }
    if (md.some((m) => m && m.size > 0)) {
      opts = { ...(options ?? {}), batchMetadata: md };
    }
  }
  if (tables.length === 1) {
    return tableToIPC(tables[0], opts);
  }
  return tableToIPC(concatTables(tables), opts);
}`,
      },
    ],
  },

  // ---------------------------------------------------------------------
  // 6. encode/table-to-ipc.js — accept batchMetadata option, synthesise
  //    metadata-only RecordBatch messages when needed, plus helper for
  //    per-type empty FieldNode/region emission.
  // ---------------------------------------------------------------------
  {
    file: "src/encode/table-to-ipc.js",
    edits: [
      {
        find: `  const { dictionaries, idMap } = assembleDictionaryBatches(columns, codec);
  const records = assembleRecordBatches(columns, codec);
  const schema = assembleSchema(table.schema, idMap);
  const data = { schema, dictionaries, records };
  return encodeIPC(data, { ...options, codec: id }).finish();
}`,
        replace: `  const { dictionaries, idMap } = assembleDictionaryBatches(columns, codec);
  const records = assembleRecordBatches(columns, codec);
  const schema = assembleSchema(table.schema, idMap);

  // Per-record-batch custom_metadata — Arrow IPC Message field 4. The
  // caller threads in an array (indexed by batch position) via
  // \`options.batchMetadata\`; encodeIPC attaches it during message
  // emission. Absent / undefined entries encode as no metadata, matching
  // the pre-feature behaviour for plain data batches.
  const batchMetadata = options?.batchMetadata;
  if (batchMetadata) {
    // Synthesise empty record batches for metadata entries past
    // \`records.length\`. Two shapes flow here:
    //  - zero-field schema (vgi-rpc EXCEPTION/log batch): no FieldNodes,
    //    no regions. Just metadata.
    //  - schema with fields but no actual column data (vgi-rpc result
    //    schema receiving an error path): emit one FieldNode per field
    //    with length=0/nullCount=0 + the validity/offset/value regions
    //    each type would consume. Without these the wire batch's body
    //    layout disagrees with what pyarrow expects for the schema.
    while (records.length < batchMetadata.length) {
      const nodes = [];
      const regions = [];
      const variadic = [];
      for (const f of schema.fields) {
        appendEmptyNodes(f.type, nodes, regions, variadic);
      }
      records.push({ length: 0, nodes, regions, variadic, buffers: [], byteLength: 0 });
    }
    for (let i = 0; i < records.length; i++) {
      const md = batchMetadata[i];
      if (md && md.size > 0) records[i].metadata = md;
    }
  }

  const data = { schema, dictionaries, records };
  return encodeIPC(data, { ...options, codec: id }).finish();
}

/** Append FieldNodes + region descriptors for a length-0 column of
 *  \`type\`. Mirrors \`visit()\`'s type → buffer mapping but emits
 *  zero-sized regions so the synthesised empty record batch survives
 *  schema validation on the reader side. */
function appendEmptyNodes(type, nodes, regions, variadic) {
  nodes.push({ length: 0, nullCount: 0 });
  if (type.typeId === Type.Null) return;
  switch (type.typeId) {
    case Type.Bool:
    case Type.Int:
    case Type.Time:
    case Type.Duration:
    case Type.Float:
    case Type.Date:
    case Type.Timestamp:
    case Type.Decimal:
    case Type.Interval:
    case Type.FixedSizeBinary:
    case Type.Dictionary:
      regions.push({ offset: 0, length: 0 });
      regions.push({ offset: 0, length: 0 });
      return;
    case Type.Utf8:
    case Type.LargeUtf8:
    case Type.Binary:
    case Type.LargeBinary:
      regions.push({ offset: 0, length: 0 });
      regions.push({ offset: 0, length: 0 });
      regions.push({ offset: 0, length: 0 });
      return;
    case Type.BinaryView:
    case Type.Utf8View:
      regions.push({ offset: 0, length: 0 });
      regions.push({ offset: 0, length: 0 });
      variadic.push(0);
      return;
    case Type.List:
    case Type.LargeList:
    case Type.Map:
      regions.push({ offset: 0, length: 0 });
      regions.push({ offset: 0, length: 0 });
      type.children?.forEach((c) => appendEmptyNodes(c.type, nodes, regions, variadic));
      return;
    case Type.ListView:
    case Type.LargeListView:
      regions.push({ offset: 0, length: 0 });
      regions.push({ offset: 0, length: 0 });
      regions.push({ offset: 0, length: 0 });
      type.children?.forEach((c) => appendEmptyNodes(c.type, nodes, regions, variadic));
      return;
    case Type.FixedSizeList:
    case Type.Struct:
      regions.push({ offset: 0, length: 0 });
      type.children?.forEach((c) => appendEmptyNodes(c.type, nodes, regions, variadic));
      return;
    case Type.RunEndEncoded:
      type.children?.forEach((c) => appendEmptyNodes(c.type, nodes, regions, variadic));
      return;
    case Type.Union:
      regions.push({ offset: 0, length: 0 });
      if (type.mode === UnionMode.Dense) {
        regions.push({ offset: 0, length: 0 });
      }
      type.children?.forEach((c) => appendEmptyNodes(c.type, nodes, regions, variadic));
      return;
    default:
      regions.push({ offset: 0, length: 0 });
      regions.push({ offset: 0, length: 0 });
  }
}`,
      },
    ],
  },

  // ---------------------------------------------------------------------
  // 7. decode/message.js — read Message field 4 (custom_metadata) and
  //    surface it on the decoded RecordBatch / DictionaryBatch content.
  // ---------------------------------------------------------------------
  {
    file: "src/decode/message.js",
    edits: [
      {
        find: "import { decodeRecordBatch } from './record-batch.js';",
        replace:
          "import { decodeMetadata } from './metadata.js';\nimport { decodeRecordBatch } from './record-batch.js';",
      },
      {
        find: `  const bodyLength = get(10, readInt64, 0);
  let content;`,
        replace: `  const bodyLength = get(10, readInt64, 0);
  const customMetadata = get(12, decodeMetadata);
  let content;`,
      },
      {
        find: `    if (!decoder) throw new Error(invalidMessageType(type));
    content = decoder(head, offset, version);

    // extract message body`,
        replace: `    if (!decoder) throw new Error(invalidMessageType(type));
    content = decoder(head, offset, version);
    // Surface per-message custom_metadata on the decoded content so
    // consumers (vgi-rpc reads per-record-batch metadata for log_level /
    // log_message / server_id / request_id) can retrieve it without
    // re-parsing the FlatBuffer. Schema messages already have their own
    // metadata field — only attach for record / dictionary batches.
    if (customMetadata && (type === MessageHeader.RecordBatch || type === MessageHeader.DictionaryBatch)) {
      content.metadata = customMetadata;
    }

    // extract message body`,
      },
    ],
  },

  // ---------------------------------------------------------------------
  // 8. decode/table-from-ipc.js — stash decoded per-batch metadata on
  //    the returned Table via `_vgiRecordMetadata` (first batch) +
  //    `_vgiRecordMetadataPerBatch` (positional) so the facade can read
  //    it through the existing `VgiBatch.metadata` shape.
  // ---------------------------------------------------------------------
  {
    file: "src/decode/table-from-ipc.js",
    edits: [
      {
        find: "  return new Table(schema, cols.map(c => c.done()), options.useProxy);\n}",
        replace: `  const table = new Table(schema, cols.map(c => c.done()), options.useProxy);
  // Surface per-record-batch custom_metadata for the common single-batch
  // case (vgi-rpc reads it from the EXCEPTION/log/result batch metadata).
  // For multi-batch tables, keep the full positional array under a
  // separate property; first-batch metadata stays addressable via the
  // shortcut so callers that don't care about batch boundaries (the
  // VgiBatch facade) don't need to know which form is set.
  if (records.length > 0) {
    const firstMd = records[0].metadata;
    if (firstMd && firstMd.size > 0) {
      table._vgiRecordMetadata = firstMd;
    }
    if (records.length > 1) {
      table._vgiRecordMetadataPerBatch = records.map(r => r.metadata ?? null);
    }
  }
  return table;
}`,
      },
    ],
  },
];

let appliedAny = false;
let alreadyApplied = 0;
for (const { file, edits } of PATCHES) {
  const path = resolve(ROOT, file);
  if (!existsSync(path)) {
    console.error(`patch-flechette: missing ${file} (skipping)`);
    continue;
  }
  let src = readFileSync(path, "utf8");
  let changed = false;
  for (const { find, replace } of edits) {
    if (src.includes(replace)) {
      // Already applied.
      alreadyApplied++;
      continue;
    }
    if (!src.includes(find)) {
      console.error(`patch-flechette: ${file} — could not locate text to patch:\n${find.split("\n")[0]}`);
      continue;
    }
    src = src.replace(find, replace);
    changed = true;
  }
  if (changed) {
    writeFileSync(path, src);
    appliedAny = true;
  }
}

if (appliedAny) {
  console.log("patch-flechette: applied per-record-batch custom_metadata patches");
} else if (alreadyApplied > 0) {
  // Quiet success — patches already in place.
}
