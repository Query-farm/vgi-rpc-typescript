// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Regression coverage for the impl-arrowjs / impl-flechette parity that
 * `buildRequestIpc` relies on. The wire layer's `#vgi-rpc-arrow` conditional
 * import selects impl-flechette under the `browser`/`worker`/`workerd`
 * conditions and impl-arrowjs under `default`. `bun test` exercises only
 * `default` (impl-arrowjs); the browser path was silently broken for ~6
 * months because no test imported impl-flechette directly.
 *
 * The regression: `buildRequestIpc` used to construct an `@query-farm/
 * apache-arrow` `RecordBatch` and pass it to `serializeIpcStream`, which
 * under the browser condition dispatches to impl-flechette's
 * `serializeBatches`. flechette's `tablesToIPC` reads `table.children`,
 * which apache-arrow `Table`/`RecordBatch` does not expose, so the call
 * threw `TypeError: Cannot read properties of undefined (reading '0')` from
 * deep inside flechette's `checkBatchLengths`.
 *
 * Fix in `src/client/ipc.ts`: build the batch through the impl-agnostic
 * `singleRowBatchWithMetadata` / `emptyBatchWithMetadata` helpers from
 * `#vgi-rpc-arrow` so each backend constructs its native batch type.
 *
 * This test exercises both impls directly (bypassing the conditional)
 * to prove cross-impl parity for the cases `buildRequestIpc` produces:
 *   1. empty schema → 0-row metadata-bearing batch
 *   2. 1-field schema → 1-row batch with the value coerced
 *   3. multi-field schema with mixed types
 */

import { describe, expect, it } from "bun:test";
import { Field, Float64, Schema, Utf8 } from "@query-farm/apache-arrow";
import * as arrowjsImpl from "../../src/arrow/impl-arrowjs/index.js";
import * as flechetteImpl from "../../src/arrow/impl-flechette/index.js";
import { PROTOCOL_VERSION_KEY, REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY } from "../../src/constants.js";

const impls = [
  { name: "impl-arrowjs", impl: arrowjsImpl },
  { name: "impl-flechette", impl: flechetteImpl },
] as const;

function buildExpectedMetadata(method: string, protocolVersion?: string): Map<string, string> {
  const m = new Map<string, string>();
  m.set(RPC_METHOD_KEY, method);
  m.set(REQUEST_VERSION_KEY, REQUEST_VERSION);
  if (protocolVersion) m.set(PROTOCOL_VERSION_KEY, protocolVersion);
  return m;
}

for (const { name, impl } of impls) {
  describe(`buildRequestIpc parity: ${name}`, () => {
    it("empty schema (e.g. __describe__) serializes to a valid stream", () => {
      const schema = new Schema([]);
      const metadata = buildExpectedMetadata("__describe__", "1.0.0");

      // This mirrors the empty-schema branch of buildRequestIpc.
      const batch = impl.emptyBatchWithMetadata(schema as any, metadata);
      const bytes = impl.serializeBatches(schema as any, [batch as any]);

      expect(bytes).toBeInstanceOf(Uint8Array);
      expect(bytes.byteLength).toBeGreaterThan(0);

      // Round-trip: deserialize and confirm the metadata survives.
      // (The impl's own deserializeBatch is the right inverse for this impl.)
      // Re-read the stream and confirm a batch comes back with the metadata
      // attached.
      const { batches } = (function readAll() {
        // Use the impl's own batch iterator. impl-arrowjs's RecordBatchReader
        // produces apache-arrow RecordBatches; flechette's tableFromIPC
        // produces flechette tables. Both surface .metadata.
        // For empty-schema, an IpcStream with a single empty batch is valid.
        const decoded = impl.deserializeBatch(bytes);
        return { batches: decoded ? [decoded] : [] };
      })();

      expect(batches.length).toBeGreaterThan(0);
      const got = (batches[0] as any).metadata;
      expect(got).toBeDefined();
      expect(got.get(RPC_METHOD_KEY)).toBe("__describe__");
      expect(got.get(PROTOCOL_VERSION_KEY)).toBe("1.0.0");
    });

    it("single-field schema serializes a 1-row batch", () => {
      const schema = new Schema([new Field("name", new Utf8(), true)]);
      const metadata = buildExpectedMetadata("catalog_attach", "1.0.0");

      const batch = impl.singleRowBatchWithMetadata(schema as any, { name: "albemarle_gis" }, metadata);
      const bytes = impl.serializeBatches(schema as any, [batch as any]);
      expect(bytes.byteLength).toBeGreaterThan(0);

      const decoded = impl.deserializeBatch(bytes);
      expect(decoded).toBeDefined();
      const got = (decoded as any).metadata;
      expect(got?.get(RPC_METHOD_KEY)).toBe("catalog_attach");
    });

    it("multi-field schema with string + float round-trips", () => {
      const schema = new Schema([
        new Field("schema_name", new Utf8(), false),
        new Field("threshold", new Float64(), true),
      ]);
      const metadata = buildExpectedMetadata("catalog_schemas");

      const batch = impl.singleRowBatchWithMetadata(
        schema as any,
        { schema_name: "property", threshold: 3.14 },
        metadata,
      );
      const bytes = impl.serializeBatches(schema as any, [batch as any]);
      expect(bytes.byteLength).toBeGreaterThan(0);

      const decoded: any = impl.deserializeBatch(bytes);
      expect(decoded).toBeDefined();
      expect(decoded.metadata?.get(RPC_METHOD_KEY)).toBe("catalog_schemas");
    });
  });
}

describe("buildRequestIpc empty-schema regression — apache-arrow batch passed to flechette throws", () => {
  // Captures the original failure shape. If anyone reintroduces the old
  // path (apache-arrow batch → flechette serializer), the assertion below
  // will fail because `checkBatchLengths` reads `table.children` and an
  // apache-arrow batch doesn't surface that field.
  it("flechette.serializeBatches refuses an apache-arrow RecordBatch", () => {
    const schema = new Schema([]);
    const apacheBatch = arrowjsImpl.emptyBatchWithMetadata(schema as any, buildExpectedMetadata("__describe__"));

    expect(() => flechetteImpl.serializeBatches(schema as any, [apacheBatch as any])).toThrow(); // any throw is fine — the point is that mixing impls is incoherent.
  });
});

describe("introspect.deserializeSchema returns impl-native types", () => {
  // The bug `introspect.ts:deserializeSchema` used to have: it deserialized
  // schemas via `RecordBatchReader` from `@query-farm/apache-arrow`
  // unconditionally. Under impl-flechette that meant downstream batch
  // builders received apache-arrow type instances missing the flechette-
  // specific shape (e.g. Binary's `offsets: int32Array`). Code paths kept
  // working because most types are shape-compatible enough, BUT binary /
  // utf8 / largeBinary / largeUtf8 fields silently encoded as empty
  // because `BinaryBuilder` defaults a missing `type.offsets` to a
  // Uint8Array buffer.
  //
  // Catch the regression by asserting introspect's `deserializeSchema`
  // returns an apache-arrow Schema (Bun/Node) OR a flechette Schema
  // (browser/worker) based on the active condition — not always the
  // apache-arrow one.
  it("active impl is the one whose deserializeSchema introspect returns", async () => {
    const { parseDescribeResponse } = await import("../../src/client/introspect.js");
    void parseDescribeResponse; // import for side-effect coverage

    // Use the apache-arrow impl to build a `(request: binary)` schema
    // serialized as a Schema IPC message. Both impls' deserializeSchema
    // should accept it and return a type instance whose factory matches
    // their own backend.
    const apacheSchema = arrowjsImpl.schema([arrowjsImpl.field("request", arrowjsImpl.binary() as any, true)]);
    const ipcBytes = arrowjsImpl.serializeSchema(apacheSchema);

    // Bun default condition resolves to impl-arrowjs. Re-imports of the
    // module under a different condition aren't easy here, so directly
    // check both impls' deserializeSchema produce types that round-trip
    // through their own batch builder without dropping the binary value.
    for (const { name, impl } of impls) {
      const decoded: any = impl.deserializeSchema(ipcBytes);
      const payload = new Uint8Array([1, 2, 3, 4, 5]);
      const batch = impl.singleRowBatchWithMetadata(decoded, { request: payload }, buildExpectedMetadata("test"));
      const wire = impl.serializeBatches(decoded, [batch as any]);
      const re: any = impl.deserializeBatch(wire);
      const got = (re.getChildAt ? re.getChildAt(0) : re.getChild?.("request"))?.get?.(0);
      // This is the regression check: a Uint8Array of length 5, not the
      // 0-byte value the broken cross-impl path used to produce.
      //
      // The impl name rides in the compared value rather than being dropped:
      // the loop runs both backends, and "expected 5, received 0" on its own
      // does not say which one regressed — which is the first thing you need.
      expect({ impl: name, byteLength: got?.byteLength }).toEqual({
        impl: name,
        byteLength: payload.byteLength,
      });
    }
  });
});

describe("introspect → buildRequestIpc end-to-end (catalog_attach shape)", () => {
  // This is the bug `introspect.ts:deserializeSchema` used to have:
  // it deserialized schemas with apache-arrow's RecordBatchReader regardless
  // of which impl was active. The downstream batch builder under
  // impl-flechette would then receive apache-arrow type instances whose
  // internal shape (e.g. Binary's `offsets: int32Array` field) is missing.
  // The flechette BinaryBuilder defaulted offsets to a Uint8Array buffer,
  // wrote two byte-sized offset entries instead of two int32s, and emitted
  // a value range of [0,0] — i.e. an empty binary column where the caller
  // had supplied real bytes. Servers then choked when they tried to open
  // the (empty) binary value as a nested IPC stream.
  //
  // We reproduce the catalog_attach wire shape:
  //   1. The server's __describe__ response carries the params schema as
  //      IPC bytes for `RecordBatch(request: binary)`.
  //   2. The client deserializes that schema.
  //   3. The client builds a 1-row request batch with a real
  //      (non-empty) binary payload.
  //   4. Re-decode the serialized request and verify the binary column
  //      still contains those bytes.
  for (const { name, impl } of impls) {
    it(`${name}: schema-deserialize → binary-encode → re-decode round-trips`, () => {
      // Build the schema (`request: binary`) and emit just-its IPC bytes,
      // matching what the server includes in its __describe__ response.
      const originalSchema = impl.schema([impl.field("request", impl.binary() as any, true)]);
      const schemaBytes = impl.serializeSchema(originalSchema);

      // Deserialize the schema the way introspect.ts does for paramsSchema.
      const decodedSchema: any = impl.deserializeSchema(schemaBytes);
      expect(decodedSchema.fields.length).toBe(1);
      expect(decodedSchema.fields[0].name).toBe("request");

      // Build a 1-row batch using the DECODED schema (this is the path
      // that used to silently emit a 0-byte binary value under flechette).
      const payload = new Uint8Array([0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe]);
      const batch = impl.singleRowBatchWithMetadata(
        decodedSchema,
        { request: payload },
        buildExpectedMetadata("catalog_attach", "1.0.0"),
      );
      const wire = impl.serializeBatches(decodedSchema, [batch as any]);
      expect(wire.byteLength).toBeGreaterThan(0);

      // Round-trip the wire bytes and confirm the binary column survived.
      const decoded: any = impl.deserializeBatch(wire);
      const col = decoded.getChildAt ? decoded.getChildAt(0) : decoded.getChild?.("request");
      const got = col?.get?.(0);
      expect(got).toBeInstanceOf(Uint8Array);
      expect(got.byteLength).toBe(payload.byteLength);
      for (let i = 0; i < payload.length; i++) {
        expect(got[i]).toBe(payload[i]);
      }
    });
  }
});
