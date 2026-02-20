import { RecordBatchStreamWriter, type Schema } from "apache-arrow";

/**
 * Serialize a Schema to the Arrow IPC Schema message format.
 * This produces bytes compatible with Python's `pa.ipc.read_schema()`.
 *
 * We serialize by writing an empty-batch IPC stream and extracting
 * the bytes, which includes the schema message. Python's read_schema()
 * uses `pa.ipc.read_schema(pa.py_buffer(bytes))` which expects
 * the schema flatbuffer message bytes directly — but the Python side
 * actually uses `schema.serialize()` which produces Schema message bytes.
 *
 * In arrow-js, we can get the equivalent by using Message.from(schema)
 * and encoding it, or by serializing a zero-batch stream.
 *
 * The Python `schema.serialize()` produces the Schema flatbuffer message bytes,
 * and `pa.ipc.read_schema()` expects an IPC stream containing a schema message.
 * The actual format is: continuation marker (0xFFFFFFFF) + length + flatbuffer bytes.
 */
export function serializeSchema(schema: Schema): Uint8Array {
  // Write a complete IPC stream with no batches.
  // This writes: Schema message + EOS marker.
  // Python's pa.ipc.read_schema() can read this format.
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, schema);
  writer.close();
  return writer.toUint8Array(true);
}
