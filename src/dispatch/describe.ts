import {
  Schema,
  Field,
  RecordBatch,
  Utf8,
  Bool,
  Binary,
  vectorFromArray,
  makeData,
  Struct,
} from "apache-arrow";
import type { MethodDefinition } from "../types.js";
import {
  PROTOCOL_NAME_KEY,
  REQUEST_VERSION_KEY,
  REQUEST_VERSION,
  DESCRIBE_VERSION_KEY,
  DESCRIBE_VERSION,
  SERVER_ID_KEY,
} from "../constants.js";
import { serializeSchema } from "../util/schema.js";

/**
 * The schema for the __describe__ response, matching Python's _DESCRIBE_SCHEMA.
 */
export const DESCRIBE_SCHEMA = new Schema([
  new Field("name", new Utf8(), false),
  new Field("method_type", new Utf8(), false),
  new Field("doc", new Utf8(), true),
  new Field("has_return", new Bool(), false),
  new Field("params_schema_ipc", new Binary(), false),
  new Field("result_schema_ipc", new Binary(), false),
  new Field("param_types_json", new Utf8(), true),
  new Field("param_defaults_json", new Utf8(), true),
  new Field("has_header", new Bool(), false),
  new Field("header_schema_ipc", new Binary(), true),
]);

/**
 * Build the __describe__ response batch and metadata.
 */
export function buildDescribeBatch(
  protocolName: string,
  methods: Map<string, MethodDefinition>,
  serverId: string,
): { batch: RecordBatch; metadata: Map<string, string> } {
  // Sort methods by name for consistent ordering
  const sortedEntries = [...methods.entries()].sort(([a], [b]) =>
    a.localeCompare(b),
  );

  const names: (string | null)[] = [];
  const methodTypes: (string | null)[] = [];
  const docs: (string | null)[] = [];
  const hasReturns: boolean[] = [];
  const paramsSchemas: (Uint8Array | null)[] = [];
  const resultSchemas: (Uint8Array | null)[] = [];
  const paramTypesJsons: (string | null)[] = [];
  const paramDefaultsJsons: (string | null)[] = [];
  const hasHeaders: boolean[] = [];
  const headerSchemas: (Uint8Array | null)[] = [];

  for (const [name, method] of sortedEntries) {
    names.push(name);
    methodTypes.push(method.type);
    docs.push(method.doc ?? null);

    // Unary methods with non-empty result schema have a return value
    const hasReturn =
      method.type === "unary" && method.resultSchema.fields.length > 0;
    hasReturns.push(hasReturn);

    paramsSchemas.push(serializeSchema(method.paramsSchema));
    resultSchemas.push(serializeSchema(method.resultSchema));

    // Build param_types_json
    if (method.paramTypes && Object.keys(method.paramTypes).length > 0) {
      paramTypesJsons.push(JSON.stringify(method.paramTypes));
    } else {
      paramTypesJsons.push(null);
    }

    // Build param_defaults_json
    if (method.defaults && Object.keys(method.defaults).length > 0) {
      const safe: Record<string, any> = {};
      for (const [k, v] of Object.entries(method.defaults)) {
        if (
          v === null ||
          typeof v === "string" ||
          typeof v === "number" ||
          typeof v === "boolean"
        ) {
          safe[k] = v;
        }
      }
      paramDefaultsJsons.push(
        Object.keys(safe).length > 0 ? JSON.stringify(safe) : null,
      );
    } else {
      paramDefaultsJsons.push(null);
    }

    hasHeaders.push(!!method.headerSchema);
    headerSchemas.push(
      method.headerSchema ? serializeSchema(method.headerSchema) : null,
    );
  }

  // Build the batch using vectorFromArray for each column
  const nameArr = vectorFromArray(names, new Utf8());
  const methodTypeArr = vectorFromArray(methodTypes, new Utf8());
  const docArr = vectorFromArray(docs, new Utf8());
  const hasReturnArr = vectorFromArray(hasReturns, new Bool());
  const paramsSchemaArr = vectorFromArray(paramsSchemas, new Binary());
  const resultSchemaArr = vectorFromArray(resultSchemas, new Binary());
  const paramTypesArr = vectorFromArray(paramTypesJsons, new Utf8());
  const paramDefaultsArr = vectorFromArray(paramDefaultsJsons, new Utf8());
  const hasHeaderArr = vectorFromArray(hasHeaders, new Bool());
  const headerSchemaArr = vectorFromArray(headerSchemas, new Binary());

  const children = [
    nameArr.data[0],
    methodTypeArr.data[0],
    docArr.data[0],
    hasReturnArr.data[0],
    paramsSchemaArr.data[0],
    resultSchemaArr.data[0],
    paramTypesArr.data[0],
    paramDefaultsArr.data[0],
    hasHeaderArr.data[0],
    headerSchemaArr.data[0],
  ];

  const structType = new Struct(DESCRIBE_SCHEMA.fields);
  const data = makeData({
    type: structType,
    length: sortedEntries.length,
    children,
    nullCount: 0,
  });

  // Build metadata for the batch
  const metadata = new Map<string, string>();
  metadata.set(PROTOCOL_NAME_KEY, protocolName);
  metadata.set(REQUEST_VERSION_KEY, REQUEST_VERSION);
  metadata.set(DESCRIBE_VERSION_KEY, DESCRIBE_VERSION);
  metadata.set(SERVER_ID_KEY, serverId);

  const batch = new RecordBatch(DESCRIBE_SCHEMA, data, metadata);

  return { batch, metadata };
}
