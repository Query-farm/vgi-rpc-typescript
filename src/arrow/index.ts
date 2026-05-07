// vgi-rpc-typescript Arrow facade. Mirrors vgi-typescript's facade so types
// flow freely between the two packages. Backend selected via `#vgi-rpc-arrow`
// in package.json's `imports` field (workerd/worker/browser → impl-flechette,
// default → impl-arrowjs).

export type {
  VgiTypeId,
  VgiDataType,
  VgiField,
  VgiSchema,
  VgiColumn,
  VgiColumnData,
  VgiBatch,
  VgiBackendInfo,
} from "./types.js";

export {
  TypeId,
  isNull, isInt, isFloat, isBinary, isUtf8, isLargeUtf8, isLargeBinary,
  isBool, isDecimal, isDate, isTime, isTimestamp, isDuration, isList,
  isStruct, isMap, isFixedSizeBinary, isDictionary, isBatch,
} from "./predicates.js";

export {
  backend,
  // Type factories
  nullType, bool,
  int8, int16, int32, int64,
  uint8, uint16, uint32, uint64,
  float32, float64,
  utf8, binary,
  timestampMicro,
  field, schema,
  // IPC
  serializeSchema, deserializeSchema,
  serializeBatch, deserializeBatch,
  // Construction
  columnFromArray,
  singleRowBatch,
  batchFromColumns,
  batchFromColumnData,
  emptyColumnData,
  emptyBatchWithMetadata,
  singleRowBatchWithMetadata,
  isOpaqueData,
  withBatchMetadata,
  serializeBatches,
  conformBatchToSchema,
} from "#vgi-rpc-arrow";
