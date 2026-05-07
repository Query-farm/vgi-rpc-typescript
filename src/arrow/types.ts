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
