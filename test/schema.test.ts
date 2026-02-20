import { describe, it, expect } from "bun:test";
import {
  Schema,
  Field,
  Utf8,
  Binary,
  Float64,
  Float32,
  Int64,
  Int32,
  Int16,
  Bool,
  List,
  Map_,
  Dictionary,
} from "apache-arrow";
import { toSchema, inferParamTypes, str, bytes, int, int32, float, float32, bool } from "../src/schema.js";

describe("toSchema", () => {
  it("passes Schema through unchanged", () => {
    const schema = new Schema([new Field("x", new Utf8(), false)]);
    const result = toSchema(schema);
    expect(result).toBe(schema);
  });

  it("converts Record<string, DataType> to Schema with non-nullable fields", () => {
    const result = toSchema({ name: str, age: int });
    expect(result).toBeInstanceOf(Schema);
    expect(result.fields).toHaveLength(2);
    expect(result.fields[0].name).toBe("name");
    expect(result.fields[0].type).toBeInstanceOf(Utf8);
    expect(result.fields[0].nullable).toBe(false);
    expect(result.fields[1].name).toBe("age");
    expect(result.fields[1].type).toBeInstanceOf(Int64);
    expect(result.fields[1].nullable).toBe(false);
  });

  it("converts Record<string, Field> preserving nullability", () => {
    const result = toSchema({
      name: new Field("name", new Utf8(), true),
      age: new Field("age", new Int64(), false),
    });
    expect(result.fields[0].nullable).toBe(true);
    expect(result.fields[1].nullable).toBe(false);
  });

  it("handles mixed DataType and Field values", () => {
    const result = toSchema({
      name: str,
      age: new Field("age", new Int64(), true),
    });
    expect(result.fields).toHaveLength(2);
    expect(result.fields[0].nullable).toBe(false);
    expect(result.fields[1].nullable).toBe(true);
  });

  it("converts empty {} to empty Schema", () => {
    const result = toSchema({});
    expect(result).toBeInstanceOf(Schema);
    expect(result.fields).toHaveLength(0);
  });

  it("throws on invalid values", () => {
    expect(() => toSchema({ name: "string" as any })).toThrow(TypeError);
    expect(() => toSchema({ name: 42 as any })).toThrow(TypeError);
  });
});

describe("inferParamTypes", () => {
  it("maps Utf8 → str", () => {
    const result = inferParamTypes({ name: str });
    expect(result).toEqual({ name: "str" });
  });

  it("maps Binary → bytes", () => {
    const result = inferParamTypes({ data: bytes });
    expect(result).toEqual({ data: "bytes" });
  });

  it("maps Float64 → float", () => {
    const result = inferParamTypes({ value: float });
    expect(result).toEqual({ value: "float" });
  });

  it("maps Int64 → int", () => {
    const result = inferParamTypes({ count: int });
    expect(result).toEqual({ count: "int" });
  });

  it("maps Bool → bool", () => {
    const result = inferParamTypes({ flag: bool });
    expect(result).toEqual({ flag: "bool" });
  });

  it("maps Float32 → float", () => {
    const result = inferParamTypes({ value: float32 });
    expect(result).toEqual({ value: "float" });
  });

  it("maps Int32 → int", () => {
    const result = inferParamTypes({ count: int32 });
    expect(result).toEqual({ count: "int" });
  });

  it("maps Int16 → int", () => {
    const result = inferParamTypes({
      count: new Field("count", new Int16(), false),
    });
    expect(result).toEqual({ count: "int" });
  });

  it("returns undefined for empty schemas", () => {
    expect(inferParamTypes({})).toBeUndefined();
  });

  it("returns undefined if any field is a List", () => {
    const schema = new Schema([
      new Field("items", new List(new Field("item", new Utf8(), false)), false),
    ]);
    expect(inferParamTypes(schema)).toBeUndefined();
  });

  it("returns undefined if any field is a Dictionary", () => {
    const schema = new Schema([
      new Field("cat", new Dictionary(new Utf8(), new Int32()), false),
    ]);
    expect(inferParamTypes(schema)).toBeUndefined();
  });

  it("works with SchemaLike input", () => {
    const result = inferParamTypes({ name: str, value: float });
    expect(result).toEqual({ name: "str", value: "float" });
  });
});
