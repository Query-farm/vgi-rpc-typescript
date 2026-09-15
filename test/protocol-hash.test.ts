import { describe, expect, test } from "bun:test";
import { TypeId } from "../src/arrow/predicates.js";
import type { VgiField } from "../src/arrow/types.js";
import { canonicalDescription, computeProtocolHash, type HashMethod } from "../src/protocol-hash.js";

const utf8 = { typeId: TypeId.Utf8 };
const field = (name: string, nullable = false): VgiField =>
  ({ name, type: utf8, nullable, metadata: new Map() }) as VgiField;

// The cross-port contract. These values are produced by the Python reference
// (vgi_rpc/rpc/_protocol_hash.py); a mismatch means this port and that one
// would disagree about whether they speak the same protocol.
describe("protocol hash", () => {
  test("matches the Python reference digest", async () => {
    const methods: HashMethod[] = [
      {
        name: "echo",
        methodType: "unary",
        hasReturn: true,
        hasHeader: false,
        paramsFields: [field("value")],
        resultFields: [field("result")],
      },
    ];
    expect(await computeProtocolHash("demo.Hash.v1", methods)).toBe(
      "a4b8ae57bf777c906081ff3610d435836b77dbc1f17a381c6a2febf1a2adb115",
    );
  });

  test("omits result for a method that returns nothing", () => {
    // Absent and empty are different and must not hash alike.
    const methods: HashMethod[] = [
      { name: "fire", methodType: "unary", hasReturn: false, hasHeader: false, paramsFields: [field("v")] },
    ];
    expect(canonicalDescription("demo.Void.v1", methods)).toBe(
      '{"methods":[{"has_header":false,"has_return":false,"name":"fire","params":[{"name":"v","nullable":false,"type":"utf8"}],"type":"unary"}],"protocol":"demo.Void.v1"}',
    );
  });

  test("sorts methods by name", async () => {
    // A port iterating a hash map must still produce this order.
    const mk = (name: string): HashMethod => ({
      name,
      methodType: "unary",
      hasReturn: false,
      hasHeader: false,
      paramsFields: [],
    });
    expect(await computeProtocolHash("p", [mk("a"), mk("b")])).toBe(await computeProtocolHash("p", [mk("b"), mk("a")]));
  });
});
