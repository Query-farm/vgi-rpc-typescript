// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// The public HTTP surface for intermediaries: the `__upload_url__` wire
// contract (mirrors Python's `test_public_upload_url_contract_is_exported`)
// and `decodeContentEncoding` (mirrors Python's `TestDecodeContentEncoding`).

import { describe, expect, test } from "bun:test";
import {
  decodeContentEncoding,
  MAX_UPLOAD_URL_COUNT,
  UPLOAD_URL_METHOD,
  UPLOAD_URL_PARAMS_SCHEMA,
  UPLOAD_URL_RESPONSE_SCHEMA,
} from "../../src/http/index.js";
import { gzipCompress } from "../../src/util/gzip.js";
import { isZstdCompressAvailable, zstdCompress } from "../../src/util/zstd.js";

describe("__upload_url__ public wire contract", () => {
  test("the contract is exported so intermediaries needn't copy it", () => {
    expect(UPLOAD_URL_METHOD).toBe("__upload_url__");
    expect(MAX_UPLOAD_URL_COUNT).toBe(100);
    expect(UPLOAD_URL_PARAMS_SCHEMA.fields.map((f) => f.name)).toEqual(["count"]);
    expect(UPLOAD_URL_RESPONSE_SCHEMA.fields.map((f) => f.name)).toEqual(["upload_url", "download_url", "expires_at"]);
  });
});

describe("decodeContentEncoding", () => {
  const payload = new TextEncoder().encode("hello");

  test.skipIf(!isZstdCompressAvailable())("decodes zstd", async () => {
    const compressed = await zstdCompress(payload, 3);
    expect(await decodeContentEncoding(compressed, "zstd")).toEqual(payload);
  });

  test("decodes gzip", async () => {
    const compressed = await gzipCompress(payload);
    expect(await decodeContentEncoding(compressed, "gzip")).toEqual(payload);
  });

  test("passes through when absent or identity", async () => {
    const plain = new TextEncoder().encode("plain");
    expect(await decodeContentEncoding(plain, null)).toEqual(plain);
    expect(await decodeContentEncoding(plain, undefined)).toEqual(plain);
    expect(await decodeContentEncoding(plain, "identity")).toEqual(plain);
    expect(await decodeContentEncoding(plain, "")).toEqual(plain);
  });

  test.skipIf(!isZstdCompressAvailable())("decodes a multi-coding list in reverse order", async () => {
    // Content-Encoding: gzip, zstd — gzip applied first, zstd second, so
    // decoding runs zstd then gzip.
    const doubly = await zstdCompress(await gzipCompress(payload), 3);
    expect(await decodeContentEncoding(doubly, "gzip, zstd")).toEqual(payload);
  });
});
