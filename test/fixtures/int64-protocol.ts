// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// A tiny protocol whose results carry int64 values on both sides of
// Number.MAX_SAFE_INTEGER, for test/client-int64-mode.test.ts. Imported
// in-process by the HTTP lane and run as a child by the pipe lane.

import { int, type OutputCollector, Protocol, str, VgiRpcServer } from "../../src/index.js";

export const BIG = 2n ** 60n;

export const protocol = new Protocol("demo.int64.v1")
  .unary("pair", {
    params: { label: str },
    result: { small: int, big: int, label: str },
    handler: async ({ label }) => ({ small: 7n, big: BIG, label }),
  })
  .producer<{ done: boolean }>("values", {
    params: {},
    outputSchema: { v: int },
    headerSchema: { small: int, big: int },
    headerInit: () => ({ small: 3n, big: BIG }),
    init: async () => ({ done: false }),
    produce: async (state: { done: boolean }, out: OutputCollector) => {
      if (state.done) {
        out.finish();
        return;
      }
      state.done = true;
      out.emit({ v: [1n, BIG, -5n] });
    },
  });

if (import.meta.main) {
  new VgiRpcServer(protocol, { enableDescribe: true }).run();
}
