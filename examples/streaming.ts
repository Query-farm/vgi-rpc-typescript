import { type RecordBatch } from "apache-arrow";
import { Protocol, VgiRpcServer, int32, float, type OutputCollector } from "../src/index.js";

const protocol = new Protocol("Streaming");

// Producer: count(limit: int, batch_size: int) → stream of {n: int, n_squared: int}

protocol.producer<{ limit: number; current: number; batchSize: number }>("count", {
  params: { limit: int32, batch_size: int32 },
  outputSchema: { n: int32, n_squared: int32 },
  init: async ({ limit, batch_size }) => ({ limit, current: 0, batchSize: batch_size }),
  produce: async (state, out) => {
    if (state.current >= state.limit) {
      out.finish();
      return;
    }

    const remaining = state.limit - state.current;
    const count = Math.min(state.batchSize, remaining);

    const nValues: number[] = [];
    const sqValues: number[] = [];
    for (let i = 0; i < count; i++) {
      const n = state.current + i;
      nValues.push(n);
      sqValues.push(n * n);
    }
    state.current += count;

    out.emit({ n: nValues, n_squared: sqValues });
  },
  doc: "Count from 0 to limit-1, emitting n and n_squared.",
  defaults: { batch_size: 1 },
  paramTypes: { limit: "int", batch_size: "int" },
});

// Exchange: scale(factor: float) → transform {value: float} → {value: float}

protocol.exchange<{ factor: number }>("scale", {
  params: { factor: float },
  inputSchema: { value: float },
  outputSchema: { value: float },
  init: async ({ factor }) => ({ factor }),
  exchange: async (state, input: RecordBatch, out: OutputCollector) => {
    const value = input.getChildAt(0)?.get(0) as number;
    out.emitRow({ value: value * state.factor });
  },
  doc: "Scale input values by a factor.",
});

const server = new VgiRpcServer(protocol, { enableDescribe: true });
server.run();
