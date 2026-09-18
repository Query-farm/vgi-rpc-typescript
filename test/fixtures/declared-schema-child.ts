// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Child process for test/flechette-declared-schema.test.ts. The Arrow backend
// is chosen when `#vgi-rpc-arrow` resolves, so a process sees exactly one --
// the parent runs this under `--conditions=flechette` and under the default
// (arrow-js) to compare them. Prints one JSON line on stdout.
//
//   bun [--conditions=flechette] run test/fixtures/declared-schema-child.ts request
//     -> { backend, bytes }: base64 of reflectionRequest("describe", "demo.v1")
//   bun [--conditions=flechette] run test/fixtures/declared-schema-child.ts http
//     -> { backend, protocols, greeting }: an in-process HTTP client on this
//        backend describes, then calls, a strict in-process server

import { backend } from "#vgi-rpc-arrow";
import { reflectionRequest } from "../../src/client/introspect.js";
import { createHttpHandler } from "../../src/http/handler.js";
import { httpConnect, Protocol, str } from "../../src/index.js";

const mode = process.argv[2];

if (mode === "request") {
  const bytes = reflectionRequest("describe", "demo.v1");
  console.log(JSON.stringify({ backend: backend.name, bytes: Buffer.from(bytes).toString("base64") }));
} else if (mode === "http") {
  const protocol = new Protocol("demo.v1");
  protocol.unary("greet", {
    // `str` is a non-nullable field: the server checks the request's
    // parameter schema against this exactly.
    params: { name: str },
    result: { result: str },
    handler: async ({ name }) => ({ result: `Hello, ${name}!` }),
  });
  const handler = createHttpHandler(protocol, { prefix: "" });
  const server = Bun.serve({ port: 0, fetch: handler });
  try {
    const client = httpConnect(`http://127.0.0.1:${server.port}`);
    const description = await client.describe();
    const reply = await client.call("greet", { name: "flechette" });
    console.log(
      JSON.stringify({
        backend: backend.name,
        protocol: description.protocolName,
        methods: description.methods.map((m) => m.name).sort(),
        greeting: reply?.result ?? null,
      }),
    );
  } finally {
    server.stop(true);
  }
} else {
  throw new Error(`unknown mode ${mode}`);
}
