// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { float, Protocol, str, VgiRpcServer } from "../src/index.js";

const protocol = new Protocol("Greeter");

protocol.unary("greet", {
  params: { name: str },
  result: { result: str },
  handler: async ({ name }) => ({ result: `Hello, ${name}!` }),
  doc: "Greet someone by name.",
});

protocol.unary("add", {
  params: { a: float, b: float },
  result: { result: float },
  handler: async ({ a, b }) => ({ result: a + b }),
  doc: "Add two numbers.",
});

// `enableDescribe` hosts `vgi_rpc.Reflection.v1`, which is how a client
// discovers this service's methods. It is the default; spelled out here so the
// example says where introspection comes from.
const server = new VgiRpcServer(protocol, { enableDescribe: true });
server.run();
