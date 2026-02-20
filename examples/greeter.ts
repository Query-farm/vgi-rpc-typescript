import { Protocol, VgiRpcServer, str, float } from "../src/index.js";

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

const server = new VgiRpcServer(protocol, { enableDescribe: true });
server.run();
