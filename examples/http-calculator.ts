import { Protocol, float, createHttpHandler } from "../src/index.js";

const protocol = new Protocol("Calculator");

protocol.unary("add", {
  params: { a: float, b: float },
  result: { result: float },
  handler: async ({ a, b }) => ({ result: a + b }),
  doc: "Add two numbers.",
});

protocol.unary("multiply", {
  params: { a: float, b: float },
  result: { result: float },
  handler: async ({ a, b }) => ({ result: a * b }),
  doc: "Multiply two numbers.",
});

protocol.unary("divide", {
  params: { a: float, b: float },
  result: { result: float },
  handler: async ({ a, b }) => {
    if (b === 0) throw new Error("Division by zero");
    return { result: a / b };
  },
  doc: "Divide two numbers.",
});

const handler = createHttpHandler(protocol, {
  prefix: "/vgi",
  corsOrigins: "*",
});

const server = Bun.serve({
  port: 8080,
  fetch: handler,
});

console.log(`HTTP Calculator server listening on http://localhost:${server.port}`);
