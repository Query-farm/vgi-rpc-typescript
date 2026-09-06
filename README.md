[![VGI — The Vector Gateway Interface](https://vgi-rpc.query.farm/logo-hero.png)](https://vgi-rpc.query.farm)

<h1 align="center">vgi-rpc</h1>

<p align="center">
  Transport-agnostic RPC framework built on <a href="https://arrow.apache.org/">Apache Arrow</a> IPC serialization — the TypeScript port of <a href="https://github.com/Query-farm/vgi-rpc-python">vgi-rpc</a>.<br>
  Built by <a href="https://query.farm">🚜 Query.Farm</a>
</p>

<p align="center">
  <a href="https://github.com/Query-farm/vgi-rpc-typescript/actions/workflows/ci.yml"><img src="https://github.com/Query-farm/vgi-rpc-typescript/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://www.npmjs.com/package/@query-farm/vgi-rpc"><img src="https://img.shields.io/npm/v/%40query-farm%2Fvgi-rpc" alt="npm"></a>
  <a href="https://www.npmjs.com/package/@query-farm/vgi-rpc"><img src="https://img.shields.io/npm/dm/%40query-farm%2Fvgi-rpc" alt="npm downloads"></a>
  <a href="LICENSE.md"><img src="https://img.shields.io/badge/license-Apache--2.0-blue" alt="License"></a>
</p>

TypeScript server library for the [vgi-rpc](https://vgi-rpc.query.farm) framework. Implements RPC servers that communicate over stdin/stdout using [Apache Arrow](https://arrow.apache.org/) IPC serialization.

Define RPC methods with Arrow-typed schemas, serve them over stdin/stdout, and interact with them using the Python `vgi-rpc` CLI or any `vgi-rpc` client. Unlike JSON-over-HTTP, structured data stays in Arrow columnar format for efficient transfer.

**Key features:**

- **Three method types** — unary (request-response), producer (server-streaming), and exchange (bidirectional-streaming)
- **Apache Arrow IPC wire format** — efficient columnar serialization compatible with the Python `vgi-rpc` framework
- **Schema shorthand** — declare schemas with `{ name: str, count: int }` instead of manual `Schema`/`Field` construction
- **Fluent protocol builder** — chain `.unary()`, `.producer()`, `.exchange()` calls to define your service
- **Type-safe streaming state** — generic `<S>` parameter threads state types through init and produce/exchange functions
- **Runtime introspection** — opt-in `__describe__` method for dynamic service discovery via the CLI
- **Result validation** — missing required fields in handler results throw descriptive errors at emit time
- **Authentication** — bearer tokens, JWT, mTLS (PEM-in-header and XFCC), with chainable authenticators
- **Six client transports** — HTTP, HTTP-over-Iroh (`httpiConnect`), raw Iroh (`irohConnect`), subprocess, raw pipe, and raw TCP, all sharing a unified `RpcClient` interface. Raw TCP carries no auth/TLS and defaults to loopback (`127.0.0.1`) — trusted networks only; use HTTP otherwise.

See [SPIFFE identity from trusted HTTP proxies](docs/spiffe-proxy-identity.md)
for Envoy, nginx, AWS ALB, Google Cloud Load Balancing, and Azure Application
Gateway deployment profiles.

## Installation

```bash
# Bun
bun add @query-farm/vgi-rpc

# npm
npm install @query-farm/vgi-rpc

# Deno
deno add npm:@query-farm/vgi-rpc
```

## Runtime Compatibility

`vgi-rpc` works with [Bun](https://bun.sh/), [Node.js](https://nodejs.org/) 22+, and [Deno](https://deno.land/) 2+. The HTTP handler and HTTP client are fully runtime-agnostic. The subprocess transport (`subprocessConnect`) requires Bun.

### Browser Usage

The HTTP client works in browsers. Use `httpConnect` to call a `vgi-rpc` server from any browser application:

```typescript
import { httpConnect } from "@query-farm/vgi-rpc";

const client = httpConnect("https://api.example.com");
const result = await client.call("add", { a: 2, b: 3 });
```

You will need a bundler (Vite, esbuild, webpack, etc.) that can resolve the `@query-farm/apache-arrow` dependency. Server-only exports (`VgiRpcServer`, `createHttpHandler`, `subprocessConnect`, `pipeConnect`) are not available in browsers.

## Quick Start

```typescript
import { Protocol, VgiRpcServer, str, float } from "@query-farm/vgi-rpc";

const protocol = new Protocol("Calculator");

protocol.unary("add", {
  params: { a: float, b: float },
  result: { result: float },
  handler: async ({ a, b }) => ({ result: a + b }),
  doc: "Add two numbers.",
});

protocol.unary("greet", {
  params: { name: str },
  result: { result: str },
  handler: async ({ name }) => ({ result: `Hello, ${name}!` }),
  doc: "Greet someone by name.",
});

const server = new VgiRpcServer(protocol, { enableDescribe: true });
server.run();
```

## Client

Connect to any vgi-rpc server programmatically:

```typescript
import { httpConnect, subprocessConnect } from "@query-farm/vgi-rpc";

// HTTP transport
const client = httpConnect("http://localhost:8080");
const result = await client.call("add", { a: 2, b: 3 });
console.log(result); // { result: 5 }
client.close();

// Subprocess transport (spawns server, communicates over pipes)
const client2 = subprocessConnect(["bun", "run", "server.ts"]);
const result2 = await client2.call("greet", { name: "World" });
console.log(result2); // { result: "Hello, World!" }
client2.close();
```

All transports share the same `RpcClient` interface: `call()`, `stream()`, `describe()`, `close()`.

### HTTP over Iroh (Node/Bun)

Install the optional native adapter and connect to the worker's canonical
64-hex Iroh EndpointId:

```bash
bun add @momics/iroh-http-node
```

```typescript
import { httpiConnect } from "@query-farm/vgi-rpc";

const client = await httpiConnect(
  "httpi://0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef/vgi",
  {
    // Optional discovery hints when the endpoint is not discoverable by relay.
    directAddresses: ["192.0.2.10:4433"],
    requestTimeoutMs: 30_000,
    authorization: "Bearer <token>",
  },
);
const result = await client.call("add", { a: 2, b: 3 });
client.close();
```

`httpiConnect` carries the ordinary VGI HTTP protocol over the `iroh-http/2`
ALPN. It therefore retains OPTIONS capability discovery, authorization,
compression, continuations, response budgets, and external-location handling.
The adapter accepts at most 256 MiB per HTTP response, matching its native
transport limit. The client owns and closes a node it creates. Pass `node` to
share an application-owned node; it remains open unless `closeNode: true` is
set.

This native adapter is for Node/Bun, not browser bundles. Browser Iroh clients
use the separately packaged Rust/WASM connector.

### Workers behind an Iroh bridge

Node/Bun raw workers use the loopback-safe convenience wrapper:

```typescript
import { serveIrohTcpUpstream } from "@query-farm/vgi-rpc";

await serveIrohTcpUpstream(protocol, {
  issuer: "production",
  port: 9400,
});
```

For HTTP workers, spread `irohHttpBridgeOptions(...)` into
`createHttpHandler`. Its required `peerResolutionContext` adapter must supply
the immediate socket peer and the raw, multiplicity-preserving header list;
this keeps a plain Fetch `Request` from becoming a header-spoofing trust
boundary. Both helpers default to authenticated EndpointIds and exact IPv4
loopback trust; observation mode is an explicit `authenticate: false`.

## Testing with the Python CLI

Test it with the Python CLI:

```bash
# Describe the service
vgi-rpc --cmd "bun run server.ts" describe

# Call a method
vgi-rpc --cmd "bun run server.ts" call add a=1.0 b=2.0
# {"result": 3.0}

vgi-rpc --cmd "bun run server.ts" call greet name=World
# {"result": "Hello, World!"}
```

## Defining Methods

### Unary

Single request, single response:

```typescript
protocol.unary("add", {
  params: { a: float, b: float },
  result: { result: float },
  handler: async ({ a, b }) => ({ result: a + b }),
  doc: "Add two numbers.",
  defaults: { b: 1.0 },
});
```

The handler receives parsed parameters and returns a record matching the result schema. The optional `defaults` field provides default values for omitted parameters.

### Producer (Server Streaming)

The server produces multiple output batches. The generic `<S>` parameter infers state types from the `init` return value:

```typescript
protocol.producer<{ limit: number; current: number }>("count", {
  params: { limit: int32, batch_size: int32 },
  outputSchema: { n: int32, n_squared: int32 },
  init: async ({ limit, batch_size }) => ({
    limit,
    current: 0,
    batchSize: batch_size,
  }),
  produce: async (state, out) => {
    if (state.current >= state.limit) {
      out.finish();
      return;
    }
    out.emitRow({ n: state.current, n_squared: state.current ** 2 });
    state.current++;
  },
  doc: "Count from 0 to limit-1.",
  defaults: { batch_size: 1 },
});
```

`produce` is called repeatedly. Call `out.finish()` to end the stream. Mutate `state` in-place between calls.

### Exchange (Bidirectional Streaming)

Client sends batches, server responds one output batch per input batch:

```typescript
protocol.exchange<{ factor: number }>("scale", {
  params: { factor: float },
  inputSchema: { value: float },
  outputSchema: { value: float },
  init: async ({ factor }) => ({ factor }),
  exchange: async (state, input, out) => {
    const value = input.getChildAt(0)?.get(0) as number;
    out.emitRow({ value: value * state.factor });
  },
  doc: "Scale input values by a factor.",
});
```

### Stream Headers

Producer and exchange methods can send a one-time header before the data stream:

```typescript
protocol.producer<{ count: number; current: number }>("produce_with_header", {
  params: { count: int },
  outputSchema: { index: int, value: int },
  headerSchema: { total_expected: int, description: str },
  headerInit: (params) => ({
    total_expected: params.count,
    description: `producing ${params.count} batches`,
  }),
  init: ({ count }) => ({ count, current: 0 }),
  produce: (state, out) => {
    if (state.current >= state.count) {
      out.finish();
      return;
    }
    out.emitRow({ index: state.current, value: state.current * 10 });
    state.current++;
  },
});
```

## Schema Shorthand

Declare schemas using convenient type singletons instead of manual `Schema`/`Field` construction:

```typescript
import { str, bytes, int, int32, float, float32, bool } from "@query-farm/vgi-rpc";

// Shorthand
protocol.unary("echo", {
  params: { name: str, count: int, value: float },
  result: { result: str },
  handler: ({ name }) => ({ result: name }),
});

// Equivalent verbose form
import { Schema, Field, Utf8, Int64, Float64 } from "@query-farm/apache-arrow";

protocol.unary("echo", {
  params: new Schema([
    new Field("name", new Utf8(), false),
    new Field("count", new Int64(), false),
    new Field("value", new Float64(), false),
  ]),
  result: new Schema([new Field("result", new Utf8(), false)]),
  handler: ({ name }) => ({ result: name }),
});
```

### Type singletons

| Singleton | Arrow Type | Python equivalent |
|-----------|-----------|-------------------|
| `str` | Utf8 | `str` |
| `bytes` | Binary | `bytes` |
| `int` | Int64 | `int` |
| `int32` | Int32 | `Annotated[int, ArrowType(pa.int32())]` |
| `float` | Float64 | `float` |
| `float32` | Float32 | `Annotated[float, ArrowType(pa.float32())]` |
| `bool` | Bool | `bool` |

For complex types (List, Map, Dictionary, nullable fields), use the full `Schema`/`Field` constructors from `@query-farm/apache-arrow`.

## Emitting Output

The `OutputCollector` provides three ways to emit data:

```typescript
// Column arrays — most efficient for multi-row batches
out.emit({ name: ["alice", "bob"], value: [1.0, 2.0] });

// Single-row convenience
out.emitRow({ name: "alice", value: 1.0 });

// Pre-built RecordBatch
out.emit(batch);
```

Int64 columns automatically coerce JavaScript Numbers to BigInt.

## Client-Directed Logging

Handler functions can emit log messages that travel over the wire to the client:

```typescript
protocol.unary("process", {
  params: { data: str },
  result: { result: str },
  handler: (params, ctx) => {
    ctx.clientLog("INFO", `Processing: ${params.data}`);
    ctx.clientLog("DEBUG", "Transform complete", { detail: "extra info" });
    return { result: params.data.toUpperCase() };
  },
});
```

In streaming methods, use `out.clientLog()`:

```typescript
produce: (state, out) => {
  out.clientLog("INFO", `Producing batch ${state.current}`);
  out.emitRow({ value: state.current });
  state.current++;
},
```

## Error Handling

Exceptions thrown in handlers are propagated to the client as `RpcError`:

```typescript
handler: async ({ a, b }) => {
  if (b === 0) throw new Error("Division by zero");
  return { result: a / b };
},
```

Errors are transmitted as zero-row Arrow batches with `EXCEPTION`-level metadata. The transport remains clean for subsequent requests.

## Authentication

The HTTP handler supports pluggable authentication. Built-in factories cover common strategies:

```typescript
import {
  createHttpHandler,
  chainAuthenticate,
  mtlsAuthenticateSubject,
  bearerAuthenticateStatic,
  jwtAuthenticate,
  AuthContext,
} from "@query-farm/vgi-rpc";

// mTLS via proxy-forwarded client certificate
const mtlsAuth = mtlsAuthenticateSubject({
  allowedSubjects: new Set(["my-service"]),
});

// Static API key map
const apiKeyAuth = bearerAuthenticateStatic({
  tokens: { "sk-abc123": new AuthContext("apikey", true, "ci-bot") },
});

// Chain: try mTLS first, fall back to API key
const auth = chainAuthenticate(mtlsAuth, apiKeyAuth);

const handler = createHttpHandler(protocol, {
  signingKey: myKey,
  authenticate: auth,
});
```

Available factories:

| Factory | Description |
|---------|-------------|
| `bearerAuthenticate` | Custom bearer token validation |
| `bearerAuthenticateStatic` | Static token map with constant-time comparison |
| `jwtAuthenticate` | JWT validation via OIDC discovery |
| `mtlsAuthenticate` | Custom X.509 certificate validation |
| `mtlsAuthenticateFingerprint` | Certificate fingerprint lookup |
| `mtlsAuthenticateSubject` | Subject CN extraction with optional allowlist |
| `mtlsAuthenticateXfcc` | Envoy `x-forwarded-client-cert` header |
| `chainAuthenticate` | Try multiple strategies in order |

## Testing with the Python CLI

The [vgi-rpc CLI](https://github.com/Query-farm/vgi-rpc-python) can introspect and call methods on any TypeScript server:

```bash
pip install vgi-rpc[cli]

# Describe the service
vgi-rpc --cmd "bun run examples/calculator.ts" describe

# Unary call
vgi-rpc --cmd "bun run examples/calculator.ts" call add a=2.0 b=3.0

# Producer stream
vgi-rpc --cmd "bun run examples/streaming.ts" call count limit=5 --format table

# Exchange stream
vgi-rpc --cmd "bun run examples/streaming.ts" call scale factor=2.0
```

## Wire Protocol Compatibility

This library implements the same wire protocol as the Python [`vgi-rpc`](https://github.com/Query-farm/vgi-rpc-python) framework:

- Multiple sequential Arrow IPC streams on stdin/stdout
- Request batches carry method name and version in batch metadata
- Lockstep streaming: one output batch per input batch
- Zero-row batches for log messages and errors
- `__describe__` introspection method for cross-language service discovery

See the [Wire Protocol Specification](https://vgi-rpc.query.farm/wire-protocol) for the full protocol details.

## Examples

| Example | Description |
|---------|-------------|
| [`calculator.ts`](examples/calculator.ts) | Unary methods: add, multiply, divide |
| [`greeter.ts`](examples/greeter.ts) | String parameters and results |
| [`streaming.ts`](examples/streaming.ts) | Producer and exchange stream patterns |
| [`conformance.ts`](examples/conformance.ts) | 87-method reference service for wire-protocol conformance testing |

## Development

```bash
# Install dependencies
bun install

# Run tests
bun test

# Build (types + bundle)
bun run build
```

## License

Copyright 2026 Query.Farm LLC [https://query.farm](https://query.farm). All rights reserved. See [LICENSE.md](LICENSE.md).
