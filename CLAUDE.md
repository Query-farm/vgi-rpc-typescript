# vgi-rpc

TypeScript server library for the vgi-rpc framework. Communicates over stdin/stdout using Apache Arrow IPC serialization. Implements the same wire protocol as the Python reference implementation.

## Related Projects

- **Python reference implementation**: `git@github.com:Query-farm/vgi-rpc-python.git` (local checkout at `/Users/rusty/Development/vgi-rpc`)
  - The Python implementation is the canonical reference for wire protocol behavior
  - The Python CLI (`vgi-rpc`) is used for integration/conformance testing
  - When in doubt about wire protocol details, check the Python implementation

## Project Structure

```
src/
  index.ts          — Public API exports
  protocol.ts       — Fluent builder for defining RPC methods
  server.ts         — VgiRpcServer: main request loop over stdin/stdout
  types.ts          — Handler types, OutputCollector, LogContext
  schema.ts         — Schema shorthand (str, int, float, etc.) and toSchema/inferParamTypes
  errors.ts         — RpcError, VersionError
  constants.ts      — Wire protocol metadata keys
  wire/             — Low-level IPC reader/writer and request/response serialization
  dispatch/         — Method dispatch (unary, stream, describe)
  util/             — Internal utilities
examples/
  calculator.ts     — Unary methods example
  greeter.ts        — String params example
  streaming.ts      — Producer and exchange streams
  conformance.ts    — 43-method conformance suite for wire-protocol testing
test/
  wire.test.ts      — Unit tests for wire serialization
  describe.test.ts  — Unit tests for __describe__ method
  schema.test.ts    — Unit tests for toSchema and inferParamTypes
  output-collector.test.ts — Unit tests for OutputCollector and result validation
  integration.test.ts      — Integration tests (requires Python CLI)
  conformance.test.ts      — Conformance tests (requires Python CLI)
```

## Testing

- Run tests: `bun test`
- Run unit tests only (no Python CLI needed): `bun test test/wire.test.ts test/describe.test.ts test/schema.test.ts test/output-collector.test.ts`
- All individual tests must complete in 5 seconds or less
- Integration and conformance tests require the Python CLI at `/Users/rusty/Development/vgi-rpc/.venv/bin/vgi-rpc`
- Always use timeouts on subprocess spawns to prevent hangs
- Build: `bun run build` (runs TypeScript type-checking then bundles)

## Dependencies

- Runtime: Bun
- Arrow: Query-farm fork of arrow-js (`github:Query-farm/arrow-js#feat_query_farm_1`) — ships TypeScript source only
- The `postinstall` script patches `node_modules/apache-arrow/package.json` to add `"main": "index.ts"` for Bun resolution
- If `bun install` is run, re-run `bun run postinstall` if arrow imports break

## Wire Protocol

This library must remain wire-compatible with the Python vgi-rpc implementation. Key protocol details:

- Multiple sequential Arrow IPC streams on stdin/stdout
- Request batches carry `vgi_rpc.method` and `vgi_rpc.request_version` in batch metadata
- Streaming uses lockstep: one output batch per input batch (interleaved reads/writes to avoid deadlock)
- Log/error messages are zero-row batches with `vgi_rpc.log_level` and `vgi_rpc.log_message` metadata
- `__describe__` introspection returns service metadata as an Arrow batch

## CI

GitHub Actions workflow at `.github/workflows/ci.yml`:
- **test** job: runs unit tests (excludes integration/conformance that need Python CLI)
- **build** job: runs full build and verifies dist outputs
- Dependabot configured for npm and github-actions updates
