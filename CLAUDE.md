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
  client/           — RPC client: transports (HTTP, pipe, subprocess), streaming, introspection
examples/
  calculator.ts     — Unary methods example
  greeter.ts        — String params example
  streaming.ts      — Producer and exchange streams
  conformance.ts    — 46-method conformance suite for wire-protocol testing
test/
  wire.test.ts      — Unit tests for wire serialization
  describe.test.ts  — Unit tests for __describe__ method
  schema.test.ts    — Unit tests for toSchema and inferParamTypes
  output-collector.test.ts — Unit tests for OutputCollector and result validation
  integration.test.ts      — Integration tests (requires Python CLI)
test_ts_conformance.py     — Python conformance suite runner (imports tests from vgi-rpc Python)
```

## Makefile

The project uses a Makefile for common tasks. Run `make help` to see all targets.

- `make` / `make build` — Install deps and build (JS bundle + type declarations)
- `make test-unit` — Run unit tests only (no external dependencies)
- `make test-integration` — Run integration tests (requires Python CLI)
- `make test-conformance` — Run conformance tests (requires Python CLI)
- `make test` — Run all tests
- `make typecheck` — Type-check without emitting
- `make docs` / `make docs-dev` — Build or serve the documentation site
- `make clean` — Remove `dist/`
- `make distclean` — Remove `dist/` and `node_modules/`

## Testing

- Run tests: `make test` or `bun test`
- Run unit tests only (no Python CLI needed): `make test-unit`
- Run conformance tests: `make test-conformance` (runs Python conformance suite against bun worker)
- All individual tests must complete in 5 seconds or less
- **Always use a 60-second timeout when running tests** (e.g., `timeout 60 make test-conformance`)
- Integration and conformance tests require the Python venv at `/Users/rusty/Development/vgi-rpc/.venv/bin/python3`
  - Conformance tests use `test_ts_conformance.py` which imports Python conformance suite and runs against `bun run examples/conformance.ts`
  - Integration tests spawn the Python CLI, which in turn runs `bun run examples/<server>.ts` as a subprocess
- Always use timeouts on subprocess spawns to prevent hangs
- Build: `make build` or `bun run build` (runs TypeScript type-checking then bundles)

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
