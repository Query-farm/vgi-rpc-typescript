# vgi-rpc

TypeScript server library for the vgi-rpc framework. Communicates over stdin/stdout using Apache Arrow IPC serialization. Implements the same wire protocol as the Python reference implementation.

## Related Projects

- **Python reference implementation**: `git@github.com:Query-farm/vgi-rpc-python.git`
  - The Python implementation is the canonical reference for wire protocol behavior
  - The Python CLI (`vgi-rpc`) and conformance suite are installed from PyPI: `pip install "vgi-rpc[http]"`
  - When in doubt about wire protocol details, check the Python implementation

  **Two Python checkouts exist locally and they are not the same implementation.**
  `vgi-rpc-python` (branch `multiservice/pr1-internal`) is canonical. The
  similarly named `vgi-rpc` checkout is `main` — flat routes, no routing key,
  `__describe__` still live, none of the multiservice work. Its version number
  is *higher*, which is exactly why this went unnoticed: this repo's harness
  was pinned to it, and the resulting all-red conformance run (1456 failed / 95
  passed) read as port breakage rather than as a harness aimed at a server that
  404s every namespaced path. Against the canonical reference the same suite is
  1512 passed / 42 failed.

  Nothing committed hardcodes either path. `test/reference.ts` resolves the
  interpreter and the `vgi-rpc` CLI in three steps — environment override, then
  the reference checkout's venv located relative to `$HOME`, then `PATH` — and
  the `Makefile` mirrors it for `$(PYTHON)`. Override with:

  | variable | what it points at |
  |---|---|
  | `VGI_RPC_PYTHON_HOME` | root of the reference checkout (default `~/Development/vgi-rpc-python`) |
  | `VGI_RPC_PYTHON_BIN` | a specific interpreter, bypassing the checkout lookup |
  | `VGI_RPC_CLI` | a specific `vgi-rpc` binary |

  CI sets none of them: it pip-installs the reference, so the `PATH` fallback
  is the correct answer there.

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
  conformance.ts    — 87-method conformance suite for wire-protocol testing
test/
  wire.test.ts      — Unit tests for wire serialization
  dispatch-identity.test.ts — Access records name the owning binding, and its digest
  schema.test.ts    — Unit tests for toSchema and inferParamTypes
  output-collector.test.ts — Unit tests for OutputCollector and result validation
  integration.test.ts      — Integration tests (requires Python CLI)
test_ts_conformance.py     — Python conformance suite runner (imports from vgi-rpc package)
```

## Makefile

The project uses a Makefile for common tasks. Run `make help` to see all targets.

- `make` / `make build` — Install deps and build (JS bundle + type declarations)
- `make test-unit` — Run unit tests only (no external dependencies)
- `make test-integration` — Run integration tests (requires Python CLI)
- `make test-conformance` — Run conformance tests (requires Python CLI)
- `make test` — Run all tests
- `make lint` — Run Biome linter/formatter checks
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
- Integration and conformance tests require `vgi-rpc[http]` installed: `pip install "vgi-rpc[http]"`
  - Conformance tests use `test_ts_conformance.py` which imports `vgi_rpc.conformance._pytest_suite` and runs against `bun run examples/conformance.ts`
  - Integration tests use the `vgi-rpc` CLI (must be on PATH)
  - Client tests spawn Python servers via the interpreter `test/reference.ts` resolves; see **Related Projects** for the override variables
  - A bare `bun test` against the canonical reference is **1255 pass / 9 skip / 0 fail**. Every failure in a run that reports ~26 fails with `Executable not found in $PATH: "vgi-rpc"` is a harness-pointing problem, not a port defect
- Always use timeouts on subprocess spawns to prevent hangs
- Build: `make build` or `bun run build` (runs TypeScript type-checking then bundles)

## Dependencies

- Runtime: Bun
- Arrow: `@query-farm/apache-arrow` (published on npm) — ships TypeScript source only
- The `postinstall` script patches `node_modules/@query-farm/apache-arrow/package.json` to add `"main": "src/Arrow.node.ts"` for Bun resolution
- If `bun install` is run, re-run `bun run postinstall` if arrow imports break

## Wire Protocol

This library must remain wire-compatible with the Python vgi-rpc implementation. Key protocol details:

- Multiple sequential Arrow IPC streams on stdin/stdout
- Request batches carry `vgi_rpc.method` and `vgi_rpc.request_version` in batch metadata
- Streaming uses lockstep: one output batch per input batch (interleaved reads/writes to avoid deadlock)
- Log/error messages are zero-row batches with `vgi_rpc.log_level` and `vgi_rpc.log_message` metadata
- Introspection is `vgi_rpc.Reflection.v1`, an ordinary co-hosted protocol (`list_protocols`, then `describe`); the reserved `__describe__` method is retired and refused with a message naming it

## Cross-language wire alignment

This port tracks `vgi-rpc-python` for wire compatibility. Two surfaces matter:

- **Introspection** — `vgi_rpc.Reflection.v1` (`src/reflection.ts`), a co-hosted protocol with two methods: `list_protocols` (what this server hosts, with versions and hashes) and `describe(protocol)` (one protocol's methods). Each reply rides as serialized Arrow IPC in a single `result` binary column, the framework's ordinary convention for a structured return — so a server that externalizes returns a pointer batch for reflection like any other method, and a client must resolve it. Decoding is tolerant by contract: by field name, ignoring unknown columns, defaulting absent ones that have defaults, raising for an absent one that does not. **A field added in a minor version must carry a default.** The `protocol_hash` it reports is the canonical digest (`src/protocol-hash.ts`: SHA-256 over RFC 8785 canonical JSON of the decoded structure), which is comparable across ports — unlike the retired describe payload's digest, which hashed Arrow IPC bytes and was comparable only against itself. `__describe__` is retired on every transport and refused with a message naming the replacement protocol and both entry points; only that one reserved name is special-cased.

- **Access log** — `AccessLogHook` in `src/access-log.ts` writes one JSONL record per dispatch when installed via `new VgiRpcServer(protocol, { dispatchHook })` or `createHttpHandler(protocol, { dispatchHook })`. The record shape conforms to `vgi_rpc/access_log.schema.json` in the Python repo and validates under `vgi-rpc-test --access-log <path>`. `DispatchInfo` (`src/types.ts`) carries `protocol`, `protocolHash`, `protocolVersion`, `remoteAddr`, `httpStatus`, `requestData`, `streamId`, `cancelled`, `claims`, `requestBytes`, `externalizedBytes`, and `deferral`. Configure `protocolVersion` via the `VgiRpcServer` constructor option.

  **`protocol` and `protocolHash` are the owning binding's, at every emit site.** A server hosts several protocols; the record must name the one that owns the dispatched method, and carry *its* canonical digest. The two disagreeing is worse than either being wrong alone — `protocol_hash` is the registry key for decoding archived records, so a record naming one protocol and carrying another's decodes against the wrong description while passing the schema. Both are read inline from `binding` via `protocolHashFor(binding)` at all four sites (stdio server, HTTP handler, unix and tcp launchers), and `test/dispatch-identity.test.ts` enumerates those sites structurally — a fifth one added later fails that test rather than reintroducing this silently. Framework endpoints owned by no protocol (`__transport_options__`, `__upload_url__`) log the primary, which is the specified behaviour rather than a gap.

  **HTTP stream records.** Over HTTP a stream is many requests, so one record is emitted per `/init` and per `/exchange`, and the fields that tie them together come from the dispatcher rather than the handler: `DispatchContext.streamObserver` (`src/http/dispatch.ts`) reports the stream's chain id and any client cancel. `stream_id` is the hex of the stream's `callId` — minted once at `/init`, carried sealed in the call token and every cursor — so `/init` and its continuations agree and two streams never collide without a second identifier or a token-format change. The all-zeros id is reserved for a stream record whose request failed before a stream existed. `request_data` rides on unary calls and stream `/init` (spec §4.3) and on no continuation; `http_status` is on every HTTP record.

  Two things pin this end-to-end, and both are necessary. `conformance/check_access_log_streams.py` (run in CI with `--http --require-continuations`) reads a log a real worker produced and rejects one that is silent about streams, one whose ids are constant, and one that stamps `request_data` on a continuation. `test/http/stream-access-log.test.ts` drives a multi-turn `produce_n` through `createHttpHandler` in-process and asserts the same contract without a server. Neither is redundant: `test/access-log.test.ts` feeds `AccessLogHook` hand-built `DispatchInfo` values, so it can only show what the hook does with a `streamId` it is *handed* — it would pass unchanged on a handler that emitted nothing at all for streams, which is the exact state two sibling ports were found in. A record validator validates the records that exist; zero records invalidates nothing and reads as clean.

  **`remote_addr` is empty over HTTP**, unlike the reference, which reports the peer address. `createHttpHandler` is a `(Request) => Response` function: the fetch API exposes no peer address, and the runtimes that can supply one (Bun's `server.requestIP`) only do so through a server object the handler never sees. The schema permits the empty string.

  **The §4.6 call-statistics group is omitted on every transport.** The six fields are all-or-nothing and measure logical Arrow buffer sizes, which neither Arrow backend exposes portably (arrow-js has `batch.data.byteLength`; flechette has no equivalent). Emitting a well-formed group of zeros would be worse than the spec-permitted omission. The egress figures — `request_bytes`, `response_bytes`, `externalized_bytes` — are unaffected and are present on stream records as well as unary ones.

  Hook options (`AccessLogOptions`): `level` (`"INFO"` omits `request_data` and marks the record `truncated: "payload_omitted"` — distinct from the `true` that means genuine size-driven shedding), `maxRecordBytes` (per-record cap, sheds `request_data` → `claims` → sentinel), `sampleRate` (deterministic per call, keyed on `stream_id` then `request_id`; errors are never sampled out; an out-of-range rate throws at construction), `async`/`queueSize` (bounded non-blocking queue; a full queue drops and the next record carries `dropped_records`; call `hook.flush()` on shutdown), `traceContext` (defaults to the active OpenTelemetry span when `@opentelemetry/api` is installed — resolved once via indirect `require`, so it stays an optional peer dep), and `redactor` (key-based claim redaction, `noRedaction` to opt out; a redactor that throws fails closed).

  `response_bytes` cannot be measured at dispatch time — compression runs afterwards — so `createHttpHandler` installs an `AccessLogDeferral` on `DispatchInfo`, and the hook hands its record to it; the handler emits once the final body exists. `request_bytes` is captured before request decompression and `externalized_bytes` at the `maybeExternalizeBatch` choke point. All three are wire/egress figures, unrelated to §4.6's logical `input_bytes`/`output_bytes`.

- **`X-Request-ID`** — `createHttpHandler` echoes an inbound header unchanged and mints a 16-hex-character id otherwise, stamps it on every response, and puts the same value in the access record's `request_id`. Agreement between the two is the point of the field: an id on the response that names nothing in the log looks like a working trail right until someone follows it. The header sat in the CORS expose list long before it was ever sent — the same shape of bug as advertising a capability header that never ships.

The conformance worker (`examples/conformance.ts`) accepts `--access-log <path>` anywhere on the CLI, plus `--access-log-sample R`, `--access-log-async`, and `--access-log-debug`. `examples/conformance-http.ts` takes the same four. `--access-log-debug` raises the hook to DEBUG so records carry `request_data`; at the default INFO the payload is a `payload_omitted` marker and `vgi-rpc-test --require-request-data` fails.

`conformance/check_access_log_streams.py` covers what neither the schema nor `--require-request-data` can reach: that a log contains stream records at all, that `stream_id` distinguishes streams and chains a stream's turns, and that `request_data` rides on `/init` and nothing else. `--require-request-data` inspects only unary records, and any 32 hex characters satisfy the schema's `stream_id` — so both reported PASS over 113 stream records that named no stream.

## Conformance in client role

Every conformance leg above points the Python reference *client* at this port's
server. `conformance/client-driver.ts` is the other direction: a small
executable speaking a newline-delimited JSON control protocol on stdin/stdout,
which lets the shared Python suite drive **this port's client**
(`httpConnect` / `pipeConnect` / `tcpConnect`) instead.

The contract is `tools/cross-port/specs/CLIENT_DRIVER_PROTOCOL.md` in the
reference repository — seventeen ops, the Arrow IPC framing, the two error
channels, log relay, and what must *not* live in a driver. The Python half of
the bridge (`vgi_rpc.conformance.client_driver`) is shared across ports and is
not ours; `ts_client_proxy.py` is the four lines of glue that name our driver.

**Why it is a separate gate.** This port's server accepts both bare and
namespaced request paths, so a client addressing the wrong one passes against
it and fails against a strict peer. That is not hypothetical — the Rust port
shipped exactly that defect for weeks, green against its own server and **730
failures** the moment it met the Python reference. A permissive server cannot
validate a client, so the run that counts is the one against the reference:

```bash
# The gate: this port's client against the Python reference server.
VGI_CONFORMANCE_ROLE=client VGI_CONFORMANCE_SERVER=python \
  VGI_CLIENT_DRIVER="bun run conformance/client-driver.ts" \
  python -m pytest test_ts_conformance.py -q

# A regression check, not a conformance claim. Useful because when the two
# legs disagree, the gap localises the defect to one side in one run.
VGI_CONFORMANCE_ROLE=client VGI_CONFORMANCE_SERVER=typescript \
  VGI_CLIENT_DRIVER="bun run conformance/client-driver.ts" \
  python -m pytest test_ts_conformance.py -q
```

`VGI_CONFORMANCE_SERVER=python` needs a *checkout* of `vgi-rpc-python`, not
just the installed package: the `serve_conformance_*.py` fixtures live in the
repository. `VGI_RPC_PYTHON_REPO` points at it (default
`~/Development/vgi-rpc-python`).

**The client's batch-level surface.** A driver must relay Arrow IPC bytes, not
values — decoding in the driver would let it paper over a client defect. So
`RpcClient` carries `callRaw` / `streamRaw` beside `call` / `stream`, and the
row-oriented methods are implemented on top of them: whatever the driver
exercises is the same code path an ordinary caller takes. `RawStreamSession`
adds `tickRaw`, `exchangeRaw`, `nextWithTokenRaw` and `cancel`.

## CI

GitHub Actions workflow at `.github/workflows/ci.yml`:
- **lint** job: runs Biome linter/formatter checks
- **test** job: runs unit tests and client tests (bun transports only)
- **build** job: runs full build and verifies dist outputs
- **conformance** job: installs `vgi-rpc[http,cli,external,conformance]>=0.42.0` from PyPI (the `conformance` extra carries jsonschema, required by the access-log validator), runs conformance + client tests with all transports, and validates the emitted access log via `vgi-rpc-test --access-log ... --require-request-data` (unfiltered, so zero-parameter methods like `void_noop` stay in scope) — twice: once against the pipe worker and once against the HTTP worker over `--url`, each followed by `conformance/check_access_log_streams.py`. The HTTP pass is the one that matters for streams, since HTTP is the only transport where a stream's turns are separate requests that have to be chained, and it went entirely unvalidated until 0.37.
- Dependabot configured for npm and github-actions updates

## Large payloads

Every transport write is split before it reaches the syscall, and every Node-stream read is clamped before it reaches `Readable.read`. `src/wire/writer.ts` caps a single `fs.writeSync` at 1 GiB and a single `socket.write` at 128 KiB; `src/client/pipe.ts` applies the same 128 KiB split to the client's writable (Bun subprocess `FileSink`, tcp socket); `src/wire/reader.ts` wraps Node readables so `read(n)` never exceeds 64 MiB. Four separate ceilings make all of it necessary, and none of them are visible on Linux:

- `fs.writeSync` throws `ERR_OUT_OF_RANGE` for a `length` past `INT32_MAX` (Node and Bun both).
- `Readable.read(n)` throws the same for `n` past 1 GiB (`MAX_HWM`). arrow-js's Node adapter asks for a whole IPC message body in one call, so this fires on any Arrow body over 1 GiB — the one that was actually reachable before the clamps went in.
- macOS fails `send(2)` with `EINVAL` above 2 GiB, on `net.Socket` and on Bun's subprocess-stdin sink alike.
- Bun collapses on large single writes to an AF_UNIX peer: 100 MB in one `socket.write` moved 1.1 MB/s, the same bytes in 128 KiB pieces moved 1.7 GB/s. Node is unaffected, and TCP does not show it.

`test/large-payload.test.ts` pins the invariant (no call is handed more than its clamp) on payloads small enough for any runner. The real check is the reference suite's `large_payload` group **run on a Mac** — Linux caps a transfer at `0x7ffff000` and returns a short count that a correct loop absorbs, so a Linux CI cannot tell a bounded writer from an unbounded one.

**Ceiling: 2 GiB − 8 bytes**, and it is not the transport. Both Arrow backends round a buffer up to an 8-byte boundary with `(byteLength + 7) & ~7` — a bitwise operator, so the result truncates to int32 and goes negative at 2^31. arrow-js does it in `visitor/vectorassembler.mjs`'s `addBuffer`, which feeds `Message.bodyLength` directly, so a 2 GiB+1 body would ship a negative `bodyLength` and the peer would reject the message ("Invalid IPC message: negative bodyLength"). flechette does it in `util/arrays.js`'s `align64` and its encoders, and gives out earlier still — somewhere between 768 MiB and 1 GiB. Neither ceiling can be raised from this repository.

`src/arrow/limits.ts` turns that into an honest refusal instead. `requireEncodable` runs on every value entering a batch — including the raw `Data` an echo-style handler hands straight back, which is the path a large payload actually takes — and throws a `RangeError` naming the field, the real size, and whose limit it is. It measures a `Data` by its **largest single buffer**, not the sum, because alignment is applied per buffer and totalling would refuse a payload of exactly the ceiling on account of its own validity bytes.

That is what the conformance suite asks for. `large_payload.echo_binary_over_int32_max` accepts either an intact round-trip or a typed error that leaves the connection usable — it proves the latter by issuing an ordinary call afterwards — and records the outcome as `note: payload refused, transport survived`. What it forbids is the silent pair: a truncated body, or a peer left waiting on bytes that never arrive. A negative `bodyLength` was the second of those, so this port **passes** the group, by the refusal path, and will switch to the round-trip path for free if the dependencies ever lift their ceilings.
