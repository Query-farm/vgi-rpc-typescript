# vgi-rpc-typescript

TypeScript server library for the vgi-rpc framework. Communicates over stdin/stdout using Apache Arrow IPC serialization.

## Testing

- Run tests: `bun test`
- All individual tests must complete in 5 seconds or less
- Integration tests use the Python CLI at `/Users/rusty/Development/vgi-rpc/.venv/bin/vgi-rpc`
- Always use timeouts on subprocess spawns to prevent hangs
