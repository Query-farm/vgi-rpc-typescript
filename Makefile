.PHONY: all build build-types build-js test test-unit test-integration test-conformance typecheck clean distclean docs docs-dev help

# Python reference implementation (required for integration/conformance tests)
VGI_CLI := /Users/rusty/Development/vgi-rpc/.venv/bin/vgi-rpc
PYTHON_VENV := /Users/rusty/Development/vgi-rpc/.venv/bin/python3

# Unit test files (no external dependencies)
UNIT_TESTS := test/wire.test.ts test/describe.test.ts test/schema.test.ts test/output-collector.test.ts

SRC := $(wildcard src/**/*.ts src/*.ts)

all: build ## Install deps and build (default)

node_modules: package.json
	bun install
	bun run postinstall
	@touch $@

build: node_modules dist/index.js dist/index.d.ts ## Build JS bundle and type declarations

build-types: node_modules dist/index.d.ts ## Emit type declarations only

build-js: node_modules dist/index.js ## Build JS bundle only

dist/index.d.ts: $(SRC) tsconfig.build.json tsconfig.json | node_modules
	bunx tsc -p tsconfig.build.json

dist/index.js: $(SRC) tsconfig.json | node_modules
	bun build ./src/index.ts --outdir dist --target node --format esm --sourcemap=external --external apache-arrow

test: node_modules ## Run all tests (integration/conformance need Python CLI)
	bun test

test-unit: node_modules ## Run unit tests only (no Python CLI needed)
	bun test $(UNIT_TESTS)

test-integration: node_modules ## Run integration tests (requires Python CLI)
	@test -x $(VGI_CLI) || { echo "error: Python CLI not found at $(VGI_CLI)"; exit 1; }
	bun test test/integration.test.ts

test-conformance: node_modules ## Run Python conformance suite against bun worker
	@test -x $(PYTHON_VENV) || { echo "error: Python venv not found at $(PYTHON_VENV)"; exit 1; }
	$(PYTHON_VENV) -m pytest test_ts_conformance.py -x -v

typecheck: node_modules ## Type-check without emitting
	bunx tsc --noEmit

docs: ## Build documentation site
	cd docs && npm install && npm run build

docs-dev: ## Start docs dev server
	cd docs && npm install && npm run dev

clean: ## Remove build artifacts
	rm -rf dist

distclean: clean ## Remove build artifacts and node_modules
	rm -rf node_modules

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
