.PHONY: all build build-types build-js test test-unit test-integration test-conformance test-smoke typecheck lint clean distclean docs docs-dev help

# Python with vgi-rpc installed (override: make PYTHON=python3.13)
PYTHON ?= python3

# Unit test files (no external dependencies)
UNIT_TESTS := test/wire.test.ts test/describe.test.ts test/schema.test.ts test/output-collector.test.ts test/http/handler.test.ts test/http/token.test.ts test/auth.test.ts test/http-auth.test.ts test/bearer.test.ts

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
	bun build ./src/index.ts --outdir dist --target node --format esm --sourcemap=external --external @query-farm/apache-arrow

test: node_modules ## Run all tests (integration/conformance need vgi-rpc Python package)
	bun test
	$(PYTHON) -m pytest test_ts_conformance.py -x -v

test-unit: node_modules ## Run unit tests only (no Python CLI needed)
	bun test $(UNIT_TESTS)

test-integration: node_modules ## Run integration tests (requires vgi-rpc CLI on PATH)
	bun test test/integration.test.ts

test-smoke: node_modules ## Run cross-runtime smoke tests (Bun + Node.js)
	bun run ./test/smoke-import.ts
	mkdir -p .smoke-bundle
	bun build ./test/smoke-import.ts --outfile .smoke-bundle/smoke.js --target node --format esm
	node .smoke-bundle/smoke.js

test-conformance: node_modules ## Run Python conformance suite against bun worker
	$(PYTHON) -m pytest test_ts_conformance.py -x -v

lint: node_modules ## Run linter and formatter checks
	bunx biome check .

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
