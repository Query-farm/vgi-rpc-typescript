#!/usr/bin/env bash
# Copyright 2025, 2026 Query Farm LLC - https://query.farm
#
# `bun build --compile` a consumer of the PACKED tarball that imports the s3
# and gcs entries, first without their optional peer packages, then with them.
#
# Why: Bun's bundler resolves a string-literal `import()` at build time and
# fails the build if the package is missing, unless the import is awaited
# inside a `try` (or has a `.catch`). 0.30.2's s3.ts had unguarded
# `await import("@aws-sdk/client-s3")` calls, so once vgi-typescript imported
# `@query-farm/vgi-rpc/s3` every worker's `bun build --compile` failed with
# `Could not resolve: "@aws-sdk/client-s3"`, even workers that never use S3.
#
#   phase 1 (no peers): the build must succeed and the binary must report the
#     "needs the optional package(s)" error for S3 and GCS.
#   phase 2 (peers installed): the build must bundle them and the binary must
#     get past loading the SDK (any other error, e.g. connection refused).
#
# Usage: scripts/check-compile-optional-peers.sh   (run after `bun run build`)
# Needs network access (installs the tarball's dependencies and the peers).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/vgi-rpc-compile.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

cd "$ROOT"
TARBALL="$WORK/$(npm pack --ignore-scripts --silent --pack-destination "$WORK")"
S3_RANGE="$(node -p "require('./package.json').devDependencies['@aws-sdk/client-s3']")"
GCS_RANGE="$(node -p "require('./package.json').devDependencies['@google-cloud/storage']")"

mkdir -p "$WORK/consumer"
cd "$WORK/consumer"
echo '{ "private": true, "type": "module" }' > package.json
bun add "$TARBALL" >/dev/null

for pkg in @aws-sdk/client-s3 @aws-sdk/s3-request-presigner @google-cloud/storage; do
  if [ -d "node_modules/$pkg" ]; then
    echo "FAIL: $pkg was installed; optional peers must not be" >&2
    exit 1
  fi
done

cat > probe.ts <<'EOF'
import { createGCSStorage } from "@query-farm/vgi-rpc/gcs";
import { createS3Storage } from "@query-farm/vgi-rpc/s3";

async function probe(name: string, run: () => Promise<void>): Promise<void> {
  const timeout = new Promise<never>((_, reject) => setTimeout(() => reject(new Error("probe timeout")), 20_000));
  try {
    await Promise.race([run(), timeout]);
    console.log(`${name}: ok`);
  } catch (error) {
    console.log(`${name}: ${(error as Error).message}`);
  }
}

// Port 9 (discard) on loopback: nothing listens, so the request fails fast
// once the SDK is loaded.
await probe("s3", () =>
  createS3Storage({
    bucket: "probe",
    endpointUrl: "http://127.0.0.1:9",
    region: "us-east-1",
    credentials: { accessKeyId: "probe", secretAccessKey: "probe" },
  }).verify(),
);
await probe("gcs", () => createGCSStorage({ bucket: "probe", endpointUrl: "http://127.0.0.1:9", projectId: "probe" }).verify());
process.exit(0);
EOF

build_and_run() {
  rm -f probe probe.exe
  if ! bun build --compile probe.ts --outfile probe >build.log 2>&1; then
    cat build.log >&2
    echo "FAIL: bun build --compile ($1)" >&2
    exit 1
  fi
  ./probe | tee run.log
}

expect() { grep -q -- "$2" run.log || { echo "FAIL ($1): expected '$2'" >&2; exit 1; }; }
reject() { if grep -q -- "$2" run.log; then echo "FAIL ($1): unexpected '$2'" >&2; exit 1; fi; }

echo "== phase 1: no optional peers"
build_and_run "no peers"
expect "no peers" "^s3: S3 storage needs the optional packages"
expect "no peers" "^gcs: GCS storage needs the optional package"

echo "== phase 2: optional peers installed"
bun add "@aws-sdk/client-s3@$S3_RANGE" "@aws-sdk/s3-request-presigner@$S3_RANGE" "@google-cloud/storage@$GCS_RANGE" >/dev/null
build_and_run "peers"
reject "peers" "needs the optional package"
reject "peers" "Cannot find module\|Cannot find package"
# The binary must not need node_modules at runtime: the SDK was bundled.
mv node_modules node_modules.off
./probe | tee run.log
mv node_modules.off node_modules
reject "peers, node_modules hidden" "needs the optional package"

echo "OK: bun build --compile works with and without the optional peers"
