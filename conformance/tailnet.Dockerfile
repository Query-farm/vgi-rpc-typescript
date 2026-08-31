# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

FROM oven/bun:1.3.14 AS build
WORKDIR /src
COPY package.json bun.lock bunfig.toml ./
COPY scripts ./scripts
RUN bun install --frozen-lockfile
COPY tsconfig.json tsconfig.build.json ./
COPY src ./src
COPY conformance/tailnet.ts ./conformance/tailnet.ts
RUN bun build --compile --minify --outfile /out/vgi-rpc-tailnet-typescript ./conformance/tailnet.ts

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/vgi-rpc-tailnet-typescript /usr/local/bin/vgi-rpc-tailnet-typescript
ENTRYPOINT ["vgi-rpc-tailnet-typescript"]
