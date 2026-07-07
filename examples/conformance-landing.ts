// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Landing-surface conformance worker. Boots an HTTP server that serves the
// standardized VGI landing surface (GET /, /describe.json, and the lazy
// per-object column endpoint) backed by a small static describe document, so
// `vgi/test/landing/run_landing_conformance.py --url http://localhost:PORT`
// can validate the routes, the vgi-landing-asset marker, and the schema
// without needing the catalog-aware @query-farm/vgi package.
//
// Prints `PORT:<n>` to stdout for test discovery.

import { createHttpHandler, type LandingDescribeProvider, Protocol } from "../src/index.js";

const protocol = new Protocol("LandingExample");

protocol.unary("noop", {
  params: {},
  result: {},
  handler: async () => ({}),
  doc: "A no-op unary method (this worker exists to exercise the landing surface).",
});

// A minimal but schema-complete describe document with one catalog, one schema,
// one table, one view, and one function of each kind — enough to exercise the
// column endpoints (one table + one view per schema) the conformance runner
// samples.
const landingDescribe: LandingDescribeProvider = {
  describe({ serverId, oauth }) {
    return {
      landing_schema_version: 1,
      worker: {
        name: "LandingExample",
        doc: "TypeScript landing-surface conformance worker.",
        version: "0.0.0",
        lang: "typescript",
      },
      server_id: serverId,
      oauth,
      cupola_base: "https://cupola.query-farm.services",
      catalogs: [
        {
          name: "example",
          implementation_version: "1",
          data_version_spec: null,
          data_versions: [{ spec: "2024-06-01", label: "Initial release" }],
          attach_options: [{ name: "read_only", type: "BOOLEAN", default: "true", description: "Open read-only." }],
          tags: {
            title: "Example Catalog",
            author: "Query.Farm",
            keywords: ["example", "landing"],
          },
          counts: { schemas: 1, tables: 1, views: 1, functions: 4 },
          schemas: [
            {
              name: "main",
              tables: [{ name: "widgets", cols: 2, comment: "A table of widgets." }],
              views: [{ name: "recent_widgets", cols: 2, comment: "Recent widgets.", def: "SELECT * FROM widgets" }],
              functions: [
                {
                  name: "upper",
                  type: "scalar",
                  doc: "Uppercase a string.",
                  args: [{ name: "s", type: "VARCHAR", desc: "Input string." }],
                  returns: "VARCHAR",
                },
                {
                  name: "gen_series",
                  type: "table",
                  doc: "Generate a series of integers.",
                  args: [
                    { name: "start", type: "BIGINT" },
                    { name: "stop", type: "BIGINT" },
                    { name: "step", type: "BIGINT", named: true, default: "1" },
                  ],
                  returns: "TABLE(n BIGINT)",
                },
                {
                  name: "sum_all",
                  type: "aggregate",
                  doc: "Sum all inputs.",
                  args: [{ name: "x", type: "DOUBLE" }],
                  returns: "DOUBLE",
                },
                {
                  name: "enrich",
                  type: "table_in_out",
                  doc: "Enrich input rows.",
                  args: [{ name: "factor", type: "DOUBLE", named: true, default: "2.0" }],
                },
              ],
            },
          ],
        },
      ],
    };
  },
  columns(catalog, schema, table) {
    if (catalog !== "example" || schema !== "main") return null;
    if (table === "widgets") {
      return {
        columns: [
          { name: "id", type: "BIGINT", comment: "Primary key." },
          { name: "name", type: "VARCHAR" },
        ],
      };
    }
    if (table === "recent_widgets") {
      return {
        columns: [
          { name: "id", type: "", comment: "Primary key." },
          { name: "name", type: "" },
        ],
      };
    }
    return null;
  },
};

const handler = createHttpHandler(protocol, {
  serverId: "vgi-landing-example",
  landingDescribe,
});

const server = Bun.serve({ port: 0, fetch: handler });
console.log(`PORT:${server.port}`);
