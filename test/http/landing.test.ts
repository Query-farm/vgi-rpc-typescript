// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, test } from "bun:test";
import { createHttpHandler, type LandingDescribeProvider, Protocol } from "../../src/index.js";

function makeHandler(prefix = "") {
  const protocol = new Protocol("LandingTest");
  protocol.unary("noop", { params: {}, result: {}, handler: async () => ({}) });

  const provider: LandingDescribeProvider = {
    describe: ({ serverId, oauth }) => ({
      landing_schema_version: 1,
      worker: { name: "LandingTest", doc: "", version: "0.0.0", lang: "typescript" },
      server_id: serverId,
      oauth,
      cupola_base: "https://cupola.query-farm.services",
      catalogs: [
        {
          name: "cat",
          implementation_version: null,
          data_version_spec: null,
          data_versions: [],
          attach_options: [],
          tags: {},
          counts: { schemas: 1, tables: 1, views: 0, functions: 0 },
          schemas: [{ name: "main", tables: [{ name: "t", cols: 1, comment: "" }], views: [], functions: [] }],
        },
      ],
    }),
    columns: (catalog, schema, table) =>
      catalog === "cat" && schema === "main" && table === "t" ? { columns: [{ name: "id", type: "BIGINT" }] } : null,
  };

  return createHttpHandler(protocol, { prefix, serverId: "srv123", landingDescribe: provider });
}

describe("VGI landing surface", () => {
  test("GET / serves the vendored landing.html for browsers", async () => {
    const handler = makeHandler();
    const resp = await handler(new Request("http://localhost/", { headers: { Accept: "text/html" } }));
    expect(resp.status).toBe(200);
    expect(resp.headers.get("Content-Type")).toContain("text/html");
    const body = await resp.text();
    expect(body).toContain("vgi-landing-asset v");
    expect(body).toContain('name="vgi-landing-version"');
  });

  test("GET /?format=json returns a JSON status object", async () => {
    const handler = makeHandler();
    const resp = await handler(new Request("http://localhost/?format=json"));
    expect(resp.status).toBe(200);
    expect(resp.headers.get("Content-Type")).toContain("application/json");
    expect(await resp.json()).toEqual({ status: "ok", server_id: "srv123", protocol: "vgi" });
  });

  test("GET / with Accept: application/json returns the JSON status", async () => {
    const handler = makeHandler();
    const resp = await handler(new Request("http://localhost/", { headers: { Accept: "application/json" } }));
    expect(resp.status).toBe(200);
    expect((await resp.json()).protocol).toBe("vgi");
  });

  test("GET /describe.json returns the contract document", async () => {
    const handler = makeHandler();
    const resp = await handler(
      new Request("http://localhost/describe.json", { headers: { Accept: "application/json" } }),
    );
    expect(resp.status).toBe(200);
    const doc = await resp.json();
    expect(doc.landing_schema_version).toBe(1);
    expect(doc.worker.lang).toBe("typescript");
    expect(doc.server_id).toBe("srv123");
    expect(doc.oauth).toBe(false);
    expect(doc.catalogs[0].name).toBe("cat");
  });

  test("GET /describe/{cat}/{schema}/{table}.json returns lazy columns", async () => {
    const handler = makeHandler();
    const resp = await handler(new Request("http://localhost/describe/cat/main/t.json"));
    expect(resp.status).toBe(200);
    expect(await resp.json()).toEqual({ columns: [{ name: "id", type: "BIGINT" }] });
  });

  test("GET /describe/... for a missing object returns 404", async () => {
    const handler = makeHandler();
    const resp = await handler(new Request("http://localhost/describe/cat/main/missing.json"));
    expect(resp.status).toBe(404);
    expect((await resp.json()).error).toBe("object not found");
  });

  test("routes honour a non-empty prefix", async () => {
    const handler = makeHandler("/vgi");
    const html = await handler(new Request("http://localhost/vgi/", { headers: { Accept: "text/html" } }));
    expect(html.status).toBe(200);
    expect(await html.text()).toContain("vgi-landing-asset v");

    const describe = await handler(
      new Request("http://localhost/vgi/describe.json", { headers: { Accept: "application/json" } }),
    );
    expect(describe.status).toBe(200);

    const cols = await handler(new Request("http://localhost/vgi/describe/cat/main/t.json"));
    expect(cols.status).toBe(200);
  });
});
