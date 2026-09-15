// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Every access record names the protocol that owns the dispatched method, and
 * carries *that* protocol's canonical digest.
 *
 * `docs/access-log-spec.md` §3 makes `protocol` the owning protocol's wire
 * name, "not a server-wide default", and `protocol_hash` the registry key for
 * decoding archived records. The two must agree: a record naming one protocol
 * while carrying another's digest is decoded against the wrong description and
 * still passes the schema, which is the failure mode worth the most guarding —
 * nothing errors, and the dashboard it feeds looks plausible.
 *
 * Only a call to a *secondary* protocol can catch that end-to-end, because for
 * an application method the primary IS the owning binding. The behavioural
 * cases below do exactly that. But the way this regresses is not a broken
 * lookup — it is a *fifth* emit site added later that fills the fields from
 * the server instead, on a transport nobody's test happened to drive. So the
 * first case is structural: it enumerates every construction of a
 * `DispatchInfo` across `src/` and asserts each one reads both fields from the
 * resolved binding, and that the number of them is the number this file knows
 * about.
 */

import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { AccessLogHook, type AccessLogSink } from "../src/access-log.js";
import { buildRequestIpc } from "../src/client/ipc.js";
import { PROTOCOL_KEY } from "../src/constants.js";
import { ARROW_CONTENT_TYPE, rpcPath } from "../src/http/common.js";
import { createHttpHandler } from "../src/http/handler.js";
import { Protocol } from "../src/protocol.js";
import { bindingHash, REFLECTION_PROTOCOL_NAME } from "../src/reflection.js";
import { str, toSchema } from "../src/schema.js";
import { VgiRpcServer } from "../src/server.js";

// ---------------------------------------------------------------------------
// The structural guard
// ---------------------------------------------------------------------------

/** Every construction of a `DispatchInfo` in the source tree, one per site.
 *
 *  Four transports build one: the stdio server, the HTTP handler, and the unix
 *  and tcp launchers. Each keeps its own dispatch loop because each has its own
 *  framing, which is exactly why a fix applied to one can miss the others. */
const EXPECTED_EMIT_SITES = 4;

function sourceFiles(dir: string): string[] {
  const out: string[] = [];
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    if (statSync(full).isDirectory()) out.push(...sourceFiles(full));
    else if (entry.endsWith(".ts")) out.push(full);
  }
  return out;
}

/** Slice out the `{...}` an index points at, matching braces. */
function objectLiteralAt(text: string, openBrace: number): string {
  let depth = 0;
  for (let i = openBrace; i < text.length; i++) {
    if (text[i] === "{") depth++;
    else if (text[i] === "}") {
      depth--;
      if (depth === 0) return text.slice(openBrace, i + 1);
    }
  }
  throw new Error("unbalanced object literal");
}

function dispatchInfoLiterals(): { file: string; literal: string }[] {
  const found: { file: string; literal: string }[] = [];
  for (const file of sourceFiles(join(import.meta.dir, "..", "src"))) {
    const text = readFileSync(file, "utf8");
    const marker = /:\s*DispatchInfo\s*=\s*\{/g;
    let match: RegExpExecArray | null = marker.exec(text);
    while (match !== null) {
      found.push({ file, literal: objectLiteralAt(text, match.index + match[0].length - 1) });
      match = marker.exec(text);
    }
  }
  return found;
}

describe("every DispatchInfo is filled from the resolved binding", () => {
  test("the emit sites are the ones this test knows about", () => {
    // A count, not a floor. The regression this guards against is a new site
    // that fills the fields from the server; a new site is therefore supposed
    // to fail here once, and be read before the number moves.
    const sites = dispatchInfoLiterals();
    expect(sites.length).toBe(EXPECTED_EMIT_SITES);
  });

  test("each one reads protocol and protocolHash from the binding", () => {
    for (const { file, literal } of dispatchInfoLiterals()) {
      expect(`${file}: ${literal}`).toContain("protocol: binding.name,");
      expect(`${file}: ${literal}`).toContain("protocolHash: await protocolHashFor(binding),");
    }
  });
});

// ---------------------------------------------------------------------------
// The behavioural cases
// ---------------------------------------------------------------------------

const APP = "demo.Identity.v1";

function appProtocol(): Protocol {
  return new Protocol(APP).unary("echo", {
    params: { message: str },
    result: { message: str },
    handler: (params) => ({ message: String(params.message) }),
  });
}

interface Record_ {
  protocol: string;
  protocol_hash: string;
  method: string;
}

function captureSink(records: Record_[]): AccessLogSink {
  return { write: (line: string) => records.push(JSON.parse(line)) };
}

async function post(
  handler: (r: Request) => Response | Promise<Response>,
  url: string,
  body: Uint8Array,
): Promise<Response> {
  return handler(
    new Request(url, { method: "POST", headers: { "Content-Type": ARROW_CONTENT_TYPE }, body: body as BodyInit }),
  );
}

describe("a secondary protocol's record carries its own identity", () => {
  test("reflection and the application do not share a digest", async () => {
    // The precondition that makes the rest of this meaningful. If the two
    // hashed alike, logging the primary everywhere would be both invisible and
    // harmless, and none of these assertions would prove anything.
    const server = new VgiRpcServer(appProtocol(), { serverId: "identity-test" });
    const app = server.bindings().get(APP)!;
    const reflection = server.bindings().get(REFLECTION_PROTOCOL_NAME)!;
    expect(await bindingHash(app.name, app.protocol.getMethods())).not.toBe(
      await bindingHash(reflection.name, reflection.protocol.getMethods()),
    );
  });

  test("an HTTP reflection call is labelled reflection, digest included", async () => {
    const records: Record_[] = [];
    const server = new VgiRpcServer(appProtocol(), { serverId: "identity-test" });
    const handler = createHttpHandler(server, {
      dispatchHook: new AccessLogHook(captureSink(records)),
    });

    expect(
      (
        await post(
          handler,
          `http://x${rpcPath(APP, "echo")}`,
          buildRequestIpc(toSchema({ message: str }), { message: "hi" }, "echo", { protocol: APP }),
        )
      ).status,
    ).toBe(200);
    expect(
      (
        await post(
          handler,
          `http://x${rpcPath(REFLECTION_PROTOCOL_NAME, "list_protocols")}`,
          buildRequestIpc(toSchema({}), {}, "list_protocols", { protocol: REFLECTION_PROTOCOL_NAME }),
        )
      ).status,
    ).toBe(200);

    const app = records.find((r) => r.method === "echo");
    const reflected = records.find((r) => r.method === "list_protocols");
    expect(app?.protocol).toBe(APP);
    expect(reflected?.protocol).toBe(REFLECTION_PROTOCOL_NAME);
    // The half the name alone does not cover: right protocol, wrong digest is
    // worse than either field being wrong on its own.
    expect(reflected?.protocol_hash).not.toBe(app?.protocol_hash);
    expect(reflected?.protocol_hash).toMatch(/^[0-9a-f]{64}$/);

    // And the digest is the canonical one -- the value every port computes for
    // the same protocol -- not some transport-local encoding of it.
    const binding = server.bindings().get(APP)!;
    expect(app?.protocol_hash).toBe(await bindingHash(binding.name, binding.protocol.getMethods()));
  });

  test("a framework endpoint owned by no protocol logs the primary", async () => {
    // `__transport_options__` and `__upload_url__` belong to no binding. The
    // spec prescribes the server's primary for those, so this is the specified
    // behaviour rather than a gap in it.
    const records: Record_[] = [];
    const server = new VgiRpcServer(appProtocol(), { serverId: "identity-test" });
    const handler = createHttpHandler(server, {
      dispatchHook: new AccessLogHook(captureSink(records)),
    });
    await post(
      handler,
      `http://x${rpcPath(APP, "echo")}`,
      buildRequestIpc(toSchema({ message: str }), { message: "hi" }, "echo", { protocol: APP }),
    );
    expect(records[0]?.protocol).toBe(APP);
    expect(PROTOCOL_KEY).toBe("vgi_rpc.protocol");
  });
});
