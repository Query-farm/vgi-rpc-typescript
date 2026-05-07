// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Tiny vgi-rpc worker fixture for launcher integration tests.
// Reads `--unix PATH` and `--idle-timeout SEC` from argv (the launcher
// contract), runs serveUnix, and exits when the idle timer fires.

import { serveUnix } from "../../src/launcher/index.js";
import { Protocol } from "../../src/protocol.js";
import { str } from "../../src/schema.js";

function arg(flag: string): string | undefined {
  const i = process.argv.indexOf(flag);
  return i >= 0 && i + 1 < process.argv.length ? process.argv[i + 1] : undefined;
}

const unix = arg("--unix");
if (!unix) {
  process.stderr.write("missing --unix PATH\n");
  process.exit(64);
}
const idleStr = arg("--idle-timeout") ?? "5";
const idle = Number(idleStr);

const protocol = new Protocol("EchoSvc").unary("ping", {
  params: { msg: str },
  result: { msg: str },
  handler: async (params) => ({ msg: String(params.msg) }),
});

const handle = await serveUnix(protocol, {
  unixPath: unix,
  idleTimeout: idle,
  startupGraceSeconds: 1,
});

await handle.done;
