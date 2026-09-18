// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// `launch()` for the socket at argv[2], run under **Node** rather than Bun.
//
// Bundled for Node by `test/launcher.test.ts`, for the one assertion Bun cannot
// make: on Linux, Node reports a connect to a full AF_UNIX accept queue as
// `EAGAIN`, while Bun (1.4.2) reports it as `ECONNREFUSED`, the same as no
// listener at all. So only under Node can the probe tell a busy worker from a
// dead one however long it stays busy.
//
// The worker argv would fail loudly ("exited before readiness") if launch()
// spawned it, so a clean exit printing the socket path means it reused the
// worker it found.

import { launch } from "../../src/launcher/launch.js";

const sock = process.argv[2];
const path = await launch({
  workerArgv: [process.execPath, "-e", "process.exit(3)"],
  socketPath: sock,
  connectTimeout: 5,
  workerStartupTimeout: 5,
});
process.stdout.write(`${JSON.stringify({ path })}\n`);
