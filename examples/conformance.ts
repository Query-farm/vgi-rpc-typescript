/**
 * Conformance worker — 43-method reference RPC service exercising all framework
 * capabilities. Used by the Python CLI to verify wire-protocol compatibility.
 *
 * Run: bun run examples/conformance.ts
 */
import { VgiRpcServer } from "../src/index.js";
import { protocol } from "./conformance-protocol.js";

const server = new VgiRpcServer(protocol, { enableDescribe: true });
server.run();
