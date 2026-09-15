// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import type { AuthContext } from "../auth.js";
import { type ExternalLocationConfig, maybeExternalizeBatch } from "../external.js";
import type { PeerEvidenceSet } from "../identity.js";
import type { MethodDefinition, TransportKind } from "../types.js";
import { OutputCollector } from "../types.js";
import { buildErrorBatch, buildResultBatch } from "../wire/response.js";
import type { IpcStreamWriter } from "../wire/writer.js";

/**
 * Dispatch a unary RPC call.
 * Calls the handler with parsed params, writes result or error batch.
 * Supports client-directed logging via ctx.clientLog().
 */
export async function dispatchUnary(
  method: MethodDefinition,
  params: Record<string, any>,
  writer: IpcStreamWriter,
  serverId: string,
  requestId: string | null,
  externalConfig?: ExternalLocationConfig,
  kind?: TransportKind,
  authContext?: AuthContext,
  peerEvidence?: PeerEvidenceSet,
): Promise<void> {
  const schema = method.resultSchema;
  const out = new OutputCollector(schema, true, serverId, requestId, authContext, undefined, kind, { peerEvidence });

  try {
    // The context is a positional argument supplied unconditionally, for every
    // binding -- which is what keeps this port clear of the bug that made
    // `vgi_rpc.Identity.v1` uncallable in the Python reference for the whole
    // life of its implementation: `ctx` injection there resolved ctx-taking
    // method names against the server's *primary* binding, so a secondary
    // protocol's methods received none and every call died on a missing
    // argument before any guard ran.
    //
    // RECORDED FINDING, because it is not obvious and cost a mutation run to
    // learn: **removing the context here is invisible to the shared
    // conformance suite.** The identity conformance group is HTTP-only by
    // design (its guards all read an authenticated caller and HTTP is the
    // transport that carries one), and the HTTP handler has its own unary
    // dispatch that never enters this function -- so a mutation replacing `out`
    // with `undefined` passes all 77 cases green. The only thing that kills it
    // is the raw-transport block in `test/token-identity.test.ts`
    // ("dispatch over a raw transport"), which drives the protocol over the
    // stdio framing and expects the guard's typed refusal rather than a
    // `TypeError`. That is precisely why IDENTITY_CONFORMANCE_FIXTURE.md §7
    // asks for port-local coverage, and it means the shared group alone is not
    // sufficient evidence for this property in any port. Do not delete those
    // cases on the grounds that conformance covers them.
    const result = await method.handler!(params, out);
    let resultBatch = buildResultBatch(schema, result, serverId, requestId);
    if (externalConfig) {
      resultBatch = await maybeExternalizeBatch(resultBatch, externalConfig);
    }
    // Collect log batches (from clientLog) + result batch
    const batches = [...out.batches.map((b) => b.batch), resultBatch];
    await writer.writeStream(schema, batches);
  } catch (error: any) {
    const batch = buildErrorBatch(schema, error, serverId, requestId);
    await writer.writeStream(schema, [batch]);
  }
}
