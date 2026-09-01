import type { AuthContext } from "../auth.js";
import { type ExternalLocationConfig } from "../external.js";
import type { PeerEvidenceSet } from "../identity.js";
import type { MethodDefinition, TransportKind } from "../types.js";
import type { IpcStreamWriter } from "../wire/writer.js";
/**
 * Dispatch a unary RPC call.
 * Calls the handler with parsed params, writes result or error batch.
 * Supports client-directed logging via ctx.clientLog().
 */
export declare function dispatchUnary(method: MethodDefinition, params: Record<string, any>, writer: IpcStreamWriter, serverId: string, requestId: string | null, externalConfig?: ExternalLocationConfig, kind?: TransportKind, authContext?: AuthContext, peerEvidence?: PeerEvidenceSet): Promise<void>;
//# sourceMappingURL=unary.d.ts.map