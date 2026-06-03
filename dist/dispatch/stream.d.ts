import { type ExternalLocationConfig } from "../external.js";
import type { MethodDefinition, TransportKind } from "../types.js";
import type { IpcStreamReader } from "../wire/reader.js";
import type { IpcStreamWriter } from "../wire/writer.js";
/**
 * Dispatch a stream RPC call (producer or exchange).
 *
 * Producer streams (empty input schema):
 * - Client sends tick batches (empty schema, 0 rows)
 * - Server reads each tick, calls produce(state, out)
 * - Server writes output batch(es) for each tick
 * - When produce() calls out.finish(), server closes output stream
 *
 * Exchange streams (real input schema):
 * - Client sends data batches
 * - Server reads each batch, calls exchange(state, input, out)
 * - Server writes output batch(es) for each input
 * - Stream ends when client closes input (EOS)
 */
export declare function dispatchStream(method: MethodDefinition, params: Record<string, any>, writer: IpcStreamWriter, reader: IpcStreamReader, serverId: string, requestId: string | null, externalConfig?: ExternalLocationConfig, kind?: TransportKind): Promise<void>;
//# sourceMappingURL=stream.d.ts.map