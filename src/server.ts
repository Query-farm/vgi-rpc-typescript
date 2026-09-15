// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { schema as makeSchema, serializeBatch } from "./arrow/index.js";
import {
  type ProtocolBinding,
  ProtocolNotSpecifiedError,
  ProtocolNotSupportedError,
  validateProtocolName,
} from "./binding.js";
import { PROTOCOL_VERSION_KEY } from "./constants.js";
import { dispatchStream } from "./dispatch/stream.js";
import { dispatchUnary } from "./dispatch/unary.js";
import {
  MethodNotImplementedError,
  ProtocolVersionError,
  parseProtocolVersion,
  RpcError,
  VersionError,
} from "./errors.js";

import type { ExternalLocationConfig } from "./external.js";
import type { Protocol } from "./protocol.js";
import {
  buildReflectionProtocol,
  describeRetiredMessage,
  protocolHashFor,
  REFLECTION_PROTOCOL_NAME,
  RETIRED_DESCRIBE_METHOD,
} from "./reflection.js";
import { buildIdentityProtocol, IDENTITY_PROTOCOL_NAME, type IdentityImpl } from "./token-identity.js";
import {
  type CallStatistics,
  type DispatchHook,
  type DispatchInfo,
  type MethodDefinition,
  MethodType,
  type ServeStartHook,
  TransportKind,
} from "./types.js";
import { IpcStreamReader } from "./wire/reader.js";
import { applyDefaults, parseRequest, validateRequestSchema } from "./wire/request.js";
import { buildErrorBatch } from "./wire/response.js";
import { type ByteSink, IpcStreamWriter } from "./wire/writer.js";

const EMPTY_SCHEMA = makeSchema([]);

function randomStreamId(): string {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  let out = "";
  for (let i = 0; i < bytes.length; i++) {
    out += bytes[i].toString(16).padStart(2, "0");
  }
  return out;
}

/**
 * RPC server that reads Arrow IPC requests from stdin and writes responses to stdout.
 * Supports unary and streaming (producer/exchange) methods.
 */
export class VgiRpcServer {
  private protocol: Protocol;
  private serverId: string;
  private protocolVersion: string;
  private dispatchHook: DispatchHook | null = null;
  private externalConfig: ExternalLocationConfig | undefined;
  private onServeStart: ServeStartHook | null = null;
  /** True once the on_serve_start hook has fired successfully. The bind
   *  state is committed only after the hook returns, so a transient
   *  failure on first request leaves it `false` and the next request
   *  re-fires rather than silently skipping. Mirrors Python 7b3999c. */
  private serveStartFired = false;
  /** Protocols hosted beyond the primary, keyed by wire name.
   *
   *  The primary stays in `protocol` so every existing path is untouched; it is
   *  projected into a binding on demand by {@link bindings}. */
  private extraBindings: Map<string, ProtocolBinding> = new Map();

  constructor(
    protocol: Protocol,
    options?: {
      /** Host `vgi_rpc.Reflection.v1`. Default `true`.
       *
       *  Named for the `__describe__` method it used to switch on, and kept
       *  under that name across the fleet: what it gates is introspection,
       *  and introspection is now a co-hosted protocol rather than a reserved
       *  method answered before dispatch. */
      enableDescribe?: boolean;
      /** Opaque per-process server identifier surfaced to clients and the landing page. */
      serverId?: string;
      /** Hook invoked around each dispatched request (tracing/metrics/auth enrichment). */
      dispatchHook?: DispatchHook;
      /** Configuration for externalizing oversized record batches to blob storage. */
      externalLocation?: ExternalLocationConfig;
      /** Protocol version string reported in the service description. */
      protocolVersion?: string;
      /** Lifecycle hook fired once before the first dispatched request. */
      onServeStart?: ServeStartHook;
    },
  ) {
    this.protocol = protocol;
    this.serverId = options?.serverId ?? crypto.randomUUID().replace(/-/g, "").slice(0, 12);
    this.dispatchHook = options?.dispatchHook ?? null;
    this.externalConfig = options?.externalLocation;
    this.protocolVersion = options?.protocolVersion ?? "";
    this.onServeStart = options?.onServeStart ?? null;
    // Registered here rather than left to the caller: a server no client can
    // introspect is not a useful default now that `__describe__` is gone, and
    // reflection is what the client bootstraps from.
    if (options?.enableDescribe ?? true) this.registerReflection();
  }

  /** Every protocol this server hosts, primary first.
   *
   *  The primary is projected from the server's own protocol rather than
   *  stored, so the existing registration paths keep working untouched. */
  bindings(): Map<string, ProtocolBinding> {
    const out = new Map<string, ProtocolBinding>();
    out.set(this.protocol.name, {
      name: this.protocol.name,
      protocol: this.protocol,
      versionExempt: false,
    });
    for (const [name, b] of this.extraBindings) out.set(name, b);
    return out;
  }

  /** Host `vgi_rpc.Reflection.v1` on this server.
   *
   *  Registered after the application protocol so it appears in its own output
   *  without being special-cased, and so the primary stays the application
   *  protocol -- which is what the single-protocol accessors report.
   *
   *  The binding is version-exempt: this is the protocol a version-mismatched
   *  client calls to learn *what* mismatched, and gating it would deny the
   *  client the diagnosis it came for.
   *
   *  Idempotent, because the constructor already calls it unless the
   *  deployment opted out: an explicit second call should not turn a working
   *  server into a duplicate-name error. */
  registerReflection(): void {
    if (this.extraBindings.has(REFLECTION_PROTOCOL_NAME)) return;
    const reflection = buildReflectionProtocol({
      listBindings: () => this.bindings() as never,
      hashFor: (name) => {
        const binding = this.bindings().get(name);
        return binding ? protocolHashFor(binding) : Promise.resolve("");
      },
      serverId: () => this.serverId,
      serverVersion: () => "",
    });
    this.addProtocol({ name: REFLECTION_PROTOCOL_NAME, protocol: reflection, versionExempt: true }, true);
  }

  /** Host `vgi_rpc.Identity.v1` on this server, when the deployment configured
   *  it.
   *
   *  Call this *after* {@link registerReflection} so identity appears in
   *  reflection's output. (`listBindings` is read at request time here, so the
   *  order is a convention rather than a mechanism -- but it is the convention
   *  every port follows, and a port that later caches the listing would break
   *  silently without it.)
   *
   *  Only the methods whose hooks the deployment supplied are hosted, so the
   *  binding's `protocol_hash` narrows with them: a method this worker cannot
   *  answer is better *absent* than routed-and-refusing, because then what the
   *  server hosts describes what it actually does and a client learns it from
   *  reflection rather than by calling and reading an error. With neither hook
   *  configured nothing is registered at all -- which is what keeps a
   *  dependency upgrade from growing a credential-to-identity oracle on every
   *  existing worker.
   *
   *  Not version-exempt: the binding declares no `protocolVersion`, so the gate
   *  never fires, and exempting it would be a claim rather than a fact.
   *
   *  **KNOWN GAP: this protocol is reachable over HTTP and stdio only.**
   *  `createHttpHandler` accepts a `ProtocolHost`, and {@link serveConnection}
   *  is a method on this class, so both can carry a secondary binding. The
   *  three launcher transports cannot: `serveTcp`, `serveUnix` and
   *  `serveStream` each take a bare {@link Protocol} and construct their *own*
   *  `VgiRpcServer` internally, so there is no seam through which a caller can
   *  register identity (or any other secondary protocol) on them.
   *
   *  That bites hardest on TCP, which is the one raw transport that does
   *  resolve a peer identity into an `AuthContext` -- so it is the only place
   *  `introspect_token` could succeed for an allowlisted caller off HTTP, and
   *  it cannot host the method to try. `serveConnection` supplies no
   *  `AuthContext` at all, so identity there is reachable but fails closed,
   *  which is correct but is only half the property
   *  IDENTITY_CONFORMANCE_FIXTURE.md §7 asks a port to cover; the allowlisted
   *  half of `test/token-identity.test.ts`'s raw-transport block rides HTTP
   *  for this reason and says so.
   *
   *  The fix is to widen those three signatures to `Protocol | ProtocolHost`
   *  the way `createHttpHandler` already is. Left undone deliberately: it is a
   *  public API change rather than a wiring fix, and naming it beats bundling
   *  it into an unrelated commit. */
  registerIdentity(identity: IdentityImpl): void {
    const protocol = buildIdentityProtocol(identity);
    if (!protocol) return;
    this.addProtocol({ name: IDENTITY_PROTOCOL_NAME, protocol, versionExempt: false }, true);
  }

  /** Host an additional protocol alongside the primary.
   *
   *  `allowReserved` is for the framework's own protocols only; an application
   *  passing `true` would be able to shadow reflection. */
  addProtocol(binding: ProtocolBinding, allowReserved = false): void {
    validateProtocolName(binding.name, allowReserved);
    if (binding.name === this.protocol.name || this.extraBindings.has(binding.name)) {
      throw new Error(
        `Two protocols are hosted under the same name '${binding.name}'. ` +
          `The name is the routing key, so it must be unique.`,
      );
    }
    this.extraBindings.set(binding.name, binding);
  }

  /** Resolve one request's (protocol, method) pair.
   *
   *  The routing key is required, including against a server hosting exactly
   *  one protocol: an exemption would let an intermediary that rebuilds a
   *  request and drops the field land silently on whichever protocol happened
   *  to be first, rather than being told.
   *
   *  The three failures are deliberately distinct, and a client depends on the
   *  difference -- particularly the last, which is the documented
   *  capability-probe signal: a client testing for an optional method must be
   *  able to tell "you do not speak this protocol" from "you speak it but lack
   *  this method". */
  resolve(
    protocol: string,
    method: string,
  ): {
    /** The resolved method definition. */
    method: MethodDefinition;
    /** The binding that owns it -- the source of the access record's identity. */
    binding: ProtocolBinding;
  } {
    // Retired rather than merely absent, and the two are indistinguishable
    // from the caller's side while needing opposite fixes -- one is a client
    // to update, the other a server to reconfigure. Answered before the
    // routing checks because a stale client does not name a protocol either,
    // and "you failed to route" is not the diagnosis it needs.
    if (method === RETIRED_DESCRIBE_METHOD) {
      throw new MethodNotImplementedError(describeRetiredMessage());
    }
    const all = this.bindings();
    const hosted = [...all.keys()].sort();
    if (!protocol) throw new ProtocolNotSpecifiedError(hosted);
    // Checked before the lookup so an arbitrary request-supplied string never
    // reaches an error message, a log field or a metric label.
    try {
      validateProtocolName(protocol, true);
    } catch (e) {
      throw new ProtocolNotSupportedError(`'vgi_rpc.protocol' is not a protocol name: ${(e as Error).message}`);
    }
    const binding = all.get(protocol);
    if (!binding) throw ProtocolNotSupportedError.notHosted(protocol, hosted);
    const found = binding.protocol.getMethod(method);
    if (!found) {
      const available = binding.protocol.methodNames().sort();
      throw new MethodNotImplementedError(
        `Protocol '${protocol}' has no method '${method}'. Available: [${available.join(", ")}].`,
      );
    }
    return { method: found, binding };
  }

  /** Fire the on_serve_start hook once for this transport. Idempotent
   *  on success — re-throws on failure without committing the bind. */
  private async notifyTransport(kind: TransportKind): Promise<void> {
    if (this.serveStartFired) return;
    if (this.onServeStart) {
      await this.onServeStart(kind);
    }
    this.serveStartFired = true;
  }

  /** Validate a client's declared protocol_version against the Protocol's
   *  declared version. Caller invokes only when
   *  `protocol.protocolVersionParts` is non-null. Mirrors Python's
   *  `RpcServer._check_protocol_version`: exact major+minor match, patch
   *  ignored; directional error message names which side is older. */
  private checkProtocolVersion(clientVersion: string | undefined, binding?: ProtocolBinding): void {
    const target = binding?.protocol ?? this.protocol;
    const serverParts = target.protocolVersionParts!;
    const serverVersion = target.protocolVersion;
    if (clientVersion === undefined) {
      throw new ProtocolVersionError(
        "VGI client/worker protocol_version mismatch.\n" +
          "  Client: <not declared>\n" +
          `  Server: ${serverVersion}\n` +
          "  Direction: the client did not send a vgi_rpc.protocol_version " +
          "metadata key. This is either a vgi-rpc framework bug or a " +
          "non-VGI client connecting to a VGI worker.",
      );
    }
    let clientParts: readonly [number, number, number];
    try {
      clientParts = parseProtocolVersion(clientVersion);
    } catch {
      throw new ProtocolVersionError(
        "VGI client/worker protocol_version mismatch.\n" +
          `  Client: ${clientVersion}\n` +
          `  Server: ${serverVersion}\n` +
          "  Direction: client sent a malformed protocol_version. " +
          "Expected canonical semver MAJOR.MINOR.PATCH.",
      );
    }
    if (clientParts[0] === serverParts[0] && clientParts[1] === serverParts[1]) {
      return;
    }
    const clientOlder =
      clientParts[0] < serverParts[0] || (clientParts[0] === serverParts[0] && clientParts[1] < serverParts[1]);
    const direction = clientOlder
      ? `client is too old; upgrade the VGI extension/client to a version supporting protocol_version ${serverVersion}.`
      : `server is too old; upgrade the VGI worker to a version supporting protocol_version ${clientVersion}.`;
    throw new ProtocolVersionError(
      "VGI client/worker protocol_version mismatch.\n" +
        `  Client: ${clientVersion}\n` +
        `  Server: ${serverVersion}\n` +
        `  Direction: ${direction}`,
    );
  }

  /** Start the server loop over stdin/stdout. Reads requests until stdin closes. */
  async run(): Promise<void> {
    // Warn if running interactively
    if (process.stdin.isTTY || process.stdout.isTTY) {
      process.stderr.write(
        "WARNING: This process communicates via Arrow IPC on stdin/stdout " +
          "and is not intended to be run interactively.\n" +
          "It should be launched as a subprocess by an RPC client " +
          "(e.g. vgi_rpc.connect()).\n",
      );
    }
    const stdin = process.stdin as unknown as ReadableStream<Uint8Array>;
    // writable omitted → IpcStreamWriter defaults to the stdout fd.
    await this.serveConnection(stdin);
  }

  /**
   * Serve requests over an explicit byte-stream pair until the readable ends —
   * the transport-agnostic core that {@link run} (stdin/stdout) is built on.
   *
   * Use this to serve over any duplex channel that the stdio/unix/tcp helpers
   * don't cover: a Web Worker / `MessagePort` bridge, an in-memory pipe, or a
   * pre-connected socket. The loop, on_serve_start firing, and EOF/broken-pipe
   * handling are identical to {@link run}.
   *
   * @param readable incoming request bytes — a web `ReadableStream<Uint8Array>`
   *   or a Node `Readable` (e.g. a `Duplex` bridging a MessagePort).
   * @param writable outgoing response sink — a stdout-like fd number, or a
   *   `net.Socket` / structurally-compatible `Duplex`. Omit for the stdout fd.
   * @param transportKind reported to the `on_serve_start` hook (default `PIPE`).
   */
  async serveConnection(
    readable: ReadableStream<Uint8Array> | NodeJS.ReadableStream,
    writable?: number | import("node:net").Socket | ByteSink,
    transportKind: TransportKind = TransportKind.PIPE,
  ): Promise<void> {
    const reader = await IpcStreamReader.create(readable);
    const writer = new IpcStreamWriter(writable);

    try {
      while (true) {
        // Fire on_serve_start lazily so the hook can do work that depends on
        // the transport binding. Inside the loop so a failure on the very
        // first request can be retried.
        await this.notifyTransport(transportKind);
        await this.serveOne(reader, writer, transportKind);
      }
    } catch (e: any) {
      // EOF or broken pipe / closed channel → clean exit
      if (
        e.message?.includes("closed") ||
        e.message?.includes("Expected Schema Message") ||
        e.message?.includes("null or length 0") ||
        e.code === "EPIPE" ||
        e.code === "ERR_STREAM_PREMATURE_CLOSE" ||
        e.code === "ERR_STREAM_DESTROYED" ||
        (e instanceof Error && e.message.includes("EOF"))
      ) {
        return;
      }
      // ArrowInvalid or unexpected error
      throw e;
    } finally {
      await reader.cancel();
    }
  }

  private async serveOne(
    reader: IpcStreamReader,
    writer: IpcStreamWriter,
    transportKind: TransportKind,
  ): Promise<void> {
    const stream = await reader.readStream();
    if (!stream) {
      throw new Error("EOF");
    }

    const { schema, batches } = stream;
    if (batches.length === 0) {
      const err = new RpcError("ProtocolError", "Request stream contains no batches", "");
      const errBatch = buildErrorBatch(EMPTY_SCHEMA, err, this.serverId, null);
      await writer.writeStream(EMPTY_SCHEMA, [errBatch]);
      return;
    }

    const batch = batches[0];
    let methodName: string;
    let protocolName: string;
    let params: Record<string, any>;
    let requestId: string | null;

    try {
      const parsed = parseRequest(schema, batch);
      methodName = parsed.methodName;
      protocolName = parsed.protocol;
      params = parsed.params;
      requestId = parsed.requestId;
    } catch (e: any) {
      // Write error response for protocol/version errors
      const errBatch = buildErrorBatch(EMPTY_SCHEMA, e, this.serverId, null);
      await writer.writeStream(EMPTY_SCHEMA, [errBatch]);
      if (e instanceof VersionError || e instanceof RpcError) {
        return; // Continue serving
      }
      throw e;
    }

    // Resolve (protocol, method). Method names may collide across protocols,
    // so the routing key is part of the lookup rather than a label on it.
    let method: MethodDefinition;
    let binding: ProtocolBinding;
    try {
      ({ method, binding } = this.resolve(protocolName, methodName));
    } catch (error) {
      const errBatch = buildErrorBatch(EMPTY_SCHEMA, error as Error, this.serverId, requestId);
      await writer.writeStream(EMPTY_SCHEMA, [errBatch]);
      return;
    }

    try {
      validateRequestSchema(schema, method.paramsSchema, methodName);
    } catch (error) {
      const errSchema = method.type === MethodType.UNARY ? method.resultSchema : EMPTY_SCHEMA;
      const errBatch = buildErrorBatch(errSchema, error as Error, this.serverId, requestId);
      await writer.writeStream(errSchema, [errBatch]);
      return;
    }

    // Application-protocol-version gate, against the binding that owns the
    // resolved method. A server hosting several protocols has a version per
    // binding and no single "server version"; gating a secondary against the
    // primary rejects correct callers and names the wrong protocol when it
    // does. A version-exempt binding (reflection) is skipped: it is what a
    // mismatched client calls to learn what mismatched.
    if (!binding.versionExempt && binding.protocol.protocolVersionParts !== null) {
      try {
        const md = batch.metadata;
        this.checkProtocolVersion(md?.get(PROTOCOL_VERSION_KEY), binding);
      } catch (exc) {
        const errSchema = method.type === MethodType.UNARY ? method.resultSchema : EMPTY_SCHEMA;
        const errBatch = buildErrorBatch(errSchema, exc as Error, this.serverId, requestId);
        await writer.writeStream(errSchema, [errBatch]);
        return;
      }
    }

    // Dispatch based on method type, with optional hook
    const methodType = method.type === MethodType.UNARY ? "unary" : "stream";

    // Capture self-contained IPC bytes of the request batch for the access log.
    // Only pay the full IPC re-encode when a hook will actually read them — the
    // default (no dispatchHook) path skips it entirely.
    let requestData: Uint8Array | undefined;
    if (this.dispatchHook) {
      try {
        requestData = serializeBatch(batch as any);
      } catch {
        // best-effort; observability must not fail dispatch
      }
    }

    let streamId: string | undefined;
    if (methodType === "stream") {
      streamId = randomStreamId();
    }

    const info: DispatchInfo = {
      method: methodName,
      methodType,
      serverId: this.serverId,
      requestId,
      // The protocol that owns the dispatched method, and *its* canonical
      // digest. Both from the resolved binding, never the server's primary:
      // `protocol_hash` is the registry key for decoding an archived record,
      // so a record naming one protocol while carrying another's is decoded
      // against the wrong description -- and passes the schema while doing it.
      // Read inline rather than through a local so the pairing is visible at
      // the emit site, which is what `test/dispatch-identity.test.ts` checks.
      protocol: binding.name,
      protocolHash: await protocolHashFor(binding),
      protocolVersion: this.protocolVersion,
      kind: transportKind,
      principal: "",
      authDomain: "",
      authenticated: false,
      remoteAddr: "",
      requestData,
      streamId,
    };
    const stats: CallStatistics = {
      inputBatches: 0,
      outputBatches: 0,
      inputRows: 0,
      outputRows: 0,
      inputBytes: 0,
      outputBytes: 0,
    };

    const token = this.dispatchHook?.onDispatchStart(info);
    let dispatchError: Error | undefined;

    applyDefaults(params, method.defaults);

    try {
      if (method.type === MethodType.UNARY) {
        await dispatchUnary(method, params, writer, this.serverId, requestId, this.externalConfig, transportKind);
      } else {
        await dispatchStream(
          method,
          params,
          writer,
          reader,
          this.serverId,
          requestId,
          this.externalConfig,
          transportKind,
        );
      }
    } catch (e) {
      dispatchError = e instanceof Error ? e : new Error(String(e));
      throw e;
    } finally {
      this.dispatchHook?.onDispatchEnd(token, info, stats, dispatchError);
    }
  }
}
