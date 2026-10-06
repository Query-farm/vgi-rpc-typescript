// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { schema as makeSchema, serializeBatch } from "./arrow/index.js";
import type { AuthContext } from "./auth.js";
import {
  gateProtocolVersion,
  type ProtocolBinding,
  ProtocolNotSpecifiedError,
  ProtocolNotSupportedError,
  RESERVED_PROTOCOL_PREFIX,
  validateProtocolName,
} from "./binding.js";
import { PROTOCOL_VERSION_KEY } from "./constants.js";
import { dispatchStream } from "./dispatch/stream.js";
import { dispatchUnary, type RawDispatchContext } from "./dispatch/unary.js";
import { MethodNotImplementedError, RpcError, VersionError } from "./errors.js";

import type { ExternalLocationConfig } from "./external.js";
import { GrantKeys } from "./grants.js";
import type { PeerEvidenceSet } from "./identity.js";
import type { Protocol } from "./protocol.js";
import {
  buildReflectionProtocol,
  describeRetiredMessage,
  protocolHashFor,
  REFLECTION_PROTOCOL_NAME,
  RETIRED_DESCRIBE_METHOD,
} from "./reflection.js";
import { buildIdentityProtocol, IDENTITY_PROTOCOL_NAME, IdentityImpl } from "./token-identity.js";
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

/** Options for {@link VgiRpcServer}. */
export interface VgiRpcServerOptions {
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
  /**
   * Additional application protocols, hosted after the primary in this order
   * (WIRE_PROTOCOL.md §3.1, "Hosting several application protocols").
   *
   * A {@link Protocol} carries its handlers, so each entry is the whole
   * `(protocol, implementation)` pair. The set is fixed for the server's life
   * and is the same on every transport the server is handed to --
   * {@link VgiRpcServer.serveConnection}, `serveTcp`, `serveUnix`,
   * `serveStream` and `createHttpHandler` all accept this server. Names must
   * be unique, and none may use the reserved `vgi_rpc.` prefix: reflection is
   * hosted automatically and identity through {@link identity}.
   */
  protocols?: readonly Protocol[];
  /** Host `vgi_rpc.Identity.v1` with this implementation. Only the methods
   *  whose hooks it was given are hosted; with neither hook nothing is. Host it
   *  only on servers whose transport authenticates callers (HTTP). */
  identity?: IdentityImpl;
  /**
   * Sealed-grant configuration (WIRE_PROTOCOL.md §16). `"env"` (the default)
   * reads `VGI_RPC_GRANT_KEYS` and friends -- unset means grants are off and
   * nothing changes. With keys, the framework mints sealed grants through
   * `issue_grant` (unless {@link identity} supplies `mintGrant`), and an HTTP
   * handler serving this server accepts them back as bearer credentials.
   * `null` turns grants off regardless of the environment. A malformed key
   * throws here: a worker refuses to start rather than run with a key it
   * misread.
   */
  grantKeys?: GrantKeys | "env" | null;
  /**
   * Whether EXCEPTION batches carry the remote traceback (`log_extra.traceback`).
   * Default `true`, on **every** transport: the DuckDB extension puts the
   * remote traceback into the error a user sees, and omitting it on HTTP hid
   * chained causes. `false` turns it off on all transports at once. The
   * exception type, message, code, kind and details are sent either way.
   * WIRE_PROTOCOL.md §8, "Tracebacks".
   */
  includeTracebacks?: boolean;
}

/** Who is calling, as a raw transport resolved it. */
export interface RawPeer {
  /** The authenticated caller, when the transport resolves one (TCP peer identity). */
  auth?: AuthContext;
  /** Connection evidence snapshotted for the connection lifetime. */
  evidence?: PeerEvidenceSet;
  /** Remote address, for the access log. */
  remoteAddr?: string;
}

/**
 * RPC server that reads Arrow IPC requests from stdin and writes responses to stdout.
 * Supports unary and streaming (producer/exchange) methods.
 *
 * It is also the **protocol host** every other transport serves: hand the same
 * instance to `serveTcp`, `serveUnix`, `serveStream` or `createHttpHandler`
 * and each serves exactly the protocols registered here, routed, gated and
 * error-encoded by the same code.
 */
export class VgiRpcServer {
  private protocol: Protocol;
  private serverId: string;
  private protocolVersion: string;
  private dispatchHook: DispatchHook | null = null;
  private externalConfig: ExternalLocationConfig | undefined;
  private onServeStart: ServeStartHook | null = null;
  private readonly tracebacks: boolean;
  private hostedIdentity: IdentityImpl | undefined;
  /** True once the on_serve_start hook has fired successfully. The bind
   *  state is committed only after the hook returns, so a transient
   *  failure on first request leaves it `false` and the next request
   *  re-fires rather than silently skipping. Mirrors Python 7b3999c. */
  private serveStartFired = false;
  /** The in-flight first notification, shared by concurrent connections so a
   *  launcher accepting two at once fires the hook once rather than twice. */
  private serveStartInFlight: Promise<void> | null = null;
  /** Set once a transport starts serving. The hosted set is fixed for the
   *  server's life from then on (WIRE_PROTOCOL.md §3.1): reflection output and
   *  every protocol_hash stay stable, and a protocol added late cannot be
   *  reachable on one transport and not another. */
  private sealed = false;
  /** Protocols hosted beyond the primary, keyed by wire name.
   *
   *  The primary stays in `protocol` so every existing path is untouched; it is
   *  projected into a binding on demand by {@link bindings}. */
  private extraBindings: Map<string, ProtocolBinding> = new Map();

  constructor(protocol: Protocol, options?: VgiRpcServerOptions) {
    this.protocol = protocol;
    this.serverId = options?.serverId ?? crypto.randomUUID().replace(/-/g, "").slice(0, 12);
    this.dispatchHook = options?.dispatchHook ?? null;
    this.externalConfig = options?.externalLocation;
    this.protocolVersion = options?.protocolVersion ?? "";
    this.onServeStart = options?.onServeStart ?? null;
    this.tracebacks = options?.includeTracebacks ?? true;
    // The primary is an application protocol like any other: the reserved
    // prefix rule applies to it too, however its name was derived.
    validateApplicationProtocol(protocol.name, protocol.name, "the primary protocol");
    // Application protocols first, in registration order, so reflection lists
    // them right after the primary. Framework protocols follow.
    for (const [index, extra] of (options?.protocols ?? []).entries()) {
      this.addProtocol(extra, `protocols[${index}]`);
    }
    // Registered here rather than left to the caller: a server no client can
    // introspect is not a useful default now that `__describe__` is gone, and
    // reflection is what the client bootstraps from.
    if (options?.enableDescribe ?? true) this.registerReflection();
    // Read at construction, so a malformed key refuses to start the worker
    // rather than failing the first mint.
    const grantKeys =
      options?.grantKeys === undefined || options.grantKeys === "env" ? GrantKeys.fromEnv() : options.grantKeys;
    let identity = options?.identity;
    if (grantKeys) {
      if (!identity) {
        // Grants on, no other identity hooks: the framework mints and accepts
        // its own, and hosts issue_grant alone.
        identity = new IdentityImpl({ grantKeys });
      } else if (!identity.grantKeys) {
        throw new Error(
          "grant keys were configured (grantKeys or VGI_RPC_GRANT_KEYS) and an IdentityImpl was passed " +
            "without them. Pass new IdentityImpl({ grantKeys, ... }) so the minter and the verifier use the " +
            "same keys.",
        );
      }
    }
    if (identity) this.registerIdentity(identity);
  }

  /** The hosted `vgi_rpc.Identity.v1` implementation, when there is one. Its
   *  sealed grants and `resolveToken` are what an HTTP handler accepts as
   *  bearer credentials. */
  get identity(): IdentityImpl | undefined {
    return this.hostedIdentity;
  }

  /** This server's identifier, as written on every response batch. */
  get id(): string {
    return this.serverId;
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

  /** Whether this server's error batches carry the remote traceback. One
   *  switch for every transport, on by default (WIRE_PROTOCOL.md §8). */
  get includeTracebacks(): boolean {
    return this.tracebacks;
  }

  /** Fix the hosted set. Called by every transport when it starts serving;
   *  idempotent. After this {@link addProtocol}, {@link registerReflection}
   *  and {@link registerIdentity} throw. */
  seal(): void {
    this.sealed = true;
  }

  private assertOpen(what: string): void {
    if (this.sealed) {
      throw new Error(
        `Cannot ${what}: this server has started serving. The hosted protocols are fixed for the ` +
          "server's lifetime, so register every protocol before handing the server to a transport.",
      );
    }
  }

  /** Host `vgi_rpc.Reflection.v1` on this server.
   *
   *  Registered after the application protocols so it appears in its own
   *  output without being special-cased, and so the primary stays the
   *  application protocol -- which is what the single-protocol accessors
   *  report.
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
    this.assertOpen("register reflection");
    const reflection = buildReflectionProtocol({
      listBindings: () => this.bindings() as never,
      hashFor: (name) => {
        const binding = this.bindings().get(name);
        return binding ? protocolHashFor(binding) : Promise.resolve("");
      },
      serverId: () => this.serverId,
      serverVersion: () => "",
    });
    this.hostBinding({ name: REFLECTION_PROTOCOL_NAME, protocol: reflection, versionExempt: true });
  }

  /** Host `vgi_rpc.Identity.v1` on this server, when the deployment configured
   *  it. Prefer the `identity` constructor option, which registers it in the
   *  canonical place (after reflection).
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
   *  Reachable on every transport this server is handed to. Its guards read
   *  the caller's `AuthContext`: HTTP and TCP-with-peer-identity supply one;
   *  stdio and unix do not, so there it fails closed. */
  registerIdentity(identity: IdentityImpl): void {
    this.assertOpen("register identity");
    const protocol = buildIdentityProtocol(identity);
    if (!protocol) return;
    this.hostedIdentity = identity;
    this.hostBinding({ name: IDENTITY_PROTOCOL_NAME, protocol, versionExempt: false });
  }

  /** Host an additional application protocol alongside the primary.
   *
   *  Prefer the `protocols` constructor option, which fixes the set at
   *  construction. Refused once the server has started serving, for a
   *  reserved `vgi_rpc.` name (checked on the binding name *and* the
   *  protocol's own name, however either was derived), and for a duplicate
   *  name. */
  addProtocol(target: Protocol | ProtocolBinding, label = "addProtocol"): void {
    this.assertOpen("add a protocol");
    const binding: ProtocolBinding =
      "protocol" in target && "versionExempt" in target
        ? target
        : { name: (target as Protocol).name, protocol: target as Protocol, versionExempt: false };
    validateApplicationProtocol(binding.name, binding.protocol.name, label);
    // An application binding is never version-exempt: exemption is for
    // reflection, which the framework registers itself.
    this.hostBinding({ ...binding, versionExempt: false });
  }

  /** Register a binding. The framework's own protocols come through here with
   *  their reserved names; application ones only after
   *  {@link validateApplicationProtocol}. */
  private hostBinding(binding: ProtocolBinding): void {
    validateProtocolName(binding.name, true);
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

  /** Fire the on_serve_start hook once for this server. Idempotent on
   *  success; re-throws on failure without committing, so the next request
   *  retries. Concurrent first requests share one attempt. Also seals the
   *  hosted set. */
  async notifyTransport(kind: TransportKind): Promise<void> {
    this.seal();
    if (this.serveStartFired) return;
    if (this.serveStartInFlight) {
      await this.serveStartInFlight;
      return;
    }
    if (!this.onServeStart) {
      this.serveStartFired = true;
      return;
    }
    const hook = this.onServeStart;
    const attempt = Promise.resolve().then(() => hook(kind));
    this.serveStartInFlight = attempt;
    try {
      await attempt;
      this.serveStartFired = true;
    } finally {
      if (this.serveStartInFlight === attempt) this.serveStartInFlight = null;
    }
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
   * @param transportKind reported to the `on_serve_start` hook, and deciding
   *   the traceback default (default `PIPE`).
   * @param peer the caller, when the transport resolved one.
   */
  async serveConnection(
    readable: ReadableStream<Uint8Array> | NodeJS.ReadableStream,
    writable?: number | import("node:net").Socket | ByteSink,
    transportKind: TransportKind = TransportKind.PIPE,
    peer: RawPeer = {},
  ): Promise<void> {
    this.seal();
    const reader = await IpcStreamReader.create(readable);
    const writer = new IpcStreamWriter(writable);

    try {
      while (true) {
        // Fire on_serve_start lazily so the hook can do work that depends on
        // the transport binding. Inside the loop so a failure on the very
        // first request can be retried.
        await this.notifyTransport(transportKind);
        await this.serveRequest(reader, writer, transportKind, peer);
      }
    } catch (e: any) {
      if (isConnectionClosed(e)) return;
      // ArrowInvalid or unexpected error
      throw e;
    } finally {
      await reader.cancel();
    }
  }

  /**
   * Read and answer exactly one request on a raw (framed Arrow IPC) transport.
   *
   * The one request path shared by stdio, unix, TCP and byte-stream serving,
   * so routing, the per-binding version gate, error encoding and the dispatch
   * hook cannot drift between them. Throws an EOF-shaped error when the peer
   * has closed; {@link isConnectionClosed} recognises it.
   *
   * @internal Used by the launchers; not a stable API.
   */
  async serveRequest(
    reader: IpcStreamReader,
    writer: IpcStreamWriter,
    transportKind: TransportKind,
    peer: RawPeer = {},
  ): Promise<void> {
    const includeTraceback = this.tracebacks;
    const stream = await reader.readStream();
    if (!stream) {
      throw new Error("EOF");
    }

    const { schema, batches } = stream;
    if (batches.length === 0) {
      const err = new RpcError("ProtocolError", "Request stream contains no batches", "");
      const errBatch = buildErrorBatch(EMPTY_SCHEMA, err, this.serverId, null, includeTraceback);
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
      const errBatch = buildErrorBatch(EMPTY_SCHEMA, e, this.serverId, null, includeTraceback);
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
      const errBatch = buildErrorBatch(EMPTY_SCHEMA, error as Error, this.serverId, requestId, includeTraceback);
      await writer.writeStream(EMPTY_SCHEMA, [errBatch]);
      return;
    }

    try {
      validateRequestSchema(schema, method.paramsSchema, methodName);
    } catch (error) {
      const errSchema = method.type === MethodType.UNARY ? method.resultSchema : EMPTY_SCHEMA;
      const errBatch = buildErrorBatch(errSchema, error as Error, this.serverId, requestId, includeTraceback);
      await writer.writeStream(errSchema, [errBatch]);
      return;
    }

    // Application-protocol-version gate, against the binding that owns the
    // resolved method. A version-exempt binding (reflection) is skipped: it is
    // what a mismatched client calls to learn what mismatched.
    if (!binding.versionExempt) {
      try {
        gateProtocolVersion(binding, batch.metadata?.get(PROTOCOL_VERSION_KEY));
      } catch (exc) {
        const errSchema = method.type === MethodType.UNARY ? method.resultSchema : EMPTY_SCHEMA;
        const errBatch = buildErrorBatch(errSchema, exc as Error, this.serverId, requestId, includeTraceback);
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
      principal: peer.auth?.principal ?? "",
      authDomain: peer.auth?.domain ?? "",
      authenticated: peer.auth?.authenticated ?? false,
      remoteAddr: peer.remoteAddr ?? "",
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

    const ctx: RawDispatchContext = {
      serverId: this.serverId,
      requestId,
      includeTraceback,
      externalConfig: this.externalConfig,
      kind: transportKind,
      authContext: peer.auth,
      peerEvidence: peer.evidence,
    };
    try {
      if (method.type === MethodType.UNARY) {
        await dispatchUnary(method, params, writer, ctx);
      } else {
        await dispatchStream(method, params, writer, reader, ctx);
      }
    } catch (e) {
      dispatchError = e instanceof Error ? e : new Error(String(e));
      throw e;
    } finally {
      this.dispatchHook?.onDispatchEnd(token, info, stats, dispatchError);
    }
  }
}

/** Refuse an application protocol whose name is reserved or malformed.
 *
 *  Checked on *both* names a registration carries -- the binding's routing
 *  key and the protocol's own name -- because the reserved-prefix rule
 *  applies however a name was derived (WIRE_PROTOCOL.md §3.1). */
function validateApplicationProtocol(bindingName: string, protocolName: string, label: string): void {
  for (const name of new Set([bindingName, protocolName])) {
    if (name.startsWith(RESERVED_PROTOCOL_PREFIX)) {
      throw new Error(
        `${label}: protocol name '${name}' claims the reserved '${RESERVED_PROTOCOL_PREFIX}' prefix, which is ` +
          "for protocols the framework defines. Reflection is hosted automatically; identity is hosted " +
          "through the `identity` option.",
      );
    }
    // Grammar only (`allowReserved`): the reserved-prefix rule is the check
    // above, kept single so it names the label and cannot be shadowed.
    try {
      validateProtocolName(name, true);
    } catch (e) {
      throw new Error(`${label}: ${(e as Error).message}`);
    }
  }
}

/** Whether a raw-transport read failed because the peer closed the connection
 *  -- the clean way a serve loop ends. */
export function isConnectionClosed(e: unknown): boolean {
  const err = e as { code?: string; message?: string } | null;
  return Boolean(
    err?.message?.includes("closed") ||
      err?.message?.includes("Expected Schema Message") ||
      err?.message?.includes("null or length 0") ||
      err?.message?.includes("EOF") ||
      err?.code === "EPIPE" ||
      err?.code === "ERR_STREAM_PREMATURE_CLOSE" ||
      err?.code === "ERR_STREAM_DESTROYED",
  );
}
