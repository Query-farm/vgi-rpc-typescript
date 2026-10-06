// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * The error model (WIRE_PROTOCOL.md §8), multi-protocol hosting (§3.1) and the
 * identity translation rule (§16), port-locally.
 *
 * The shared suite asserts all of this across the wire; these pin the pieces
 * a cross-port test cannot isolate -- the cap boundary in bytes, each emission
 * rule on its own, every launcher taking the same host -- so a mutation shows
 * up here first and names the guard.
 */

import { describe, expect, test } from "bun:test";
import { Schema } from "@query-farm/apache-arrow";
import { AuthContext } from "../src/auth.js";
import { httpConnect, type RpcClient } from "../src/client/connect.js";
import { tcpConnect } from "../src/client/tcp.js";
import {
  AUTH_UNAVAILABLE_PURPOSE,
  buildSecondaryProtocol,
  conformanceIdentity,
  expectedFailDetails,
  SECONDARY_PROTOCOL_NAME,
  TOKEN_AUTH_UNAVAILABLE,
} from "../src/conformance/index.js";
import { ERROR_CODE_KEY, ERROR_DETAILS_KEY, ERROR_KIND_KEY, LOG_EXTRA_KEY } from "../src/constants.js";
import {
  encodeErrorDetails,
  errorInfo,
  isRetryable,
  MAX_ERROR_DETAILS_BYTES,
  parseErrorDetail,
  retryInfo,
  StatusError,
} from "../src/error-model.js";
import { MethodNotImplementedError, ProtocolVersionError, type RpcError, ServerDrainingError } from "../src/errors.js";
import { createHttpHandler } from "../src/http/index.js";
import { AuthUnavailableError } from "../src/http/unauthorized.js";
import { serveTcp } from "../src/launcher/serve-tcp.js";
import { dispatchLogOrError } from "../src/log-batch.js";
import { Protocol } from "../src/protocol.js";
import { str } from "../src/schema.js";
import { serveStream } from "../src/serve-stream.js";
import { VgiRpcServer } from "../src/server.js";
import { IdentityUnavailableError } from "../src/token-identity.js";
import { buildErrorBatch } from "../src/wire/response.js";

const EMPTY = new Schema([]);

function decode(error: Error, includeTraceback = true): RpcError {
  const batch = buildErrorBatch(EMPTY, error, "srv", "req-1", includeTraceback);
  try {
    dispatchLogOrError(batch as never);
  } catch (e) {
    return e as RpcError;
  }
  throw new Error("no error raised");
}

function extraOf(error: Error, includeTraceback = true): Record<string, unknown> {
  const batch = buildErrorBatch(EMPTY, error, "srv", null, includeTraceback);
  return JSON.parse(batch.metadata.get(LOG_EXTRA_KEY)!);
}

/** `[{"@type":"vgi_rpc.ErrorInfo","metadata":{"p":"<n x>"}}]` is 51 + n bytes compactly. */
function paddedErrorInfo(n: number): StatusError {
  return new StatusError("pad", { code: "INTERNAL", details: [errorInfo({ p: "x".repeat(n) })] });
}

describe("emission", () => {
  test("every EXCEPTION batch carries a code; unclassified is UNKNOWN", () => {
    const batch = buildErrorBatch(EMPTY, new Error("boom"), "srv", null, false);
    expect(batch.metadata.get(ERROR_CODE_KEY)).toBe("UNKNOWN");
    expect(batch.metadata.has(ERROR_KIND_KEY)).toBe(false);
    expect(batch.metadata.has(ERROR_DETAILS_KEY)).toBe(false);
  });

  test("framework kinds carry the code the table assigns them", () => {
    expect(decode(new MethodNotImplementedError("x")).errorCode).toBe("UNIMPLEMENTED");
    expect(decode(new IdentityUnavailableError("down", 5)).errorCode).toBe("UNAVAILABLE");
    const draining = decode(new ServerDrainingError("draining"));
    expect([draining.errorCode, draining.errorKind]).toEqual(["UNAVAILABLE", "server_draining"]);
    expect(draining.retryInfo()?.retry_delay_seconds).toBe(1);
    const version = decode(
      new ProtocolVersionError("old", { protocol: "app.v1", clientVersion: "1.0.0", serverVersion: "2.0.0" }),
    );
    expect(version.errorCode).toBe("FAILED_PRECONDITION");
    expect(version.preconditionFailure()?.violations[0]).toMatchObject({
      type: "protocol_version",
      subject: "app.v1",
    });
  });

  test("code, kind and details ride top-level and are mirrored in log_extra", () => {
    const error = new StatusError("down", {
      code: "UNAVAILABLE",
      kind: "backend_down",
      details: expectedFailDetails(7),
    });
    const batch = buildErrorBatch(EMPTY, error, "srv", null, false);
    expect(batch.metadata.get(ERROR_CODE_KEY)).toBe("UNAVAILABLE");
    expect(batch.metadata.get(ERROR_KIND_KEY)).toBe("backend_down");
    const top = JSON.parse(batch.metadata.get(ERROR_DETAILS_KEY)!);
    expect(top).toEqual(expectedFailDetails(7));
    const extra = JSON.parse(batch.metadata.get(LOG_EXTRA_KEY)!);
    expect(extra.error_code).toBe("UNAVAILABLE");
    expect(extra.error_kind).toBe("backend_down");
    expect(extra.error_details).toEqual(top);
  });

  test("the 4 KiB cap is measured in bytes as emitted, and the boundary is exact (V5)", () => {
    expect(encodeErrorDetails([errorInfo({ p: "x".repeat(4045) })])?.length).toBe(MAX_ERROR_DETAILS_BYTES);
    const at = buildErrorBatch(EMPTY, paddedErrorInfo(4045), "srv", null, false);
    expect(at.metadata.has(ERROR_DETAILS_KEY)).toBe(true);
    const over = buildErrorBatch(EMPTY, paddedErrorInfo(4046), "srv", null, false);
    expect(over.metadata.has(ERROR_DETAILS_KEY)).toBe(false);
    expect(over.metadata.get(ERROR_CODE_KEY)).toBe("INTERNAL");
    const accented = new StatusError("x", {
      code: "INTERNAL",
      details: [{ "@type": "vgi_rpc.LocalizedMessage", locale: "fr", message: "é".repeat(2048) }],
    });
    expect(buildErrorBatch(EMPTY, accented, "srv", null, false).metadata.has(ERROR_DETAILS_KEY)).toBe(false);
  });

  test("over the cap the WHOLE array is dropped, mirror included -- never the part that fits", () => {
    const oversized = new StatusError("x", {
      code: "RESOURCE_EXHAUSTED",
      kind: "details_oversized",
      details: [retryInfo(1), errorInfo({ padding: "x".repeat(5000) })],
    });
    const extra = extraOf(oversized, false);
    expect(extra.error_details).toBeUndefined();
    const err = decode(oversized);
    expect(err.errorDetails).toEqual([]);
    expect(err.isRetryable()).toBe(false);
  });

  test("rule-breaking arrays are dropped at emission (V6)", () => {
    const fake = (details: Record<string, unknown>[]) => {
      const e = new Error("x") as Error & { errorDetails: unknown };
      e.errorDetails = details;
      return buildErrorBatch(EMPTY, e, "srv", null, false).metadata.has(ERROR_DETAILS_KEY);
    };
    expect(fake([retryInfo(1) as never, retryInfo(2) as never])).toBe(false);
    expect(fake([{ "@type": "vgi_rpc.Made.Up" }])).toBe(false);
    expect(fake([{ "@type": "Unqualified" }])).toBe(false);
    expect(fake([{ note: "no type" }])).toBe(false);
    expect(fake([{ "@type": "app.v1.Fine" }])).toBe(true);
  });

  test("the traceback follows the setting; nothing else does", () => {
    const withTb = extraOf(new Error("boom"), true);
    const without = extraOf(new Error("boom"), false);
    expect(typeof withTb.traceback).toBe("string");
    expect(without.traceback).toBeUndefined();
    expect(without.error_code).toBe("UNKNOWN");
    expect(without.exception_message).toBe("boom");
  });

  test("StatusError refuses a non-canonical code and a rule-breaking list eagerly", () => {
    expect(() => new StatusError("x", { code: "OK" as never })).toThrow(TypeError);
    expect(() => new StatusError("x", { code: "INTERNAL", details: [retryInfo(1), retryInfo(2)] })).toThrow(TypeError);
  });
});

describe("client decode", () => {
  test("RpcError exposes code, kind, details, typed accessors and retryability", () => {
    const err = decode(
      new StatusError("down", { code: "UNAVAILABLE", kind: "backend_down", details: expectedFailDetails(7) }),
    );
    expect(err.errorCode).toBe("UNAVAILABLE");
    expect(err.errorKind).toBe("backend_down");
    expect(err.errorDetails).toEqual(expectedFailDetails(7));
    expect(err.retryInfo()?.retry_delay_seconds).toBe(7);
    expect(err.errorInfo()?.metadata).toEqual({ fixture: SECONDARY_PROTOCOL_NAME });
    // The probe type is kept in the raw array and skipped by typed access.
    expect(err.details().map((d) => d["@type"])).toEqual(["vgi_rpc.ErrorInfo", "vgi_rpc.RetryInfo"]);
    expect(err.isRetryable()).toBe(true);
    expect(err.requestId).toBe("req-1");
  });

  test("an absent kind reads as empty; a code-less (pre-model) batch reads code as empty", () => {
    const err = decode(new StatusError("x", { code: "ABORTED" }));
    expect([err.errorCode, err.errorKind]).toEqual(["ABORTED", ""]);
    const legacy = buildErrorBatch(EMPTY, new Error("x"), "srv", null, false);
    const meta = new Map(legacy.metadata);
    meta.delete(ERROR_CODE_KEY);
    meta.set(LOG_EXTRA_KEY, JSON.stringify({ exception_type: "Error", exception_message: "x" }));
    try {
      dispatchLogOrError({ metadata: meta } as never);
    } catch (e) {
      expect((e as RpcError).errorCode).toBe("");
      expect((e as RpcError).code).toBe("UNKNOWN");
    }
  });

  test("the log_extra mirror is the fallback when the top-level keys are absent", () => {
    const meta = new Map<string, string>([
      ["vgi_rpc.log_level", "EXCEPTION"],
      ["vgi_rpc.log_message", "x"],
      [
        LOG_EXTRA_KEY,
        JSON.stringify({
          exception_type: "E",
          exception_message: "x",
          error_code: "NOT_FOUND",
          error_kind: "gone",
          error_details: [retryInfo(3)],
        }),
      ],
    ]);
    let err: RpcError | undefined;
    try {
      dispatchLogOrError({ metadata: meta } as never);
    } catch (e) {
      err = e as RpcError;
    }
    expect([err?.errorCode, err?.errorKind]).toEqual(["NOT_FOUND", "gone"]);
    expect(err?.retryInfo()?.retry_delay_seconds).toBe(3);
  });

  test("malformed details never turn the error into a decode failure (V7)", () => {
    const meta = new Map<string, string>([
      ["vgi_rpc.log_level", "EXCEPTION"],
      ["vgi_rpc.log_message", "x"],
      [ERROR_CODE_KEY, "UNAVAILABLE"],
      [
        ERROR_DETAILS_KEY,
        JSON.stringify([7, { "@type": "vgi_rpc.RetryInfo", retry_delay_seconds: "soon" }, { "@type": "x.y.Z" }]),
      ],
    ]);
    let err: RpcError | undefined;
    try {
      dispatchLogOrError({ metadata: meta } as never);
    } catch (e) {
      err = e as RpcError;
    }
    expect(err?.errorDetails.length).toBe(2);
    expect(err?.retryInfo()).toBeNull();
    expect(parseErrorDetail({ "@type": "vgi_rpc.RetryInfo", retry_delay_seconds: -1 })).toBeNull();
    expect(parseErrorDetail({ "@type": "vgi_rpc.ErrorInfo", metadata: { a: 1 } })).toBeNull();
    meta.set(ERROR_DETAILS_KEY, "{not an array");
    try {
      dispatchLogOrError({ metadata: meta } as never);
    } catch (e) {
      expect((e as RpcError).errorDetails).toEqual([]);
    }
  });

  test("retryability follows the code (V3)", () => {
    expect(isRetryable("UNAVAILABLE")).toBe(true);
    expect(isRetryable("RESOURCE_EXHAUSTED", [retryInfo(1)])).toBe(true);
    expect(isRetryable("RESOURCE_EXHAUSTED")).toBe(false);
    expect(isRetryable("ABORTED", [retryInfo(1)])).toBe(false);
    expect(isRetryable("INTERNAL", [retryInfo(1)])).toBe(false);
    expect(isRetryable("")).toBe(false);
    expect(isRetryable("SOMETHING")).toBe(false);
  });
});

describe("identity translation (WIRE_PROTOCOL.md §16)", () => {
  const caller = new AuthContext("conformance", true, "conformance-introspector", {});
  const minter = new AuthContext("conformance", true, "someone", { auth_time: String(Date.now() / 1000) });

  test("AuthUnavailableError from resolve_token becomes identity_unavailable with ITS retry hint", async () => {
    const identity = conformanceIdentity("both");
    let raised: unknown;
    try {
      await identity.introspectToken(TOKEN_AUTH_UNAVAILABLE, caller);
    } catch (e) {
      raised = e;
    }
    expect(raised).toBeInstanceOf(IdentityUnavailableError);
    const err = decode(raised as Error);
    expect([err.errorCode, err.errorKind]).toEqual(["UNAVAILABLE", "identity_unavailable"]);
    expect(err.retryInfo()?.retry_delay_seconds).toBe(7);
  });

  test("...and from mint_grant too", async () => {
    const identity = conformanceIdentity("both");
    let raised: unknown;
    try {
      await identity.issueGrant(AUTH_UNAVAILABLE_PURPOSE, [], 60, minter);
    } catch (e) {
      raised = e;
    }
    expect(raised).toBeInstanceOf(IdentityUnavailableError);
    expect((raised as IdentityUnavailableError).retryAfter).toBe(7);
  });

  test("an untranslated AuthUnavailableError would still read UNAVAILABLE -- only the kind tells", () => {
    const err = decode(new AuthUnavailableError("down", 7));
    expect([err.errorCode, err.errorKind]).toEqual(["UNAVAILABLE", ""]);
  });
});

describe("hosting several application protocols (§3.1)", () => {
  const primary = () =>
    new Protocol("app.Primary.v1", { protocolVersion: "2.0.0" }).unary("echo_string", {
      params: { value: str },
      result: { result: str },
      handler: (p) => ({ result: String(p.value) }),
    });

  test("registration order, primary first, framework protocols after", () => {
    const server = new VgiRpcServer(primary(), { protocols: [buildSecondaryProtocol()] });
    expect([...server.bindings().keys()]).toEqual(["app.Primary.v1", SECONDARY_PROTOCOL_NAME, "vgi_rpc.Reflection.v1"]);
  });

  test("list_protocols reports registration order, not name order", async () => {
    // "app.Zeta.v1" sorts after "aaa.Second.v1"; registration says primary first.
    const zeta = new Protocol("app.Zeta.v1").unary("noop", { params: {}, result: {}, handler: () => ({}) });
    const second = new Protocol("aaa.Second.v1").unary("noop", { params: {}, result: {}, handler: () => ({}) });
    const server = new VgiRpcServer(zeta, { protocols: [second] });
    const handle = await serveTcp(server, {
      idleTimeout: 0,
      announcementSink: { write: () => true } as unknown as NodeJS.WritableStream,
    });
    try {
      const client = tcpConnect(handle.host, handle.port);
      try {
        const description = await client.describe();
        expect(description.protocolName).toBe("app.Zeta.v1");
        expect(description.hostedProtocols?.filter((p) => !p.startsWith("vgi_rpc."))).toEqual([
          "app.Zeta.v1",
          "aaa.Second.v1",
        ]);
      } finally {
        client.close();
      }
    } finally {
      await handle.stop();
    }
  });

  test("the reserved prefix is refused however the name was derived", () => {
    const reserved = new Protocol("vgi_rpc.Sneaky.v1");
    expect(() => new VgiRpcServer(primary(), { protocols: [reserved] })).toThrow(/reserved/);
    expect(() => new VgiRpcServer(reserved)).toThrow(/reserved/);
    const server = new VgiRpcServer(primary());
    // A clean routing key over a reserved protocol name, and the reverse.
    expect(() => server.addProtocol({ name: "app.Ok.v1", protocol: reserved, versionExempt: false })).toThrow(
      /reserved/,
    );
    expect(() =>
      server.addProtocol({ name: "vgi_rpc.Reflection.v2", protocol: new Protocol("app.X.v1"), versionExempt: true }),
    ).toThrow(/reserved/);
  });

  test("duplicate names are a construction error", () => {
    expect(
      () => new VgiRpcServer(primary(), { protocols: [buildSecondaryProtocol(), buildSecondaryProtocol()] }),
    ).toThrow(/same name/);
  });

  test("the hosted set is fixed once serving starts", async () => {
    const server = new VgiRpcServer(primary());
    // An empty stream ends the connection at once; serving started regardless.
    await server.serveConnection(new ReadableStream({ start: (c) => c.close() }), { write() {} }).catch(() => {});
    expect(() => server.addProtocol(buildSecondaryProtocol())).toThrow(/started serving/);
  });

  test("a launcher refuses server-level options alongside a built host", async () => {
    const server = new VgiRpcServer(primary());
    await expect(serveTcp(server, { serverId: "x", idleTimeout: 0 })).rejects.toThrow(/VgiRpcServer constructor/);
    await expect(
      serveStream(server, { readable: new ReadableStream(), serverOptions: { serverId: "x" } }),
    ).rejects.toThrow(/VgiRpcServer constructor/);
  });

  test("serveTcp serves every protocol of a host, gated per binding", async () => {
    const server = new VgiRpcServer(primary(), { protocols: [buildSecondaryProtocol()] });
    const handle = await serveTcp(server, {
      idleTimeout: 0,
      announcementSink: { write: () => true } as unknown as NodeJS.WritableStream,
    });
    try {
      // The secondary declares no version, so its call carries none; a gate
      // against the primary's 2.0.0 would refuse it.
      const secondary = tcpConnect(handle.host, handle.port, { protocol: SECONDARY_PROTOCOL_NAME });
      try {
        expect(await secondary.call("echo_string", { value: "ping" })).toEqual({ result: "secondary:ping" });
        let err: RpcError | undefined;
        try {
          await secondary.call("fail", { code: "UNAVAILABLE", kind: "backend_down", retry_delay_seconds: 7 });
        } catch (e) {
          err = e as RpcError;
        }
        expect([err?.errorCode, err?.errorKind]).toEqual(["UNAVAILABLE", "backend_down"]);
        // Included by default on every transport, TCP too.
        expect(err?.remoteTraceback).toContain("StatusError");
      } finally {
        secondary.close();
      }
    } finally {
      await handle.stop();
    }
  });

  test("tracebacks are on by default; one switch turns them off on every transport", async () => {
    expect(new VgiRpcServer(primary()).includeTracebacks).toBe(true);
    const off = new VgiRpcServer(primary(), { protocols: [buildSecondaryProtocol()], includeTracebacks: false });
    expect(off.includeTracebacks).toBe(false);
    const failOver = async (call: (c: RpcClient) => Promise<unknown>, client: RpcClient): Promise<RpcError> => {
      try {
        await call(client);
      } catch (e) {
        return e as RpcError;
      } finally {
        client.close();
      }
      throw new Error("no error");
    };
    const fail = (c: RpcClient) => c.call("fail", { code: "INTERNAL", kind: "", retry_delay_seconds: 0 });
    // TCP
    const handle = await serveTcp(off, {
      idleTimeout: 0,
      announcementSink: { write: () => true } as unknown as NodeJS.WritableStream,
    });
    try {
      const err = await failOver(fail, tcpConnect(handle.host, handle.port, { protocol: SECONDARY_PROTOCOL_NAME }));
      expect([err.errorCode, err.remoteTraceback]).toEqual(["INTERNAL", ""]);
    } finally {
      await handle.stop();
    }
    // HTTP, same server object: the switch reaches the handler too.
    const handler = createHttpHandler(off, { serverId: "off" });
    const viaHttp = await failOver(
      fail,
      httpConnect("http://test", {
        protocol: SECONDARY_PROTOCOL_NAME,
        fetch: (async (input: RequestInfo | URL, init?: RequestInit) =>
          handler(new Request(input, init))) as typeof globalThis.fetch,
      }),
    );
    expect([viaHttp.errorCode, viaHttp.remoteTraceback]).toEqual(["INTERNAL", ""]);
    // And on: an error with no stack still sends a non-empty trace.
    const stackless = new Error("x");
    stackless.stack = undefined;
    expect(extraOf(stackless, true).traceback).toBe("Error: x");
  });
});
