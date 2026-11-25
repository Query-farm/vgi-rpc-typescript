// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Timeouts for storage HTTP: idle (stall) timeouts, never a whole-request
 * deadline.
 *
 * A storage transfer (a pre-signed PUT or GET to R2/S3/GCS, an inbound pointer
 * fetch, a client upload) may legitimately take minutes on a slow link. What
 * must fail is a transfer that has stopped moving. So every storage request
 * here runs under a {@link StallWatchdog}: it aborts the request only when no
 * bytes have moved for the stall timeout (default 30 s,
 * `VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS`), matching vgi-rpc-python, whose
 * httpx timeouts bound each connect, read and write rather than the request.
 *
 * What counts as progress:
 *
 * - **Before the response:** connecting and waiting for the response headers
 *   get one stall window each (the connect timeout equals the stall timeout,
 *   30 s, as in Python; `fetch` does not expose the connect phase separately).
 * - **Upload body:** on Bun and Node the body is sent as a fixed-length
 *   stream of 64 KiB chunks, and every chunk the transport pulls counts. A
 *   single `Uint8Array` body gives no progress signal at all, which is how a
 *   whole-body deadline creeps in (a 100 MB PUT at 2 MB/s needs 50 s). Where a
 *   runtime cannot stream a fixed-length body (Cloudflare Workers, browsers,
 *   Deno send a stream chunked, which pre-signed PUTs refuse), the body goes
 *   as bytes under one window of `stall + size / minRate`, with `minRate`
 *   {@link DEFAULT_MIN_UPLOAD_RATE_BYTES_PER_SECOND} (64 KiB/s) unless set.
 * - **Response body:** every chunk read counts.
 *
 * After the last chunk is pulled the transport may still hold several MB in
 * socket buffers. The window after the last pull adds the time to drain them
 * at the observed rate (see {@link progressBody}), so a slow upload is not
 * mistaken for a stall at its very end.
 *
 * @module
 */

/** Environment variable: seconds without any byte moving before a storage
 *  transfer is abandoned (and retried). Same name in every vgi-rpc SDK. */
export const ENV_STORAGE_STALL_TIMEOUT = "VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS";
/** Default stall timeout, ms (vgi-rpc-python: 30 s). */
export const DEFAULT_STORAGE_STALL_TIMEOUT_MS = 30_000;
/** Upload chunk size: the granularity of upload progress. */
export const STORAGE_UPLOAD_CHUNK_BYTES = 64 * 1024;
/** Floor rate for the single upload window on runtimes that cannot stream a
 *  fixed-length body: 64 KiB/s. */
export const DEFAULT_MIN_UPLOAD_RATE_BYTES_PER_SECOND = 64 * 1024;

type EnvLike = Readonly<Record<string, string | undefined>>;

function processEnv(): EnvLike {
  const proc = (globalThis as { process?: { env?: EnvLike } }).process;
  return proc?.env ?? {};
}

/**
 * The stall timeout from `VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS` (seconds, a
 * positive number; fractions allowed), in ms. Unset: 30 s.
 *
 * @throws Error if the variable is set but is not a positive finite number.
 */
export function storageStallTimeoutMsFromEnv(env?: EnvLike): number {
  const raw = ((env ?? processEnv())[ENV_STORAGE_STALL_TIMEOUT] ?? "").trim();
  if (!raw) return DEFAULT_STORAGE_STALL_TIMEOUT_MS;
  const seconds = Number(raw);
  if (!/^\+?(\d+\.?\d*|\.\d+)$/.test(raw) || !Number.isFinite(seconds) || seconds <= 0) {
    throw new Error(`${ENV_STORAGE_STALL_TIMEOUT} must be a positive number of seconds, got ${JSON.stringify(raw)}`);
  }
  return Math.max(1, Math.round(seconds * 1000));
}

/** Validate a stall timeout option (ms). */
export function checkStallTimeoutMs(value: number, name = "stallTimeoutMs"): number {
  if (!Number.isFinite(value) || value <= 0) throw new Error(`${name} must be a positive number of ms, got ${value}`);
  return value;
}

/** A storage transfer moved no bytes for the stall timeout. */
export class StorageStallError extends Error {
  constructor(
    /** The stall timeout that elapsed, ms. */
    readonly stallTimeoutMs: number,
  ) {
    super(`storage transfer stalled: no bytes moved for ${stallTimeoutMs} ms`);
    this.name = "StorageStallError";
  }
}

/**
 * Aborts `controller` once no progress has been reported for `stallMs`.
 *
 * One timer per watchdog: progress only records a timestamp, and the timer
 * re-arms itself for the remaining window when it fires early.
 */
export class StallWatchdog {
  private last: number;
  private window: number;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private done = false;
  /** True once the watchdog aborted the request. */
  stalled = false;

  constructor(
    /** Aborted, with a {@link StorageStallError}, when the transfer stalls. */
    readonly controller: AbortController,
    /** The stall timeout, ms. */
    readonly stallMs: number,
    firstWindowMs = stallMs,
  ) {
    this.last = Date.now();
    this.window = Math.max(stallMs, firstWindowMs);
    this.arm(this.window);
  }

  private arm(ms: number): void {
    this.timer = setTimeout(() => this.check(), ms);
    // Never keep a process alive just to watch a transfer.
    (this.timer as { unref?: () => void }).unref?.();
  }

  private check(): void {
    if (this.done) return;
    const idle = Date.now() - this.last;
    if (idle >= this.window) {
      this.stalled = true;
      this.done = true;
      this.controller.abort(new StorageStallError(this.stallMs));
      return;
    }
    this.arm(this.window - idle);
  }

  /** Bytes moved: restart the stall window. */
  progress(): void {
    this.last = Date.now();
    this.window = this.stallMs;
  }

  /** Bytes moved, and the next window is `ms` (at least the stall timeout). */
  progressWithWindow(ms: number): void {
    this.last = Date.now();
    this.window = Math.max(this.stallMs, ms);
  }

  /** The transfer finished (or failed): stop watching. */
  stop(): void {
    this.done = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
  }
}

/** Whether this runtime sends a `ReadableStream` request body with an explicit
 *  `Content-Length` as a fixed-length body (Bun, Node), rather than chunked. */
export function canStreamFixedLengthBody(): boolean {
  const g = globalThis as {
    Bun?: unknown;
    Deno?: unknown;
    navigator?: { userAgent?: string };
    process?: { versions?: { node?: string } };
    window?: unknown;
  };
  if (g.Bun !== undefined) return true;
  if (g.Deno !== undefined || g.window !== undefined) return false;
  if (g.navigator?.userAgent === "Cloudflare-Workers") return false;
  return typeof g.process?.versions?.node === "string";
}

/** How long the first burst of pulls (the transport filling its buffers) lasts. */
const BURST_MS = 250;

/**
 * `data` as a pull-driven stream of `chunk`-byte views; each pull reports progress.
 *
 * The transport pulls ahead of the wire by however much its socket buffers
 * hold (several MB on Linux). So when the last chunk is pulled, that much may
 * still be draining, and draining it plus the server's reply must not read as
 * a stall. The final window is therefore the stall timeout plus the time to
 * drain the first burst (the buffer fill) at the rate observed after it, or,
 * with no rate observed (the whole body fit in the buffers), at `minRate`;
 * never more than the whole body at `minRate`.
 */
export function progressBody(
  data: Uint8Array,
  watchdog: StallWatchdog,
  chunk = STORAGE_UPLOAD_CHUNK_BYTES,
  minRate = DEFAULT_MIN_UPLOAD_RATE_BYTES_PER_SECOND,
): ReadableStream<Uint8Array> {
  let offset = 0;
  let firstPull = 0;
  let burstBytes = 0;
  let burstEnd = 0;
  return new ReadableStream<Uint8Array>(
    {
      pull(controller) {
        const now = Date.now();
        if (firstPull === 0) firstPull = now;
        if (now - firstPull <= BURST_MS) {
          burstBytes = offset;
          burstEnd = now;
        }
        if (offset >= data.byteLength) {
          const steadyBytes = offset - burstBytes;
          const steadyMs = now - burstEnd;
          const rate =
            steadyBytes >= 4 * chunk && steadyMs > 0 ? Math.max(minRate, (steadyBytes / steadyMs) * 1000) : minRate;
          const drainMs = Math.min((burstBytes / rate) * 1000, (data.byteLength / minRate) * 1000);
          watchdog.progressWithWindow(watchdog.stallMs + Math.ceil(drainMs));
          controller.close();
          return;
        }
        watchdog.progress();
        const end = Math.min(offset + chunk, data.byteLength);
        controller.enqueue(data.subarray(offset, end));
        offset = end;
      },
    },
    { highWaterMark: 0 },
  );
}

/** Options for {@link storageFetch}. */
export interface StorageFetchOptions {
  /** HTTP method. */
  method: string;
  /** Request headers, sent exactly. */
  headers?: Record<string, string>;
  /** Request body (bytes), if any. */
  body?: Uint8Array | null;
  /** Idle timeout, ms. */
  stallTimeoutMs: number;
  /** The `fetch` to call (default the global one). */
  fetch?: typeof globalThis.fetch;
  /** Follow redirects (`follow`) or hand them back (`manual`, default). */
  redirect?: RequestRedirect;
  /** Stream a body as fixed-length chunks (default: when the runtime can). */
  streamBody?: boolean;
  /** Floor rate for a body that cannot be streamed, bytes/s (default 64 KiB/s). */
  minUploadRateBytesPerSecond?: number;
  /** Extra init fields (e.g. Bun's `decompress: false`). */
  extra?: Record<string, unknown>;
  /** A caller's cancellation, linked to the request. */
  signal?: AbortSignal;
}

/** An in-flight storage response; its body is still being watched. */
export interface StorageResponse {
  /** The response; its body is not read yet. */
  response: Response;
  /** Still watching: the body read restarts it per chunk. */
  watchdog: StallWatchdog;
  /** Aborts the request (and its body). */
  controller: AbortController;
}

/**
 * `fetch` under a stall watchdog. The watchdog keeps running after the
 * response arrives so the body read stays bounded too: read it with
 * {@link readStorageBody} (or cancel it) and the watchdog stops.
 *
 * @throws StorageStallError when the request stalled before the response.
 * @throws The `fetch` error for any other transport failure.
 */
export async function storageFetch(url: string, options: StorageFetchOptions): Promise<StorageResponse> {
  const controller = new AbortController();
  if (options.signal) {
    const outer = options.signal;
    if (outer.aborted) controller.abort(outer.reason);
    else outer.addEventListener("abort", () => controller.abort(outer.reason), { once: true });
  }
  const body = options.body ?? null;
  const stream = body !== null && body.byteLength > 0 && (options.streamBody ?? canStreamFixedLengthBody());
  let firstWindow = options.stallTimeoutMs;
  if (body !== null && !stream) {
    const rate = options.minUploadRateBytesPerSecond ?? DEFAULT_MIN_UPLOAD_RATE_BYTES_PER_SECOND;
    firstWindow += Math.ceil((body.byteLength / rate) * 1000);
  }
  const watchdog = new StallWatchdog(controller, options.stallTimeoutMs, firstWindow);
  const headers: Record<string, string> = { ...(options.headers ?? {}) };
  const init: Record<string, unknown> = {
    method: options.method,
    headers,
    redirect: options.redirect ?? "manual",
    signal: controller.signal,
    ...(options.extra ?? {}),
  };
  if (body !== null) {
    if (stream) {
      // A fixed-length stream: Bun and Node send exactly this Content-Length
      // (a pre-signed PUT signs it) instead of chunked encoding.
      if (!Object.keys(headers).some((h) => h.toLowerCase() === "content-length")) {
        headers["Content-Length"] = String(body.byteLength);
      }
      init.body = progressBody(
        body,
        watchdog,
        STORAGE_UPLOAD_CHUNK_BYTES,
        options.minUploadRateBytesPerSecond ?? DEFAULT_MIN_UPLOAD_RATE_BYTES_PER_SECOND,
      );
      init.duplex = "half";
    } else {
      init.body = body;
    }
  }
  try {
    const response = await (options.fetch ?? globalThis.fetch)(url, init as RequestInit);
    watchdog.progress();
    return { response, watchdog, controller };
  } catch (error) {
    watchdog.stop();
    if (watchdog.stalled) throw new StorageStallError(options.stallTimeoutMs);
    throw error;
  }
}

/** The body is larger than the caller's cap. */
export class StorageBodyTooLargeError extends Error {
  constructor(
    message: string,
    /** True when the declared Content-Length was over the cap. */
    readonly declared: boolean,
  ) {
    super(message);
    this.name = "StorageBodyTooLargeError";
  }
}

/**
 * Read a {@link storageFetch} response body, refusing more than `maxBytes`,
 * with every chunk restarting the stall window. Always stops the watchdog.
 *
 * @throws StorageBodyTooLargeError over the cap (declared or streamed).
 * @throws StorageStallError when the body stalled.
 */
export async function readStorageBody(r: StorageResponse, maxBytes: number): Promise<Uint8Array> {
  const { response, watchdog } = r;
  try {
    const declared = response.headers.get("Content-Length");
    if (declared != null) {
      const length = Number(declared);
      if (Number.isFinite(length) && length > maxBytes) {
        await response.body?.cancel().catch(() => {});
        throw new StorageBodyTooLargeError(`body is ${length} bytes, over the ${maxBytes}-byte cap`, true);
      }
    }
    if (!response.body) return new Uint8Array();
    const reader = response.body.getReader();
    const chunks: Uint8Array[] = [];
    let total = 0;
    try {
      for (;;) {
        let step: Awaited<ReturnType<typeof reader.read>>;
        try {
          step = await reader.read();
        } catch (error) {
          if (watchdog.stalled) throw new StorageStallError(watchdog.stallMs);
          throw error;
        }
        if (step.done) break;
        watchdog.progress();
        total += step.value.byteLength;
        if (total > maxBytes) throw new StorageBodyTooLargeError(`body exceeded the ${maxBytes}-byte cap`, false);
        chunks.push(step.value);
      }
    } finally {
      await reader.cancel().catch(() => {});
      reader.releaseLock();
    }
    if (chunks.length === 1) return chunks[0]!;
    const out = new Uint8Array(total);
    let offset = 0;
    for (const chunk of chunks) {
      out.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return out;
  } finally {
    watchdog.stop();
  }
}

/** Discard a {@link storageFetch} response body and stop its watchdog. */
export async function discardStorageBody(r: StorageResponse): Promise<void> {
  try {
    await r.response.body?.cancel().catch(() => {});
  } finally {
    r.watchdog.stop();
  }
}

/** Read a short (error) body as text, at most `maxBytes`, never throwing. */
export async function storageBodyText(r: StorageResponse, maxBytes = 64 * 1024): Promise<string> {
  try {
    return new TextDecoder().decode(await readStorageBody(r, maxBytes));
  } catch {
    return "";
  }
}
