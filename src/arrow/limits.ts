/**
 * The largest payload this port can encode, and the guard that says so.
 *
 * Both Arrow backends round a buffer up to an 8-byte boundary with
 * `(byteLength + 7) & ~7`. `&` is a *bitwise* operator, so JavaScript
 * truncates the operand to int32 first: at `2 ** 31` the result goes
 * negative. arrow-js hands that straight to `Message.bodyLength`, and the
 * peer rejects the whole message with "Invalid IPC message: negative
 * bodyLength" — a corrupt frame, not an error anyone can act on.
 *
 * The ceiling is therefore `2 GiB - 8`: 2 147 483 640 bytes round-trips,
 * 2 147 483 648 does not. It lives in the dependencies, not here, so this
 * module cannot raise it — only refuse to cross it.
 *
 * Refusing explicitly is what the vgi-rpc conformance suite asks for. Its
 * `large_payload.echo_binary_over_int32_max` test accepts either an intact
 * round-trip or a typed error that leaves the connection usable; what it
 * forbids is the silent pair — a truncated body, or a peer left waiting for
 * bytes that never come. Emitting a negative `bodyLength` is squarely in
 * the second category, so the guard converts it into the first.
 */

/** Largest byte length either Arrow backend can encode without overflowing int32. */
export const MAX_ENCODABLE_BYTES = 2_147_483_640; // 2 GiB - 8

/** An Arrow `Data`-like value: carries raw buffers rather than a JS payload. */
interface BufferBearing {
  buffers: Record<string, unknown>;
}

function isBufferBearing(value: unknown): value is BufferBearing {
  return typeof value === "object" && value !== null && typeof (value as BufferBearing).buffers === "object";
}

/**
 * Throw if `value` is too large for the Arrow encoders to represent.
 *
 * Cheap by construction: it inspects `byteLength`/`length` and never walks
 * the value, so putting it on the per-field path costs nothing measurable
 * for ordinary rows.
 *
 * @param value - The value about to become an Arrow buffer.
 * @param fieldName - Field name, so the error names the culprit.
 */
export function requireEncodable(value: unknown, fieldName: string): void {
  let bytes = -1;
  if (ArrayBuffer.isView(value)) {
    bytes = value.byteLength;
  } else if (value instanceof ArrayBuffer) {
    bytes = value.byteLength;
  } else if (isBufferBearing(value)) {
    // An Arrow `Data` handed straight back by an echo-style handler, never
    // decoded to a JS value. Measure the *largest single buffer* rather than
    // the container's total: alignment is applied per buffer, so that is the
    // quantity that overflows, and totalling would refuse a payload of
    // exactly the ceiling on account of its own validity/offset bytes.
    // Any buffer that reports a `byteLength` counts. Deliberately not
    // `ArrayBuffer.isView`: that tests an internal slot, so it would refuse to
    // measure a buffer some other Arrow build hands over, and silently pass a
    // payload nothing had checked.
    for (const buf of Object.values(value.buffers)) {
      const len = (buf as { byteLength?: unknown } | null)?.byteLength;
      if (typeof len === "number" && len > bytes) bytes = len;
    }
  } else if (typeof value === "string") {
    // UTF-8 is at least one byte per code unit, so a string longer than the
    // ceiling is over it whatever the encoding works out to. Checking the
    // length avoids encoding a two-gigabyte string just to measure it.
    bytes = value.length;
  }
  if (bytes > MAX_ENCODABLE_BYTES) {
    throw new RangeError(
      `${fieldName} is ${bytes} bytes; this TypeScript worker can encode at most ${MAX_ENCODABLE_BYTES} ` +
        `(2 GiB - 8). Both Arrow backends align buffers with (byteLength + 7) & ~7, and the bitwise & ` +
        `truncates to int32, so a larger value would be sent as a negative bodyLength. The wire and the ` +
        `protocol carry it fine — the limit is the JavaScript Arrow encoders', not vgi-rpc's.`,
    );
  }
}
