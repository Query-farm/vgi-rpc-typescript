// The protocol hash: a fingerprint of a protocol's wire surface.
//
// A client and a worker agree on a protocol or they do not, and the hash is how
// either side says which one it has without shipping the whole description. For
// that to be worth anything the same protocol must hash the same in every port,
// which the previous definition could not promise: it hashed serialized Arrow
// IPC bytes, and each language's Arrow implementation may legitimately emit
// different bytes for the same logical schema. The docs said so, which made the
// field advisory -- comparable only against itself.
//
// So the preimage is canonical JSON of what Arrow *decodes to*:
//
//     sha256("vgi_rpc.protocol_hash.v1|" + canonicalJson(description))
//
// Profile: RFC 8785 (JCS), chosen for its published test vectors. The structure
// is deliberately restricted to objects, arrays, strings and booleans; every
// number is folded into a type token (`decimal128(38,9)`), so JCS's hardest
// rule -- number canonicalisation, and the likeliest place for six ports to
// diverge -- never applies. Keep it that way.
//
// Not in the preimage: server identity, docstrings, parameter defaults,
// language-specific type names, and the framework's own request/describe
// versions. Those vary across processes, builds and ports without changing
// what is on the wire.
//
// Also not in the preimage: whether a stream is an exchange. Not because no
// port can determine it -- every port can, for most methods -- but because
// *which* methods a port can classify depends on how that port's registration
// works. A port that decides producer-vs-exchange from the returned stream
// cannot state it ahead of the call; a registration whose output schema is
// computed at run time may not carry the shape. So two ports can disagree
// about a method while neither is wrong, and a field one port can state and
// another cannot is not a contract.
//
// It still reaches clients as `stream_kind` on the description, where
// "unknown" is a sayable answer. A hash has no such option -- which is the
// whole difference: a description may admit what it does not know, a
// fingerprint may not.

import type { VgiField } from "./arrow/types.js";
import { type FieldToken, schemaTokens } from "./type-tokens.js";

/** Domain separator. Moves only when the hash definition moves, never when a
 *  protocol changes -- that is what the hash itself is for. */
export const HASH_DOMAIN = "vgi_rpc.protocol_hash.v1|";

/** One method's input to {@link computeProtocolHash}.
 *
 *  Takes decoded fields rather than serialized IPC: the hash is over structure,
 *  and accepting bytes would invite a caller to pass whatever its encoder
 *  produced. */
export interface HashMethod {
  name: string;
  /** `"unary"` or `"stream"`. */
  methodType: string;
  hasReturn: boolean;
  hasHeader: boolean;
  paramsFields: readonly VgiField[];
  /** Omitted entirely when `hasReturn` is false. */
  resultFields?: readonly VgiField[];
  /** Omitted entirely when `hasHeader` is false. */
  headerFields?: readonly VgiField[];
}

interface MethodEntry {
  name: string;
  type: string;
  has_return: boolean;
  has_header: boolean;
  params: FieldToken[];
  // Absent and empty are different: a method returning nothing is not a method
  // returning an empty struct, and they must not hash alike.
  result?: FieldToken[];
  header?: FieldToken[];
}

/** Build the hash preimage for one protocol. */
export function protocolDescription(protocolName: string, methods: readonly HashMethod[]): unknown {
  // Sorted so two ports iterating differently-ordered maps still agree.
  const sorted = [...methods].sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  const entries: MethodEntry[] = sorted.map((m) => {
    const entry: MethodEntry = {
      name: m.name,
      type: m.methodType,
      has_return: m.hasReturn,
      has_header: m.hasHeader,
      params: schemaTokens(m.paramsFields),
    };
    if (m.hasReturn && m.resultFields) entry.result = schemaTokens(m.resultFields);
    if (m.hasHeader && m.headerFields) entry.header = schemaTokens(m.headerFields);
    return entry;
  });
  return { protocol: protocolName, methods: entries };
}

/** Serialize `value` as RFC 8785 canonical JSON.
 *
 *  Object keys are sorted by their UTF-16 code units, which is what
 *  `Array.prototype.sort` does by default for the ASCII key set used here.
 *  Output is UTF-8 with no insignificant whitespace.
 *
 *  Throws on a number: JCS's number rules are the hardest part of the spec to
 *  implement identically in six languages, and the preimage is designed so they
 *  never apply -- a number reaching here means a type parameter leaked out of
 *  its token. */
export function canonicalJson(value: unknown): string {
  if (value === null) return "null";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "string") return JSON.stringify(value);
  if (typeof value === "number" || typeof value === "bigint") {
    throw new TypeError(
      `The protocol-hash preimage carries no numbers, but found ${String(value)}. ` +
        `Fold it into a type token (e.g. 'decimal128(38,9)') instead.`,
    );
  }
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (typeof value === "object") {
    const obj = value as Record<string, unknown>;
    const keys = Object.keys(obj).sort();
    return `{${keys.map((k) => `${JSON.stringify(k)}:${canonicalJson(obj[k])}`).join(",")}}`;
  }
  throw new TypeError(`Unsupported value in the protocol-hash preimage: ${String(value)}`);
}

/** Return the exact preimage bytes {@link computeProtocolHash} digests.
 *
 *  Exposed because a hash mismatch between ports is otherwise one bit of
 *  information. With the preimage in hand a failing port diffs two JSON
 *  documents and sees which method, field or type token it spells
 *  differently. */
export function canonicalDescription(protocolName: string, methods: readonly HashMethod[]): string {
  return canonicalJson(protocolDescription(protocolName, methods));
}

/** Return the SHA-256 hex digest of a protocol's canonical description.
 *
 *  Identical in every port for the same protocol -- which is a property
 *  conformance can assert, and could not before. */
export async function computeProtocolHash(protocolName: string, methods: readonly HashMethod[]): Promise<string> {
  const preimage = new TextEncoder().encode(HASH_DOMAIN + canonicalDescription(protocolName, methods));
  const digest = await crypto.subtle.digest("SHA-256", preimage);
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}
