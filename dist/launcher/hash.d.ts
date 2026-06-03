/** Recursively stringify with sorted object keys and `,`/`:` separators —
 *  the JS equivalent of `json.dumps(..., sort_keys=True, separators=(",",":"))`.
 *  We can't reuse `JSON.stringify` because the V8 implementation preserves
 *  insertion order rather than sorting. */
declare function canonicalJson(value: unknown): string;
/**
 * Compute the 16-hex-char tuple hash for a worker.
 *
 * @param workerArgv The worker command and its arguments.
 * @param cwd        Working directory; defaults to `process.cwd()`.
 * @param env        Process environment; defaults to `process.env`.  Only
 *                   keys starting with `VGI_RPC_` participate in the hash —
 *                   workers that differ only in unrelated env (PATH,
 *                   HOME, …) intentionally share a worker.
 */
export declare function computeHash(workerArgv: readonly string[], cwd?: string, env?: Record<string, string | undefined>): Promise<string>;
/** Exposed for parity-test fixtures that need to inspect the canonical form. */
export declare const _internal: {
    canonicalJson: typeof canonicalJson;
};
export {};
//# sourceMappingURL=hash.d.ts.map