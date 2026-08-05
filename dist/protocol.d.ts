import { type SchemaLike } from "./schema.js";
import { type ExchangeFn, type ExchangeInit, type HeaderInit, type MethodDefinition, type OnCancelFn, type ProducerFn, type ProducerInit, type UnaryHandler } from "./types.js";
/**
 * Fluent builder for defining RPC methods.
 * Register unary, producer, and exchange methods, then pass to `VgiRpcServer`.
 */
export declare class Protocol {
    /** Service / protocol name, exposed to clients via introspection. */
    readonly name: string;
    /**
     * Application protocol surface version. When non-empty, the server enforces
     * exact major+minor match (patch ignored) against every request's
     * `vgi_rpc.protocol_version` metadata; clients bound to this Protocol emit
     * the value on every request. Format: canonical semver MAJOR.MINOR.PATCH.
     * Mirrors Python's `Protocol.protocol_version` ClassVar.
     */
    readonly protocolVersion: string;
    /** Parsed semver tuple; null when `protocolVersion` is unset. */
    readonly protocolVersionParts: readonly [number, number, number] | null;
    private _methods;
    constructor(name: string, options?: {
        protocolVersion?: string;
    });
    /**
     * Register a unary (request-response) method.
     * @param name - Method name exposed to clients
     * @param config.params - Parameter schema (SchemaLike)
     * @param config.result - Result schema (SchemaLike)
     * @param config.handler - Async function receiving params and returning result values
     * @param config.doc - Optional documentation string
     * @param config.defaults - Optional default parameter values
     * @param config.paramTypes - Optional parameter type hints (inferred from params if omitted)
     */
    unary(name: string, config: {
        params: SchemaLike;
        result: SchemaLike;
        handler: UnaryHandler;
        doc?: string;
        defaults?: Record<string, any>;
        paramTypes?: Record<string, string>;
    }): this;
    /**
     * Register a producer (server-streaming) method.
     * The generic `S` is inferred from the `init` return type and threaded to `produce`.
     */
    producer<S>(name: string, config: {
        params: SchemaLike;
        outputSchema: SchemaLike;
        init: ProducerInit<S>;
        produce: ProducerFn<S>;
        onCancel?: OnCancelFn<S>;
        headerSchema?: SchemaLike;
        headerInit?: HeaderInit;
        doc?: string;
        defaults?: Record<string, any>;
        paramTypes?: Record<string, string>;
    }): this;
    /**
     * Register an exchange (bidirectional-streaming) method.
     * The generic `S` is inferred from the `init` return type and threaded to `exchange`.
     */
    exchange<S>(name: string, config: {
        params: SchemaLike;
        inputSchema: SchemaLike;
        outputSchema: SchemaLike;
        init: ExchangeInit<S>;
        exchange: ExchangeFn<S>;
        onCancel?: OnCancelFn<S>;
        headerSchema?: SchemaLike;
        headerInit?: HeaderInit;
        doc?: string;
        defaults?: Record<string, any>;
        paramTypes?: Record<string, string>;
    }): this;
    /** Snapshot of the registered methods, keyed by method name. Returns a copy,
     *  so mutating it does not affect the protocol. */
    getMethods(): Map<string, MethodDefinition>;
    /** Look up a single method without copying the whole map — for the
     *  per-request dispatch path. Callers must not mutate the returned value. */
    getMethod(name: string): MethodDefinition | undefined;
    /** Registered method names, sorted — for diagnostics/error messages only. */
    methodNames(): string[];
}
//# sourceMappingURL=protocol.d.ts.map