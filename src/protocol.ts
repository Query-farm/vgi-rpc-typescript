// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { Schema } from "apache-arrow";
import { type SchemaLike, toSchema, inferParamTypes } from "./schema.js";
import {
  MethodType,
  type MethodDefinition,
  type UnaryHandler,
  type HeaderInit,
  type ProducerInit,
  type ProducerFn,
  type ExchangeInit,
  type ExchangeFn,
} from "./types.js";

const EMPTY_SCHEMA = new Schema([]);

/**
 * Fluent builder for defining RPC methods.
 * Register unary, producer, and exchange methods, then pass to `VgiRpcServer`.
 */
export class Protocol {
  readonly name: string;
  private _methods: Map<string, MethodDefinition> = new Map();

  constructor(name: string) {
    this.name = name;
  }

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
  unary(
    name: string,
    config: {
      params: SchemaLike;
      result: SchemaLike;
      handler: UnaryHandler;
      doc?: string;
      defaults?: Record<string, any>;
      paramTypes?: Record<string, string>;
    },
  ): this {
    const params = toSchema(config.params);
    this._methods.set(name, {
      name,
      type: MethodType.UNARY,
      paramsSchema: params,
      resultSchema: toSchema(config.result),
      handler: config.handler,
      doc: config.doc,
      defaults: config.defaults,
      paramTypes: config.paramTypes ?? inferParamTypes(params),
    });
    return this;
  }

  /**
   * Register a producer (server-streaming) method.
   * The generic `S` is inferred from the `init` return type and threaded to `produce`.
   */
  producer<S>(
    name: string,
    config: {
      params: SchemaLike;
      outputSchema: SchemaLike;
      init: ProducerInit<S>;
      produce: ProducerFn<S>;
      headerSchema?: SchemaLike;
      headerInit?: HeaderInit;
      doc?: string;
      defaults?: Record<string, any>;
      paramTypes?: Record<string, string>;
    },
  ): this {
    const params = toSchema(config.params);
    this._methods.set(name, {
      name,
      type: MethodType.STREAM,
      paramsSchema: params,
      resultSchema: EMPTY_SCHEMA,
      outputSchema: toSchema(config.outputSchema),
      inputSchema: EMPTY_SCHEMA,
      producerInit: config.init as ProducerInit,
      producerFn: config.produce as ProducerFn,
      headerSchema: config.headerSchema ? toSchema(config.headerSchema) : undefined,
      headerInit: config.headerInit,
      doc: config.doc,
      defaults: config.defaults,
      paramTypes: config.paramTypes ?? inferParamTypes(params),
    });
    return this;
  }

  /**
   * Register an exchange (bidirectional-streaming) method.
   * The generic `S` is inferred from the `init` return type and threaded to `exchange`.
   */
  exchange<S>(
    name: string,
    config: {
      params: SchemaLike;
      inputSchema: SchemaLike;
      outputSchema: SchemaLike;
      init: ExchangeInit<S>;
      exchange: ExchangeFn<S>;
      headerSchema?: SchemaLike;
      headerInit?: HeaderInit;
      doc?: string;
      defaults?: Record<string, any>;
      paramTypes?: Record<string, string>;
    },
  ): this {
    const params = toSchema(config.params);
    this._methods.set(name, {
      name,
      type: MethodType.STREAM,
      paramsSchema: params,
      resultSchema: EMPTY_SCHEMA,
      inputSchema: toSchema(config.inputSchema),
      outputSchema: toSchema(config.outputSchema),
      exchangeInit: config.init as ExchangeInit,
      exchangeFn: config.exchange as ExchangeFn,
      headerSchema: config.headerSchema ? toSchema(config.headerSchema) : undefined,
      headerInit: config.headerInit,
      doc: config.doc,
      defaults: config.defaults,
      paramTypes: config.paramTypes ?? inferParamTypes(params),
    });
    return this;
  }

  getMethods(): Map<string, MethodDefinition> {
    return new Map(this._methods);
  }
}
