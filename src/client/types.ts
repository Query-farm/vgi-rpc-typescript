// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

export interface HttpConnectOptions {
  prefix?: string;
  onLog?: (msg: LogMessage) => void;
  compressionLevel?: number;
}

export interface LogMessage {
  level: string;
  message: string;
  extra?: Record<string, any>;
}

export interface StreamSession {
  readonly header: Record<string, any> | null;
  exchange(input: Record<string, any>[]): Promise<Record<string, any>[]>;
  [Symbol.asyncIterator](): AsyncIterableIterator<Record<string, any>[]>;
  close(): void;
}

export interface PipeConnectOptions {
  onLog?: (msg: LogMessage) => void;
}

export interface SubprocessConnectOptions extends PipeConnectOptions {
  cwd?: string;
  env?: Record<string, string>;
  stderr?: "inherit" | "pipe" | "ignore";
}
