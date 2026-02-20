/** Error thrown when the server encounters an RPC protocol error. */
export class RpcError extends Error {
  constructor(
    public readonly errorType: string,
    public readonly errorMessage: string,
    public readonly remoteTraceback: string,
  ) {
    super(`${errorType}: ${errorMessage}`);
    this.name = "RpcError";
  }
}

/** Error thrown when the client sends an unsupported request version. */
export class VersionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "VersionError";
  }
}
