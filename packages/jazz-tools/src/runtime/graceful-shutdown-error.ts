/** @internal Synchronization failed before any context resources were closed. */
export class GracefulShutdownSyncError extends Error {
  constructor(cause: unknown) {
    super("Graceful shutdown could not synchronize pending writes; the context remains open", {
      cause,
    });
    this.name = "GracefulShutdownSyncError";
  }
}

/** @internal Other holders prevented teardown from starting; the client remains open. */
export class SharedClientShutdownError extends Error {
  constructor() {
    super("Release other holders before gracefully shutting down a shared Jazz client");
    this.name = "SharedClientShutdownError";
  }
}
