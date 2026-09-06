/** @internal Synchronization failed before any context resources were closed. */
export class GracefulShutdownSyncError extends Error {
  constructor(cause: unknown) {
    super("Graceful shutdown could not synchronize pending writes; the context remains open", {
      cause,
    });
    this.name = "GracefulShutdownSyncError";
  }
}
