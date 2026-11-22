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

/**
 * @internal An operation reached a Db that takes no new work: it is draining
 * for a graceful shutdown, or it is closing or closed (see `DbPhase`).
 */
export class DbClosedError extends Error {
  constructor(readonly phase: "draining" | "closing" | "closed") {
    super(
      phase === "draining"
        ? "Cannot operate on a Db that is shutting down or closed: it is shutting down, waiting for its pending writes to sync."
        : `Cannot operate on a Db that is shutting down or closed: it is ${phase}.`,
    );
    this.name = "DbClosedError";
  }
}
