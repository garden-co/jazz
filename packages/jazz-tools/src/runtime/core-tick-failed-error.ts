/** Stable `code` of a failed core tick on every platform and binding. */
export const CORE_TICK_FAILED_ERROR_CODE = "core_tick_failed";

/**
 * The local Jazz core failed a tick: applying what the server sent,
 * evaluating queries, or committing to the local store. Its native error is
 * the `cause`, with the core `code` it carried (for example `storage`).
 *
 * The failure belongs to the core, not to one connection, so a connected
 * client publishes it as a terminal transport error: remote subscriptions end
 * with it and waits for the server fail with it instead of hanging. A client
 * without a server connection gets it as an uncaught error.
 *
 * Client stores have no chunk store, so a client tick has no retryable
 * failure: every error a client tick returns is a core failure.
 *
 * Class identity does not survive native bindings or browser worker
 * messages, so detect it with {@link isCoreTickFailedError}, which checks
 * the stable `code`.
 */
export class CoreTickFailedError extends Error {
  override readonly name = "CoreTickFailedError";
  readonly code = CORE_TICK_FAILED_ERROR_CODE;

  constructor(cause: unknown) {
    super(`core tick failed: ${causeMessage(cause)}`, { cause });
  }
}

/** Whether `error` is a failed core tick, by its stable `code`. */
export function isCoreTickFailedError(
  error: unknown,
): error is Error & { readonly code: typeof CORE_TICK_FAILED_ERROR_CODE } {
  if (!error || typeof error !== "object") return false;
  return (error as { code?: unknown }).code === CORE_TICK_FAILED_ERROR_CODE;
}

function causeMessage(cause: unknown): string {
  if (cause instanceof Error) return cause.message;
  if (cause && typeof cause === "object") {
    const message = (cause as { message?: unknown }).message;
    if (typeof message === "string") return message;
  }
  return String(cause);
}
