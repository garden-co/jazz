import { PersistedWriteRejectedError } from "jazz-tools";

/**
 * The stable code of a rejected write, or undefined for any other error (an
 * app error, a timeout, a lost connection).
 *
 * Two kinds of error carry one. The authority's verdict on a committed
 * transaction is a `PersistedWriteRejectedError` with its fate `code`
 * (`permission_denied`, `exclusive_conflict`, ...). A core error raised while
 * staging a transaction crosses the native boundary as an `Error` whose `code`
 * is the core error code (`transaction_conflict`, `write_rejected`, ...).
 *
 * Both are matched by the `code` property, never the message text. The class
 * check alone isn't enough: the browser topology test runs this module against
 * one jazz-tools module instance and its Db against another.
 */
export function writeRejectionCode(error: unknown): string | undefined {
  if (error instanceof PersistedWriteRejectedError) return error.code;
  if (error instanceof Error) {
    const { code } = error as Error & { code?: unknown };
    if (typeof code === "string") return code;
  }
  return undefined;
}

const RETRYABLE = new Set(["exclusive_conflict", "transaction_conflict", "cascade_rejected"]);
const DEFINITIVE = new Set(["permission_denied", "write_rejected"]);

/** An exclusive transaction lost to a concurrent write; running it again may succeed. */
export function isRetryableConflict(error: unknown): boolean {
  const code = writeRejectionCode(error);
  return code !== undefined && RETRYABLE.has(code);
}

/** The authority refused the write outright; the same write will be refused again. */
export function isDefinitiveRejection(error: unknown): boolean {
  const code = writeRejectionCode(error);
  return code !== undefined && DEFINITIVE.has(code);
}

/** Re-run an exclusive write the authority rejected as a conflict. Every other error is final. */
export async function retryOnConflict<T>(
  attempt: () => Promise<T>,
  beforeRetry?: () => Promise<unknown>,
  maxAttempts = 8,
): Promise<T> {
  for (let tries = 1; ; tries++) {
    try {
      return await attempt();
    } catch (error) {
      if (!isRetryableConflict(error) || tries >= maxAttempts) throw error;
      await beforeRetry?.();
    }
  }
}
