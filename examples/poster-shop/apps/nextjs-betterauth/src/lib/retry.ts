import { PersistedWriteRejectedError } from "jazz-tools";

/**
 * Rejection codes that mean "another exclusive transaction raced us; read
 * again and retry". These are the codes the Better Auth adapter retries.
 */
export const RETRYABLE_CONFLICT_CODES: ReadonlySet<string> = new Set([
  "cascade_rejected",
  "exclusive_conflict",
  "transaction_conflict",
]);

/**
 * A settled rejection (`PersistedWriteRejectedError`) with a conflict code.
 * On the native backend an exclusive conflict currently surfaces as the
 * binding's core error instead, an `Error` whose stable `code` property is
 * `transaction_conflict` (see jazz-tools native-error-code.ts), so that
 * documented shape is accepted as well. Error messages are never parsed.
 */
export function isRetryableConflict(error: unknown): boolean {
  if (error instanceof PersistedWriteRejectedError) return RETRYABLE_CONFLICT_CODES.has(error.code);
  const code = error instanceof Error ? (error as { code?: unknown }).code : undefined;
  return typeof code === "string" && RETRYABLE_CONFLICT_CODES.has(code);
}

/** A recoverable bootstrap failure: the client may simply try again later. */
export class BootstrapConflictError extends Error {
  readonly attempts: number;
  constructor(attempts: number, cause: unknown) {
    super(`Poster bootstrap kept conflicting after ${attempts} attempts; try again.`, { cause });
    this.name = "BootstrapConflictError";
    this.attempts = attempts;
  }
}

export type RetryOptions = {
  attempts?: number;
  baseDelayMs?: number;
  sleep?: (ms: number) => Promise<void>;
};

/**
 * Run `attempt` until it succeeds, retrying only conflict errors with
 * exponential backoff (#2615). Other errors propagate immediately; persistent
 * conflicts end in a `BootstrapConflictError` instead of looping forever.
 */
export async function withBoundedConflictRetry<T>(
  attempt: () => Promise<T>,
  { attempts = 5, baseDelayMs = 50, sleep = defaultSleep }: RetryOptions = {},
): Promise<T> {
  let lastError: unknown;
  for (let index = 0; index < attempts; index += 1) {
    try {
      return await attempt();
    } catch (error) {
      if (!isRetryableConflict(error)) throw error;
      lastError = error;
      if (index < attempts - 1) await sleep(baseDelayMs * 2 ** index);
    }
  }
  throw new BootstrapConflictError(attempts, lastError);
}

function defaultSleep(ms: number) {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}
