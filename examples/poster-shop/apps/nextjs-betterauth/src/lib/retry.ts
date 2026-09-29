/** Error codes that mean "another first-open raced us; read again and retry". */
export const RETRYABLE_BOOTSTRAP_CONFLICT =
  /exclusive_conflict|transaction_conflict|cascade_rejected/;

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
      const message = error instanceof Error ? error.message : String(error);
      if (!RETRYABLE_BOOTSTRAP_CONFLICT.test(message)) throw error;
      lastError = error;
      if (index < attempts - 1) await sleep(baseDelayMs * 2 ** index);
    }
  }
  throw new BootstrapConflictError(attempts, lastError);
}

function defaultSleep(ms: number) {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}
