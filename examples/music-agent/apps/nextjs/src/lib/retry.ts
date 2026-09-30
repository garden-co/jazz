import { isExclusiveConflict } from "./write-errors";

/** How many times an exclusive transaction that lost a race is read again and retried. */
export const CONFLICT_ATTEMPTS = 5;

/**
 * Run an exclusive transaction, retrying only when a concurrent write won the
 * race. Each attempt re-reads, so the retry decides on fresh data. Retries are
 * bounded: after the last attempt the conflict is rethrown.
 */
export async function retryOnConflict<T>(
  attempt: () => Promise<T>,
  attempts = CONFLICT_ATTEMPTS,
): Promise<T> {
  for (let tries = 1; ; tries++) {
    try {
      return await attempt();
    } catch (error) {
      if (tries >= attempts || !isExclusiveConflict(error)) throw error;
    }
  }
}
