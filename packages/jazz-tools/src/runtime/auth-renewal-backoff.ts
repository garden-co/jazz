const BASE_DELAY_MS = 1_000;
const MAX_DELAY_MS = 5 * 60_000;

/** Delay before retry `attempt` (from 0): 1s doubling, capped at 5 minutes. */
export function authRetryDelay(attempt: number): number {
  return Math.min(MAX_DELAY_MS, BASE_DELAY_MS * 2 ** Math.min(attempt, 20));
}

/**
 * Spaces out consecutive auth renewals that did not take, such as tokens a
 * server with a skewed clock reports expired on arrival. The first renewal
 * runs at once; the streak ends only when a token lives to its scheduled
 * refresh, not when minting succeeds.
 */
export class AuthRenewalBackoff {
  private streak = 0;

  next(): number {
    const streak = this.streak++;
    return streak === 0 ? 0 : authRetryDelay(streak - 1);
  }

  reset(): void {
    this.streak = 0;
  }
}
