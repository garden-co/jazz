/**
 * A write to an encrypted space whose key this device does not hold yet
 * fails `key-pending` (or `space-pending`, while the space's first key is
 * being made). The device's own space is made ready, and a member's key is
 * waited for within a deadline (the first holder's delivery, or the pass
 * the runtime runs at natural moments); then the write is tried again. The
 * failed write stored nothing, so trying it again repeats nothing.
 */

/** How long a write waits for a delivery before it fails `key-pending`. */
export const KEY_WAIT_MS = 10_000;

const PENDING = /(?:key-pending|space-pending): space ([0-9a-f-]+)\/([0-9a-f-]+)/;

/** The space a `key-pending` or `space-pending` failure names. */
export function pendingSpace(error: unknown): { scopeId: string; identifier: string } | undefined {
  const message = error instanceof Error ? error.message : undefined;
  const found = message === undefined ? null : PENDING.exec(message);
  return found ? { scopeId: found[1]!, identifier: found[2]! } : undefined;
}

/**
 * Runs `operation`; if it fails for want of a key, makes the space ready
 * with `prepare` (true once the write can be tried again) and tries again,
 * until `waitMs` has passed.
 */
export function retryAfterKey<T>(
  operation: () => T,
  prepare: (space: { scopeId: string; identifier: string }) => Promise<boolean>,
  waitMs = KEY_WAIT_MS,
): T | Promise<T> {
  try {
    return operation();
  } catch (first) {
    const space = pendingSpace(first);
    if (!space) throw first;
    return (async () => {
      const deadline = Date.now() + waitMs;
      let error: unknown = first;
      do {
        // Making the space ready is bounded by the same window.
        const left = Math.max(0, deadline - Date.now());
        const ready = await prepareWithin(prepare(space), left);
        if (ready) {
          try {
            return operation();
          } catch (next) {
            if (!pendingSpace(next)) throw next;
            error = next;
          }
        }
        const delay = Math.min(200, deadline - Date.now());
        if (delay > 0) await new Promise((resolve) => setTimeout(resolve, delay));
      } while (Date.now() < deadline);
      throw error;
    })();
  }
}

/** A ready space, or `false` once the wait expires. Preparation failures propagate. */
function prepareWithin(work: Promise<boolean>, ms: number): Promise<boolean> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => resolve(false), ms);
    work.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}
