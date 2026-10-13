import { PersistedWriteRejectedError } from "jazz-tools";

/**
 * Rejections that mean another transaction touched the same rows first, so
 * running this one again sees the winner's writes:
 * - `transaction_conflict`: the conflict was found locally, before the commit
 *   left this process. Two requests racing on the same server session (a
 *   double click, two tabs, React strict mode's double effect) end here.
 * - `exclusive_conflict`: the authority found the conflict.
 * - `cascade_rejected`: an earlier transaction this one built on lost.
 *   It can also follow a rejection that never clears, which is why the
 *   retries below are bounded.
 */
const retryableConflicts = new Set([
  "transaction_conflict",
  "exclusive_conflict",
  "cascade_rejected",
]);

/** Whether an exclusive transaction lost to a concurrent one and can run again. */
export function isExclusiveConflict(error: unknown): boolean {
  // The server routes load `jazz-tools/backend` outside the Next bundle, so its
  // rejection can be an instance of a different copy of this class: match the
  // name too.
  const rejected =
    error instanceof PersistedWriteRejectedError ||
    (error instanceof Error && error.name === "PersistedWriteRejectedError");
  return rejected && retryableConflicts.has((error as PersistedWriteRejectedError).code);
}

/** Run an exclusive transaction again after a conflict, a few times at most. */
export async function retryOnConflict<T>(attempt: () => Promise<T>, tries = 6): Promise<T> {
  for (let tried = 1; ; tried++) {
    try {
      return await attempt();
    } catch (error) {
      if (tried >= tries || !isExclusiveConflict(error)) throw error;
    }
  }
}
