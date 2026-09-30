import { PersistedWriteRejectedError } from "jazz-tools";

/**
 * The authority rejected an exclusive transaction because another one touched
 * the same rows first. Running it again sees the winner's writes.
 */
export function isExclusiveConflict(error: unknown): boolean {
  return error instanceof PersistedWriteRejectedError && error.code === "exclusive_conflict";
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
