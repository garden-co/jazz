import { PersistedWriteRejectedError } from "jazz-tools";

/**
 * Why the server refused a write, or undefined for any other error.
 *
 * Only a rejection means the write will never sync. Other failures while
 * waiting, such as the database shutting down when the tab closes, leave the
 * write committed locally, and it may still sync later.
 */
export function writeRejectionReason(error: unknown): string | undefined {
  // Another copy of jazz-tools can raise it, so match the name too.
  if (
    error instanceof PersistedWriteRejectedError ||
    (error instanceof Error && error.name === "PersistedWriteRejectedError")
  )
    // The message embeds the transaction id; the reason is for people.
    return (error as PersistedWriteRejectedError).reason;
  return undefined;
}
