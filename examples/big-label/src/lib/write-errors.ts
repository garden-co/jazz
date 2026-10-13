import { PersistedWriteRejectedError } from "jazz-tools";

/**
 * The rejection code of a write the server or the local runtime refused, such
 * as "permission_denied" or "transaction_conflict", or undefined for any other
 * error.
 */
export function writeErrorCode(error: unknown): string | undefined {
  // Server routes load the native backend outside the Next bundle, so its
  // error class can be a different copy from this import: match the name too.
  if (
    error instanceof PersistedWriteRejectedError ||
    (error instanceof Error && error.name === "PersistedWriteRejectedError")
  )
    return (error as PersistedWriteRejectedError).code;
  return undefined;
}

/**
 * Codes that mean another transaction won the race: read again and retry.
 * A conflict found locally is `transaction_conflict`; one found by the
 * authority is `exclusive_conflict`; a write whose earlier transaction lost is
 * `cascade_rejected`. Callers bound their retries, because a cascade can also
 * follow a rejection that won't clear.
 */
const retryableConflicts = new Set([
  "transaction_conflict",
  "exclusive_conflict",
  "cascade_rejected",
]);

/** Whether an exclusive transaction lost to a concurrent write and can be retried. */
export function isExclusiveConflict(error: unknown) {
  return retryableConflicts.has(writeErrorCode(error) ?? "");
}

/** Whether the server refused a write because the policies don't allow it. */
export function isPermissionDenied(error: unknown) {
  return writeErrorCode(error) === "permission_denied";
}
