import { PersistedWriteRejectedError } from "jazz-tools";

/**
 * The rejection code of a failed write, such as "permission_denied" or
 * "exclusive_conflict", or undefined for other errors.
 */
export function writeErrorCode(error: unknown): string | undefined {
  if (!(error instanceof Error)) return undefined;
  // Server routes load the native backend outside the Next bundle, so its
  // error class can be a different copy from this import: match the name too.
  if (error instanceof PersistedWriteRejectedError || error.name === "PersistedWriteRejectedError")
    return (error as PersistedWriteRejectedError).code;
  const { code } = error as { code?: unknown };
  if (typeof code === "string") return code;
  // Workaround for https://github.com/garden-co/jazz/issues/2713: with the
  // native runtime, a conflict can be thrown from `wait()` as a plain Error
  // "(code): reason" instead of a PersistedWriteRejectedError. Delete this
  // fallback when that lands (#3753).
  return /^\((\w+)\): /.exec(error.message)?.[1];
}

const retryableConflicts = new Set([
  "exclusive_conflict",
  "transaction_conflict",
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
