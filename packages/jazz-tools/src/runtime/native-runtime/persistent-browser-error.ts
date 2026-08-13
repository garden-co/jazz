import type { PersistentBrowserWorkerError } from "./persistent-browser-protocol.js";

/**
 * Converts runtime failures into the stable worker transport payload.
 *
 * Native waits may carry a non-enumerable diagnostic `message` for direct
 * callers. Rejected-write transport is deliberately structural, so that
 * diagnostic must not cross the worker boundary.
 */
export function serializePersistentBrowserWorkerError(
  error: unknown,
): PersistentBrowserWorkerError {
  if (isRejectedWrite(error)) {
    const { kind, batchId, code, reason } = error;
    return { kind, batchId, code, reason };
  }
  if (error instanceof Error) {
    return { name: error.name, message: error.message, stack: error.stack };
  }
  return { message: String(error) };
}

function isRejectedWrite(
  error: unknown,
): error is Extract<PersistentBrowserWorkerError, { kind: "rejected" }> {
  if (!error || typeof error !== "object") return false;
  const candidate = error as Record<string, unknown>;
  return (
    candidate.kind === "rejected" &&
    typeof candidate.batchId === "string" &&
    typeof candidate.code === "string" &&
    typeof candidate.reason === "string"
  );
}
