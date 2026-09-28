import { Data } from "effect";
import { PersistedWriteRejectedError } from "../runtime/client.js";

/**
 * A Jazz operation failed before it produced a result: an invalid query or
 * write, a closed database, a storage or transport failure, or a terminal
 * subscription error. `cause` holds the error thrown by the core runtime.
 */
export class JazzError extends Data.TaggedError("JazzError")<{
  /** The Jazz operation that failed, such as `"all"` or `"insert"`. */
  readonly operation: string;
  readonly cause: unknown;
}> {
  override get message(): string {
    return `Jazz ${this.operation} failed: ${describeCause(this.cause)}`;
  }
}

/** Describe a thrown value without assuming it is an Error or has a prototype. */
function describeCause(cause: unknown): string {
  if (cause instanceof Error) return cause.message;
  if (typeof cause === "object" && cause !== null) {
    const message = (cause as { message?: unknown }).message;
    if (typeof message === "string") return message;
    try {
      return JSON.stringify(cause);
    } catch {
      return Object.prototype.toString.call(cause);
    }
  }
  return String(cause);
}

/**
 * A write was applied locally but rejected when it reached the requested
 * durability tier (for example by permissions or exclusive-transaction
 * validation at the authority). The local write has already been undone.
 */
export class JazzWriteRejected extends Data.TaggedError("JazzWriteRejected")<{
  readonly transactionId: string;
  readonly code: string;
  readonly reason: string;
}> {
  override get message(): string {
    return `Jazz write ${this.transactionId} was rejected (${this.code}): ${this.reason}`;
  }
}

/** @internal */
export const toJazzError =
  (operation: string) =>
  (cause: unknown): JazzError =>
    new JazzError({ operation, cause });

/** @internal */
export const toWriteError =
  (operation: string) =>
  (cause: unknown): JazzError | JazzWriteRejected =>
    cause instanceof PersistedWriteRejectedError
      ? new JazzWriteRejected({
          transactionId: String(cause.transactionId),
          code: cause.code,
          reason: cause.reason,
        })
      : new JazzError({ operation, cause });
