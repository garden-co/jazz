import type { MutatorResult } from "@rocicorp/zero";

/** The server refused a write (a failed check, or a constraint). */
export class WriteRejectedError extends Error {
  constructor(
    message: string,
    readonly code: "permission_denied" | "rejected",
  ) {
    super(message);
  }
}

/** Whether the server refused a write because the role doesn't allow it. */
export function isPermissionDenied(error: unknown) {
  return error instanceof WriteRejectedError && error.code === "permission_denied";
}

/** Resolves once the server has applied the mutation; rejects if it refused it. */
export async function confirmed(write: MutatorResult): Promise<void> {
  const result = await write.server;
  if (result.type === "error") {
    const message = result.error.message;
    throw new WriteRejectedError(message, message.startsWith("permission_denied") ? "permission_denied" : "rejected");
  }
}
