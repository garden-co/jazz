import type { Write } from "./live";

/**
 * Surfaces a write's server receipt. Writes apply locally at once; this
 * waits for the server so a permission rejection is shown to the user
 * rather than silently rolled back.
 */
export type ReportWrite = (write: Promise<unknown>, subject: string) => Promise<void>;

/** The server refused a write; `code` is "permission_denied" for a failed check. */
export class WriteRejectedError extends Error {
  constructor(
    message: string,
    readonly code: "permission_denied" | "error",
  ) {
    super(message);
  }
}

/** Resolves once the server has applied the mutation; rejects if it refused it. */
export async function confirmed(write: Write): Promise<void> {
  const result = await write.server;
  if (result.type === "error") {
    const denied = result.error.message.startsWith("permission_denied");
    throw new WriteRejectedError(result.error.message, denied ? "permission_denied" : "error");
  }
}
