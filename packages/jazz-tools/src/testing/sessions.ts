import { trackJazzSessions } from "../session/session-tracking.js";

// Importing this module turns tracking on: every Jazz session opened from
// now on (`createJazzClient`, or one a `<JazzProvider config>` opens) is
// kept until it closes.
const open = trackJazzSessions();

/**
 * Close every Jazz session that is still open, for a test's `afterEach`:
 *
 * ```ts
 * import { afterEach } from "vitest";
 * import { closeJazzSessions } from "jazz-tools/testing/sessions";
 *
 * afterEach(closeJazzSessions);
 * ```
 *
 * Tracks sessions opened after this module was imported, so import it in a
 * setup file. Waits for every session to close; if any failed to, throws an
 * `AggregateError` with their errors once all have finished.
 */
export async function closeJazzSessions(): Promise<void> {
  const sessions = [...open];
  open.clear();
  const results = await Promise.allSettled(sessions.map((session) => session.close()));
  const errors = results.flatMap((result) =>
    result.status === "rejected" ? [result.reason as unknown] : [],
  );
  if (errors.length > 0)
    throw new AggregateError(
      errors,
      `closeJazzSessions: ${errors.length} session(s) failed to close`,
    );
}
