/**
 * Why a Jazz session refused or dropped an operation (`reason`):
 *
 * - `pending`: another operation is running (a sign-in, a link, a refresh
 *   while an action runs, or the reverse). Wait for it, then ask again.
 * - `signing-out`: the session is signing out (its status is
 *   `transitioning` for the sign-out, or `signing-out` while the signed-out
 *   user's last writes upload). Sign in once it is `signed-out`.
 * - `superseded`: a sign-out (or `close()`) started while this operation
 *   ran, and took over: the operation's outcome no longer counts.
 */
export class JazzSessionOperationError extends Error {
  override readonly name = "JazzSessionOperationError";
  constructor(readonly reason: "pending" | "signing-out" | "superseded") {
    super(
      reason === "pending"
        ? "A Jazz session operation is already pending"
        : reason === "signing-out"
          ? 'The Jazz session is signing out: wait for status "signed-out", then sign in'
          : "Jazz session operation was superseded",
    );
  }
}
