/**
 * The request's user has no Jazz account yet, so there is no one to act as.
 * Jazz never creates an account on the backend by itself: the user gets one
 * when they sign in from a client, when a local-first guest links to them
 * (`linkWithBackend`), or when your backend calls `jazz.createAccountFor`.
 *
 * `jazz-tools/backend` is often loaded outside an app's bundle, so a caller
 * can see this error from a different copy of the class: match it with
 * `isNoJazzAccountError`, which also compares the name, or check
 * `code === "no_account"`.
 */
export class NoJazzAccountError extends Error {
  readonly name = "NoJazzAccountError";
  readonly code = "no_account";

  constructor(
    /** The user, as your `authenticate` names them. */
    readonly userId: string,
    caller: string,
  ) {
    super(
      `${caller}: user ${JSON.stringify(userId)} has no Jazz account yet. They get one when they sign in from a client, or when your backend calls jazz.createAccountFor(userId).`,
    );
  }
}

/** Whether `error` is a `NoJazzAccountError`, from any copy of jazz-tools. */
export function isNoJazzAccountError(error: unknown): error is NoJazzAccountError {
  return (
    error instanceof NoJazzAccountError ||
    (error instanceof Error &&
      error.name === "NoJazzAccountError" &&
      (error as { code?: unknown }).code === "no_account")
  );
}
