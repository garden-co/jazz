import { Data, Effect } from "effect";
import { authClient } from "./auth-client.js";

export class ClearCompletedFailed extends Data.TaggedError("ClearCompletedFailed")<{
  readonly reason: string;
}> {}

const fail = (reason: string) => new ClearCompletedFailed({ reason });

/** A short-lived Better Auth JWT, the same kind of token Jazz sync uses. */
const bearerToken = Effect.tryPromise({
  try: () => authClient.$fetch<{ token: string }>("/token", { method: "GET" }),
  catch: () => fail("Could not reach the server"),
}).pipe(
  Effect.flatMap((result) =>
    result.data ? Effect.succeed(result.data.token) : Effect.fail(fail("Not signed in")),
  ),
);

/**
 * Ask the server (server/jazz-api.ts) to delete the signed-in user's
 * completed todos. The server acts as this user, so it can only delete their
 * rows; the deletions then sync back into the live todo list.
 */
export const clearCompleted = Effect.gen(function* () {
  const token = yield* bearerToken;
  const response = yield* Effect.tryPromise({
    try: () =>
      fetch("/api/todos/clear-completed", {
        method: "POST",
        headers: { Authorization: `Bearer ${token}` },
      }),
    catch: () => fail("Could not reach the server"),
  });
  const body = yield* Effect.tryPromise({
    try: () => response.json() as Promise<{ cleared?: number; error?: string }>,
    catch: () => fail(`Unexpected response (${response.status})`),
  });
  if (!response.ok || body.cleared === undefined) {
    return yield* fail(body.error ?? `Request failed (${response.status})`);
  }
  return body.cleared;
});
