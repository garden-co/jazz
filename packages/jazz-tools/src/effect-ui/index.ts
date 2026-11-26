/**
 * Jazz for Effect UI (`@effect/ui`). Experimental: `@effect/ui` has no npm
 * release yet, and this module follows it.
 *
 * Nothing here is a new reactive primitive. Jazz fits the parts Effect UI
 * already has:
 *
 * - A live query is a keyed Subscription ({@link liveQuery}) over
 *   `Jazz.stream`: each result arrives as a message, and the subscription
 *   is removed when its key goes away.
 * - A write is a `Command.Service` whose body uses the {@link Jazz} service
 *   and matches `JazzError | JazzWriteRejected` to messages. The local write
 *   flows back through the live queries, so it needs no `invalidates` and
 *   no optimistic patch.
 * - Who is signed in comes from {@link JazzSession}: its state is a
 *   Subscription ({@link sessionState}), and {@link JazzSession.jazz}
 *   provides `Jazz` for the signed-in user to a component keyed on that user.
 */
export { liveQuery, type LiveQueryOptions } from "./live-query.js";
export {
  JazzSession,
  JazzSessionState,
  JazzNotSignedIn,
  sessionState,
  type JazzSessionService,
} from "./session.js";
