import { Subscription } from "@effect/ui";
import { Effect, Stream } from "effect";
import type { QueryBuilder, QueryOptions } from "../runtime/db.js";
import { Jazz } from "../effect/jazz.js";
import type { JazzError } from "../effect/errors.js";

export interface LiveQueryOptions<D extends Subscription.Deps, T, RowsMsg, ErrorMsg> {
  /** The query, from the Subscription's deps only (never the Model). */
  readonly query: (deps: D) => QueryBuilder<T>;
  /** Read options, such as a durability tier. */
  readonly options?: QueryOptions | undefined;
  /** The message for each complete result. */
  readonly onRows: (rows: ReadonlyArray<T>) => RowsMsg;
  /**
   * The message for a failed subscription. The Subscription ends after it;
   * change its key (or stop listing it and list it again) to retry.
   */
  readonly onError: (error: JazzError) => ErrorMsg;
}

/**
 * A Jazz live query as an Effect UI Subscription. It runs while a
 * component's `subscriptions` lists it and requires {@link Jazz}.
 *
 * The deps become part of the key, so a component listing
 * `liveQuery("tasks", props.showId, ...)` drops the old show's
 * subscription and starts the new one when `showId` changes. Every
 * emission is the complete current result: a slow component only ever
 * receives the newest one.
 *
 * @example
 * ```ts
 * subscriptions: ({ props }) => [
 *   liveQuery("tasks", props.showId, {
 *     query: (showId) => app.tasks.where({ showId }),
 *     onRows: (tasks) => Message.GotTasks({ tasks }),
 *     onError: (error) => Message.FailedTasks({ error }),
 *   }),
 * ]
 * ```
 */
export const liveQuery = <
  const D extends Subscription.Deps,
  T extends { id: string },
  RowsMsg,
  ErrorMsg,
>(
  name: string,
  deps: D,
  { query, options, onRows, onError }: LiveQueryOptions<D, T, RowsMsg, ErrorMsg>,
): Subscription.Subscription<RowsMsg | ErrorMsg, Jazz> =>
  Subscription.make(name, deps, (deps: D) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const jazz = yield* Jazz;
        return jazz.stream(query(deps), options);
      }),
    ).pipe(
      Stream.map((rows): RowsMsg | ErrorMsg => onRows(rows)),
      Stream.catch((error) => Stream.succeed<RowsMsg | ErrorMsg>(onError(error))),
    ),
  );
