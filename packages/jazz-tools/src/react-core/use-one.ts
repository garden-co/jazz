import type { QueryBuilder, QueryOptions } from "../shared/index.js";
import { limitQueryToOne } from "../runtime/limit-query.js";
import { useAllBase } from "./use-all.js";

// `useOne` is `useAll` narrowed to a single row: the query is executed with
// `limit 1` (the same wrapper `db.one` uses, so the two stay in lockstep), and
// the subscription tracks just that row — replacing it should a closer match
// appear or the current one be removed.

/**
 * Read the first matching row and subscribe to changes that affect it.
 *
 * The query is run with `limit 1`, mirroring {@link Db.one}. Loading and error
 * states are handled the React way: `undefined` means the query has not resolved
 * yet, `null` means it resolved with no matching row, and for error handling use
 * {@link useOneSuspense} with a Suspense + error boundary.
 *
 * @param query - the database query (e.g. `app.todos.where({ id })`)
 *
 * @returns the matching row, `null` if none matched, or `undefined` if the query
 * is not yet executed
 */
export function useOne<T extends { id: string }>(
  query?: QueryBuilder<T>,
  options?: QueryOptions,
): T | null | undefined {
  const rows = useAllBase(query ? limitQueryToOne(query) : undefined, options, {
    suspense: false,
  });
  return rows === undefined ? undefined : (rows[0] ?? null);
}

/**
 * Read the first matching row and subscribe to changes that affect it.
 * Suspends until the query is executed.
 *
 * The query is run with `limit 1`, mirroring {@link Db.one}. Once resolved,
 * returns the matching row, or `null` if none matched. See {@link useAllSuspense}
 * for the server-render and Suspense semantics, which this hook shares.
 *
 * @param query - the database query (e.g. `app.todos.where({ id })`)
 *
 * @returns the matching row, or `null` if none matched
 */
export function useOneSuspense<T extends { id: string }>(
  query?: QueryBuilder<T>,
  options?: QueryOptions,
): T | null {
  const rows = useAllBase(query ? limitQueryToOne(query) : undefined, options, {
    suspense: true,
  }) as T[];
  return rows[0] ?? null;
}
