import type { QueryBuilder, QueryOptions } from "../shared/index.js";
import { limitQueryToOne } from "../runtime/limit-query.js";
import { useAllResultBase, useAllSuspenseBase } from "./use-all.js";

// `useOne` is `useAll` narrowed to a single row: the query is executed with
// `limit 1` (the same wrapper `db.one` uses, so the two stay in lockstep), and
// the subscription tracks just that row — replacing it should a closer match
// appear or the current one be removed.

/**
 * Reactive result of {@link useOne}. Like {@link UseAllResult} but for a single
 * row: `data` is the matching row, `null` once the query resolves with no match,
 * or `undefined` while loading (and on error); `isLoading` is `true` until the
 * query resolves; `error` holds the subscription error, or `null`.
 */
export type UseOneResult<T extends { id: string }> = {
  data: T | null | undefined;
  isLoading: boolean;
  error: Error | null;
};

/**
 * Read the first matching row and subscribe to changes that affect it.
 *
 * The query is run with `limit 1`, mirroring {@link Db.one}.
 *
 * @param query - the database query (e.g. `app.todos.where({ id })`)
 *
 * @returns reactive `{ data, isLoading, error }`. `data` is the matching row,
 *   `null` once the query resolves with no match, or `undefined` while loading
 *   (and on error); `isLoading` is `true` until the query resolves. For
 *   Suspense-based loading/error handling use {@link useOneSuspense}.
 */
export function useOne<T extends { id: string }>(
  query?: QueryBuilder<T>,
  options?: QueryOptions,
): UseOneResult<T> {
  const {
    data: rows,
    isLoading,
    error,
  } = useAllResultBase(query ? limitQueryToOne(query) : undefined, options);
  const data = rows === undefined ? undefined : (rows[0] ?? null);
  return { data, isLoading, error };
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
  const rows = useAllSuspenseBase(query ? limitQueryToOne(query) : undefined, options);
  return rows[0] ?? null;
}
