import type { Accessor } from "solid-js";
import type { QueryBuilder, QueryOptions } from "../runtime/db.js";
import { limitQueryToOne } from "../runtime/limit-query.js";
import { useAll } from "./use-all.js";

/**
 * Reactive result of {@link useOne}. Like {@link UseAllResult} but for a single
 * row: `data` is the matching row, `null` once the query resolves with no match,
 * or `undefined` while loading; `isLoading` is `true` until the query resolves;
 * `error` holds the subscription error, or `null`.
 */
export type UseOneResult<T extends { id: string }> = {
  data: T | null | undefined;
  isLoading: boolean;
  error: Error | null;
};

/**
 * Read the first matching row and subscribe to changes that affect it.
 *
 * Narrows {@link useAll} to a single row: the query is executed with `limit 1`
 * (via {@link limitQueryToOne}, the same wrapper `Db.one` uses, so the two stay
 * in lockstep), and the subscription tracks just that row.
 *
 * `data` is `undefined` while loading, `null` once the query resolves with no
 * match, or the matching row otherwise.
 */
export function useOne<T extends { id: string }>(
  args: Accessor<{
    query: QueryBuilder<T> | undefined;
    options?: QueryOptions | undefined;
  }>,
): UseOneResult<T> {
  const all = useAll<T>(() => {
    const { query, options } = args();
    return { query: query ? limitQueryToOne(query) : undefined, options };
  });

  return {
    get data() {
      return all.data === undefined ? undefined : (all.data[0] ?? null);
    },
    get isLoading() {
      return all.isLoading;
    },
    get error() {
      return all.error;
    },
  };
}
