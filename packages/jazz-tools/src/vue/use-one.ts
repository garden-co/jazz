import { computed, toValue, type MaybeRefOrGetter, type Ref } from "vue";
import { limitQueryToOne } from "../shared/index.js";
import type { QueryBuilder, QueryOptions } from "../shared/index.js";
import { useAll, useAllSuspense } from "./use-all.js";

// `useOne` is `useAll` narrowed to a single row: the query is executed with
// `limit 1` (the same wrapper `db.one` uses, so the two stay in lockstep), and
// the subscription tracks just that row — replacing it should a closer match
// appear or the current one be removed.

/**
 * Reactive result of {@link useOne}. Like {@link UseAllResult} but for a single
 * row: `data` is the matching row, `null` once the query resolves with no match,
 * or `undefined` while loading (and on error); `error` is the last subscription
 * error (or `null`), and `loading` is `true` until the first result or error.
 */
export interface UseOneResult<T extends { id: string }> {
  data: Ref<T | null | undefined>;
  error: Ref<Error | null>;
  loading: Ref<boolean>;
}

/**
 * Result of {@link useOneSuspense}. Like {@link UseOneResult} but without
 * `loading`: the suspense variant only returns once the first result (or error)
 * has resolved, so a `loading` flag would carry no information at the point of
 * use.
 */
export interface UseOneSuspenseResult<T extends { id: string }> {
  data: Ref<T | null | undefined>;
  error: Ref<Error | null>;
}

function toSingle<T>(rows: T[] | undefined): T | null | undefined {
  return rows === undefined ? undefined : (rows[0] ?? null);
}

/**
 * Read the first matching row and subscribe to changes that affect it.
 *
 * The query is run with `limit 1`, mirroring {@link useAll} but narrowed to a
 * single row.
 *
 * @param query - the database query (e.g. `app.todos.where({ id })`)
 * @param options - optional query execution options
 *
 * @returns reactive `{ data, error, loading }`. `data` is `undefined` while
 *   loading, `null` once the query resolves with no match, or the matching row;
 *   `error` is set if the subscription fails.
 */
export function useOne<T extends { id: string }>(
  query: MaybeRefOrGetter<QueryBuilder<T> | undefined>,
  options?: MaybeRefOrGetter<QueryOptions | undefined>,
): UseOneResult<T> {
  const {
    data: rows,
    error,
    loading,
  } = useAll<T>(() => {
    const q = toValue(query);
    return q ? limitQueryToOne(q) : undefined;
  }, options);

  const data = computed(() => toSingle(rows.value));

  return { data, error, loading };
}

/**
 * Suspense-compatible variant of {@link useOne} for use in an `async setup()`
 * under Vue `<Suspense>`. Resolves once the query's first result is available,
 * and rejects (surfacing to the nearest error boundary) if it fails.
 *
 * The query is run with `limit 1`, mirroring {@link useOne}.
 *
 * @param query - the database query (e.g. `app.todos.where({ id })`)
 * @param options - optional query execution options
 *
 * @returns reactive `{ data, error }`. Unlike {@link useOne}, there is no
 *   `loading` flag: the promise only resolves once the first result is
 *   available, so the query is never loading at the point of use. `data` is the
 *   matching row, or `null` if none matched.
 */
export async function useOneSuspense<T extends { id: string }>(
  query: QueryBuilder<T>,
  options?: QueryOptions,
): Promise<UseOneSuspenseResult<T>> {
  const { data: rows, error } = await useAllSuspense<T>(limitQueryToOne(query), options);

  const data = computed(() => toSingle(rows.value));

  return { data, error };
}
