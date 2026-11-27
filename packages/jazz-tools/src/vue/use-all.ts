import {
  ref,
  shallowRef,
  toValue,
  watchEffect,
  type MaybeRefOrGetter,
  type Ref,
  type ShallowRef,
} from "vue";
import { applyDeltaToRows } from "../reconcile-array.js";
import type { Db } from "../runtime/db.js";
import type {
  CacheEntryHandle,
  QueryBuilder,
  QueryOptions,
  SubscriptionDelta,
  UseAllState,
} from "../shared/index.js";
import { getSubscriptionStore } from "../subscription-store-internal.js";
import { useJazzClient } from "./provider.js";
import { toError } from "../runtime/util.js";
import { unreadableRows, type UnreadableRow } from "../encryption/unreadable.js";

const NO_UNREADABLE: readonly UnreadableRow[] = Object.freeze([]);

/**
 * Reactive result of {@link useAll}. `data` is the matching rows (or `undefined`
 * while loading or on error), `error` is the last subscription error (or
 * `null`), and `isLoading` is `true` until the first result or error. `db` is
 * the (non-reactive) {@link Db} handle for writes, so components don't need a
 * separate `useDb()`. `unreadable` lists the rows `data` leaves out because
 * this device could not decrypt them (empty when nothing was left out).
 */
export interface UseAllResult<T extends { id: string }> {
  data: Ref<T[] | undefined>;
  error: Ref<Error | null>;
  isLoading: Ref<boolean>;
  db: Db;
  unreadable: Readonly<ShallowRef<readonly UnreadableRow[]>>;
}

/**
 * Result of {@link useAllSuspense}. Like {@link UseAllResult} but without
 * `isLoading`: the suspense variant only returns once the first result (or error)
 * has resolved, so an `isLoading` flag would carry no information at the point of
 * use.
 */
export interface UseAllSuspenseResult<T extends { id: string }> {
  data: Ref<T[] | undefined>;
  error: Ref<Error | null>;
  db: Db;
  unreadable: Readonly<ShallowRef<readonly UnreadableRow[]>>;
}

type Refs<T extends { id: string }> = {
  data: Ref<T[] | undefined>;
  error: Ref<Error | null>;
  isLoading: Ref<boolean>;
  unreadable: ShallowRef<readonly UnreadableRow[]>;
};

function applyEntryState<T extends { id: string }>(
  state: UseAllState<T>,
  { data, error, isLoading, unreadable }: Refs<T>,
): void {
  if (state.status === "fulfilled") {
    // Own a copy: deltas are applied to this array in place, and the cache
    // entry's array is shared with every other listener on the same query.
    data.value = [...state.data];
    unreadable.value = unreadableRows(state.data);
    error.value = null;
    isLoading.value = false;
  } else if (state.status === "rejected") {
    data.value = undefined;
    unreadable.value = NO_UNREADABLE;
    error.value = toError(state.error);
    isLoading.value = false;
  } else {
    data.value = undefined;
    unreadable.value = NO_UNREADABLE;
  }
}

function subscribeToEntry<T extends { id: string }>(
  entry: CacheEntryHandle<T>,
  refs: Refs<T>,
): () => void {
  const { data, error, isLoading, unreadable } = refs;
  applyEntryState(entry.state, refs);

  return entry.subscribe({
    onfulfilled: (nextData) => {
      data.value = [...nextData];
      unreadable.value = unreadableRows(nextData);
      error.value = null;
      isLoading.value = false;
    },
    onDelta: (delta: SubscriptionDelta<T>) => {
      data.value = applyDeltaToRows(data.value, delta);
      // The cache entry's array carries the list of rows its result leaves out.
      unreadable.value =
        entry.state.status === "fulfilled" ? unreadableRows(entry.state.data) : NO_UNREADABLE;
      isLoading.value = false;
    },
    onError: (err) => {
      error.value = toError(err);
      data.value = undefined;
      unreadable.value = NO_UNREADABLE;
      isLoading.value = false;
    },
    onReset: () => {
      data.value = undefined;
      unreadable.value = NO_UNREADABLE;
      error.value = null;
      isLoading.value = true;
    },
  });
}

/**
 * Read all matching rows and subscribe to changes that modify the query's results.
 *
 * @param query - the database query (e.g. `app.todos.where({ done: false })`)
 * @param options - optional query execution options
 *
 * @returns `{ data, isLoading, error, db, unreadable }`. `data`, `isLoading`,
 *   `error` and `unreadable` are refs: `data` is `undefined` until the query
 *   resolves; `error` is set if the subscription fails; `unreadable` lists the
 *   rows left out because this device could not decrypt them. `db` is the
 *   {@link Db} handle for writes.
 */
export function useAll<T extends { id: string }>(
  query: MaybeRefOrGetter<QueryBuilder<T> | undefined>,
  options?: MaybeRefOrGetter<QueryOptions | undefined>,
): UseAllResult<T> {
  const client = useJazzClient();
  const store = getSubscriptionStore(client);
  const data = ref<T[] | undefined>(undefined) as Ref<T[] | undefined>;
  const error = ref<Error | null>(null);
  const isLoading = ref(true);
  // Shallow: the list is replaced whole, and stays the frozen array it was given.
  const unreadable = shallowRef<readonly UnreadableRow[]>(NO_UNREADABLE);
  const refs: Refs<T> = { data, error, isLoading, unreadable };

  watchEffect((onCleanup) => {
    const resolvedQuery = toValue(query);
    if (!resolvedQuery) {
      data.value = undefined;
      unreadable.value = NO_UNREADABLE;
      error.value = null;
      isLoading.value = false;
      return;
    }
    const resolvedOptions = toValue(options);

    isLoading.value = true;
    error.value = null;

    const key = store.makeQueryKey(resolvedQuery, resolvedOptions);
    const entry = store.getCacheEntry<T>(key);
    const unsubscribe = subscribeToEntry(entry, refs);

    onCleanup(() => {
      unsubscribe();
    });
  });

  return { data, isLoading, error, db: client.db, unreadable };
}

/**
 * Suspense-compatible variant of {@link useAll} for use in an `async setup()`
 * under Vue `<Suspense>`. Resolves once the query's first result is available,
 * and rejects (surfacing to the nearest error boundary) if it fails.
 *
 * @param query - the database query (e.g. `app.todos.where({ done: false })`)
 * @param options - optional query execution options
 *
 * @returns `{ data, error, db, unreadable }` (`data`, `error` and `unreadable` are refs). Unlike
 *   {@link useAll}, there is no
 *   `isLoading` flag: the promise only resolves once the first result is
 *   available, so the query is never loading at the point of use.
 */
export async function useAllSuspense<T extends { id: string }>(
  query: QueryBuilder<T>,
  options?: QueryOptions,
): Promise<UseAllSuspenseResult<T>> {
  const store = getSubscriptionStore(useJazzClient());
  const { data, error, db, unreadable } = useAll<T>(query, options);

  const key = store.makeQueryKey(query, options);
  const entry = store.getCacheEntry<T>(key);
  await entry.promise;

  return { data, error, db, unreadable };
}
