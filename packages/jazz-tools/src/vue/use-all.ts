import { ref, toValue, watchEffect, type MaybeRefOrGetter, type Ref } from "vue";
import { applyDelta } from "../shared/index.js";
import type {
  CacheEntryHandle,
  QueryBuilder,
  QueryOptions,
  SubscriptionDelta,
  UseAllState,
} from "../shared/index.js";
import { getSubscriptionStore } from "../subscription-store-internal.js";
import { useJazzClient } from "./provider.js";

/**
 * Reactive result of {@link useAll}. `data` is the matching rows (or `undefined`
 * while loading or on error), `error` is the last subscription error (or
 * `null`), and `isLoading` is `true` until the first result or error.
 */
export interface UseAllResult<T extends { id: string }> {
  data: Ref<T[] | undefined>;
  error: Ref<Error | null>;
  isLoading: Ref<boolean>;
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
}

function toError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}

function applyEntryState<T extends { id: string }>(
  state: UseAllState<T>,
  data: Ref<T[] | undefined>,
  error: Ref<Error | null>,
  isLoading: Ref<boolean>,
): void {
  if (state.status === "fulfilled") {
    data.value = state.data;
    error.value = null;
    isLoading.value = false;
  } else if (state.status === "rejected") {
    data.value = undefined;
    error.value = toError(state.error);
    isLoading.value = false;
  } else {
    data.value = undefined;
  }
}

function subscribeToEntry<T extends { id: string }>(
  entry: CacheEntryHandle<T>,
  data: Ref<T[] | undefined>,
  error: Ref<Error | null>,
  isLoading: Ref<boolean>,
): () => void {
  applyEntryState(entry.state, data, error, isLoading);

  return entry.subscribe({
    onfulfilled: (nextData) => {
      data.value = nextData;
      error.value = null;
      isLoading.value = false;
    },
    onDelta: (delta: SubscriptionDelta<T>) => {
      if (data.value) {
        applyDelta(data.value, delta);
      } else if (delta.reset) {
        data.value = delta.all;
      } else {
        data.value = [];
        applyDelta(data.value, delta);
      }
      isLoading.value = false;
    },
    onError: (err) => {
      error.value = toError(err);
      data.value = undefined;
      isLoading.value = false;
    },
    onReset: () => {
      data.value = undefined;
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
 * @returns reactive `{ data, isLoading, error }`. `data` is `undefined` until the
 *   query resolves; `error` is set if the subscription fails.
 */
export function useAll<T extends { id: string }>(
  query: MaybeRefOrGetter<QueryBuilder<T> | undefined>,
  options?: MaybeRefOrGetter<QueryOptions | undefined>,
): UseAllResult<T> {
  const store = getSubscriptionStore(useJazzClient());
  const data = ref<T[] | undefined>(undefined) as Ref<T[] | undefined>;
  const error = ref<Error | null>(null);
  const isLoading = ref(true);

  watchEffect((onCleanup) => {
    const resolvedQuery = toValue(query);
    if (!resolvedQuery) {
      data.value = undefined;
      error.value = null;
      isLoading.value = false;
      return;
    }
    const resolvedOptions = toValue(options);

    isLoading.value = true;
    error.value = null;

    const key = store.makeQueryKey(resolvedQuery, resolvedOptions);
    const entry = store.getCacheEntry<T>(key);
    const unsubscribe = subscribeToEntry(entry, data, error, isLoading);

    onCleanup(() => {
      unsubscribe();
    });
  });

  return { data, isLoading, error };
}

/**
 * Suspense-compatible variant of {@link useAll} for use in an `async setup()`
 * under Vue `<Suspense>`. Resolves once the query's first result is available,
 * and rejects (surfacing to the nearest error boundary) if it fails.
 *
 * @param query - the database query (e.g. `app.todos.where({ done: false })`)
 * @param options - optional query execution options
 *
 * @returns reactive `{ data, error }`. Unlike {@link useAll}, there is no
 *   `isLoading` flag: the promise only resolves once the first result is
 *   available, so the query is never loading at the point of use.
 */
export async function useAllSuspense<T extends { id: string }>(
  query: QueryBuilder<T>,
  options?: QueryOptions,
): Promise<UseAllSuspenseResult<T>> {
  const store = getSubscriptionStore(useJazzClient());
  const { data, error } = useAll<T>(query, options);

  const key = store.makeQueryKey(query, options);
  const entry = store.getCacheEntry<T>(key);
  await entry.promise;

  return { data, error };
}
