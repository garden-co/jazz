import { ref, toValue, watchEffect, type MaybeRefOrGetter, type Ref } from "vue";
import { applyDelta } from "../shared/index.js";
import type {
  CacheEntryHandle,
  QueryBuilder,
  QueryOptions,
  QuerySettlementLevel,
  SubscriptionDelta,
  UseAllState,
} from "../shared/index.js";
import { getSubscriptionStore } from "../subscription-store-internal.js";
import { useJazzClient } from "./provider.js";

/**
 * Reactive result of {@link useAll}. `data` is the matching rows or a materialized
 * preview while loading (and `undefined` on error), `error` is the last subscription
 * error (or `null`), `isLoading` is `true` until the requested first result, and
 * `highestSettledAt` is the highest settlement level observed during the subscription.
 */
export interface UseAllResult<T extends { id: string }> {
  data: Ref<T[] | undefined>;
  error: Ref<Error | null>;
  isLoading: Ref<boolean>;
  highestSettledAt: Ref<QuerySettlementLevel>;
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
  highestSettledAt: Ref<QuerySettlementLevel>,
): void {
  highestSettledAt.value = state.highestSettledAt;
  if (state.status === "fulfilled") {
    data.value = state.data;
    error.value = null;
    isLoading.value = false;
  } else if (state.status === "rejected") {
    data.value = undefined;
    error.value = toError(state.error);
    isLoading.value = false;
  } else {
    data.value = state.data;
    error.value = null;
    isLoading.value = true;
  }
}

function subscribeToEntry<T extends { id: string }>(
  entry: CacheEntryHandle<T>,
  data: Ref<T[] | undefined>,
  error: Ref<Error | null>,
  isLoading: Ref<boolean>,
  highestSettledAt: Ref<QuerySettlementLevel>,
): () => void {
  applyEntryState(entry.state, data, error, isLoading, highestSettledAt);

  return entry.subscribe({
    onfulfilled: (nextData) => {
      data.value = nextData;
      highestSettledAt.value = entry.state.highestSettledAt;
      error.value = null;
      isLoading.value = false;
    },
    onDelta: (delta: SubscriptionDelta<T>) => {
      highestSettledAt.value = entry.state.highestSettledAt;
      const metadataOnly = !delta.reset && delta.all === undefined && delta.delta.length === 0;
      if (!metadataOnly) {
        if (data.value) {
          applyDelta(data.value, delta);
        } else if (delta.reset) {
          data.value = delta.all;
        } else {
          data.value = [];
          applyDelta(data.value, delta);
        }
      }
      isLoading.value = entry.state.status === "pending";
      error.value = null;
    },
    onError: (err) => {
      highestSettledAt.value = entry.state.highestSettledAt;
      error.value = toError(err);
      data.value = undefined;
      isLoading.value = false;
    },
    onReset: () => {
      data.value = undefined;
      highestSettledAt.value = "unconfirmed";
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
  const highestSettledAt = ref<QuerySettlementLevel>("unconfirmed");

  watchEffect((onCleanup) => {
    const resolvedQuery = toValue(query);
    if (!resolvedQuery) {
      data.value = undefined;
      error.value = null;
      isLoading.value = false;
      highestSettledAt.value = "unconfirmed";
      return;
    }
    const resolvedOptions = toValue(options);

    isLoading.value = true;
    error.value = null;

    const key = store.makeQueryKey(resolvedQuery, resolvedOptions);
    const entry = store.getCacheEntry<T>(key);
    const unsubscribe = subscribeToEntry(entry, data, error, isLoading, highestSettledAt);

    onCleanup(() => {
      unsubscribe();
    });
  });

  return { data, isLoading, error, highestSettledAt };
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
