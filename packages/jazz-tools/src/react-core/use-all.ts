import { type Usable, use, useCallback, useRef, useSyncExternalStore } from "react";
import type { QueryBuilder, QueryOptions, UseAllState } from "../shared/index.js";
import { useJazzClient } from "./provider.js";

/**
 * Reactive result of {@link useAll}. `data` is the matching rows (`undefined`
 * until the first result resolves, or on error), `isLoading` is `true` until the
 * first result or error arrives, and `error` is the last subscription error (or
 * `null`). Mirrors the shape Solid's `useAll` returns.
 */
export type UseAllResult<T extends { id: string }> = {
  data: T[] | undefined;
  isLoading: boolean;
  error: Error | null;
};

// A query that never arrives has nothing to fetch, so the suspense variant
// suspends on this until one is supplied on a later render (the boundary shows
// its fallback meanwhile). Distinct from a pending real query, which suspends on
// its entry promise — opened during render so a suspended effect can't strand it.
const SUSPEND_FOREVER: Promise<never> = new Promise(() => {});

function toError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}

/**
 * Subscription wiring shared by every react-core query hook: it computes the
 * pure cache key, keeps the latest query/options in refs, and drives a
 * `useSyncExternalStore`, returning the raw cache state. Not part of the public
 * API.
 *
 * `computeKey` neither registers the query nor opens a subscription, so the
 * render phase stays side-effect free (concurrent/strict and SSR safe).
 * Registration + subscription happen in the commit-phase `subscribe` callback.
 *
 * The refs let the keyed `subscribe`/`getSnapshot` callbacks read the latest
 * query/options without depending on object identity — an inline
 * `app.todos.where(...)` is a fresh object every render, but the string key is
 * stable, so we must not resubscribe just because the object changed.
 *
 * @internal
 */
function useQuerySubscription<T extends { id: string }>(
  query?: QueryBuilder<T>,
  queryOptions?: QueryOptions,
): {
  manager: ReturnType<typeof useJazzClient>["manager"];
  key: string | null;
  state: UseAllState<T> | null;
} {
  const { manager } = useJazzClient();

  const key = query ? manager.computeKey(query, queryOptions) : null;

  const queryRef = useRef(query);
  queryRef.current = query;
  const optionsRef = useRef(queryOptions);
  optionsRef.current = queryOptions;

  const subscribe = useCallback(
    (onStoreChange: () => void) => {
      const q = queryRef.current;
      if (!q || key === null) {
        return () => {};
      }
      manager.makeQueryKey(q, optionsRef.current);
      const entry = manager.getCacheEntry<T>(key);
      return entry.subscribe({
        onfulfilled: onStoreChange,
        onDelta: onStoreChange,
        onError: onStoreChange,
        onReset: onStoreChange,
      });
    },
    [manager, key],
  );

  const getSnapshot = useCallback(
    (): UseAllState<T> | null => (key === null ? null : manager.peekState<T>(key)),
    [manager, key],
  );

  const state = useSyncExternalStore(subscribe, getSnapshot, getSnapshot);

  return { manager, key, state };
}

/**
 * Non-suspense engine behind {@link useAll} and the `useOne` bindings: returns
 * the live `{ data, isLoading, error }` for a query. Not part of the public API.
 *
 * @internal
 */
export function useAllResultBase<T extends { id: string }>(
  query?: QueryBuilder<T>,
  queryOptions?: QueryOptions,
): UseAllResult<T> {
  const { state } = useQuerySubscription(query, queryOptions);

  if (!query) {
    return { data: undefined, isLoading: false, error: null };
  }
  if (!state || state.status === "pending") {
    return { data: undefined, isLoading: true, error: null };
  }
  if (state.status === "rejected") {
    return { data: undefined, isLoading: false, error: toError(state.error) };
  }
  return { data: state.data, isLoading: false, error: null };
}

/**
 * Suspense engine behind {@link useAllSuspense} and {@link useOneSuspense}:
 * returns the resolved rows, suspending until the query resolves and throwing
 * its error to the nearest boundary. Not part of the public API.
 *
 * @internal
 */
export function useAllSuspenseBase<T extends { id: string }>(
  query?: QueryBuilder<T>,
  queryOptions?: QueryOptions,
): T[] {
  const { manager, key, state } = useQuerySubscription(query, queryOptions);

  if (!query || key === null) {
    return use(SUSPEND_FOREVER as unknown as Usable<T[]>);
  }

  if (state) {
    if (state.status === "fulfilled") {
      return state.data;
    }
    if (state.status === "rejected") {
      throw state.error;
    }
  }

  // Pending: a suspense data source must start fetching during render — the
  // `subscribe` effect can't run while the boundary is suspended — so create
  // the entry here and suspend on its real promise (which resolves on the
  // first result), rather than a sentinel that never resolves.
  manager.makeQueryKey(query, queryOptions);
  const entry = manager.getCacheEntry<T>(key);
  if (entry.state.status === "rejected") {
    throw entry.state.error;
  }
  if (entry.state.status === "fulfilled") {
    return entry.state.data;
  }
  return use(entry.promise as unknown as Usable<T[]>);
}

/**
 * Read all matching rows and subscribe to changes that modify the query's results.
 *
 * @param query - the database query (e.g. `app.todos.where({done: false})`)
 *
 * @returns reactive `{ data, isLoading, error }`. `data` is `undefined` until the
 *   query resolves (and on error); `isLoading` is `true` until the first result
 *   or error; `error` holds the subscription error, or `null`. For Suspense-based
 *   loading/error handling use {@link useAllSuspense}. (The Svelte and Vue
 *   bindings expose the same capabilities idiomatically — Svelte's
 *   `QuerySubscription` via `.current`/`.loading`/`.error`, Vue's `useAll` via
 *   `{ data, error, loading }`.)
 */
export function useAll<T extends { id: string }>(
  query?: QueryBuilder<T>,
  options?: QueryOptions,
): UseAllResult<T> {
  return useAllResultBase(query, options);
}

/**
 * Read all matching rows and subscribe to changes that modify the query's results.
 * Suspends until the query is executed.
 *
 * On the server, a seeded snapshot (provided by the SSR hydration setup) is read
 * synchronously and no subscription is opened. Without a seed the hook suspends:
 * a suspense data source must begin fetching during render, so the subscription
 * is opened during the server render and the boundary renders its fallback (the
 * promise does not resolve server-side).
 *
 * @param query - the database query (e.g. `app.todos.where({done: false})`)
 *
 * @returns the matching rows
 */
export function useAllSuspense<T extends { id: string }>(
  query?: QueryBuilder<T>,
  options?: QueryOptions,
): T[] {
  return useAllSuspenseBase(query, options);
}
