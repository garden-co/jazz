import { type Usable, use, useCallback, useRef, useSyncExternalStore } from "react";
import type { QueryBuilder, QueryOptions } from "../runtime/db.js";
import type { UseAllState } from "../subscriptions-orchestrator.js";
import { useJazzClient } from "./provider.js";

type UseAllOptions = {
  suspense?: boolean;
};

// A query that never arrives has nothing to fetch, so the suspense variant
// suspends on this until one is supplied on a later render (the boundary shows
// its fallback meanwhile). Distinct from a pending real query, which suspends on
// its entry promise — opened during render so a suspended effect can't strand it.
const SUSPEND_FOREVER: Promise<never> = new Promise(() => {});

function useAllBase<T extends { id: string }>(
  query?: QueryBuilder<T>,
  queryOptions?: QueryOptions,
  options?: UseAllOptions,
): T[] | undefined {
  const { suspense = false } = options ?? {};
  const { manager } = useJazzClient();

  // Pure, render-safe key: computeKey neither registers the query nor opens a
  // subscription, so the render phase stays side-effect free (concurrent/strict
  // and SSR safe). Registration + subscription happen in the commit-phase
  // `subscribe` callback below.
  const key = query ? manager.computeKey(query, queryOptions) : null;

  // Keep the latest query/options in refs so the keyed `subscribe`/`getSnapshot`
  // callbacks can read them without depending on object identity — an inline
  // `app.todos.where(...)` is a fresh object every render, but the string key is
  // stable, so we must not resubscribe just because the object changed.
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

  if (suspense) {
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

  return state?.status === "fulfilled" ? state.data : undefined;
}

/**
 * Read all matching rows and subscribe to changes that modify the query's results.
 *
 * Loading and error states are handled the React way: `undefined` means the
 * query has not resolved yet, and for error handling use {@link useAllSuspense}
 * with a Suspense + error boundary. (The Svelte and Vue bindings expose the same
 * capabilities idiomatically — Svelte's `QuerySubscription` via
 * `.current`/`.loading`/`.error`, Vue's `useAll` via `{ data, error, loading }`.)
 *
 * @param query - the database query (e.g. `app.todos.where({done: false})`)
 *
 * @returns the matching rows, or `undefined` if the query is not yet executed
 */
export function useAll<T extends { id: string }>(
  query?: QueryBuilder<T>,
  options?: QueryOptions,
): T[] | undefined {
  return useAllBase(query, options, { suspense: false });
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
  return useAllBase(query, options, { suspense: true }) as T[];
}
