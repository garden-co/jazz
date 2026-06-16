import { type Usable, use, useCallback, useRef, useSyncExternalStore } from "react";
import type { DehydratedSnapshot } from "../backend/ssr.js";
import type { QueryBuilder, QueryOptions } from "../runtime/db.js";
import { applySnapshot } from "../ssr/apply-snapshot.js";
import type { UseAllState } from "../subscriptions-orchestrator.js";
import { useJazzClient } from "./provider.js";

/** Options for {@link useAll}: ordinary query options plus an optional SSR snapshot. */
type UseAllOptions = QueryOptions & {
  /**
   * A server-rendered snapshot for this query, co-located at the call site.
   * Seeds the rows for synchronous first paint and queues its sync bundle for
   * flash-free hydration when the db attaches. The orchestrator decides what to
   * do with it; the hook just hands it over.
   */
  snapshot?: DehydratedSnapshot;
};

type UseAllBaseOptions = {
  suspense?: boolean;
  snapshot?: DehydratedSnapshot;
};

// A query that never arrives has nothing to fetch, so the suspense variant
// suspends on this until one is supplied on a later render (the boundary shows
// its fallback meanwhile). Distinct from a pending real query, which suspends on
// its entry promise — opened during render so a suspended effect can't strand it.
const SUSPEND_FOREVER: Promise<never> = new Promise(() => {});

function useAllBase<T extends { id: string }>(
  query?: QueryBuilder<T>,
  queryOptions?: QueryOptions,
  options?: UseAllBaseOptions,
): T[] | undefined {
  const { suspense = false, snapshot } = options ?? {};
  const client = useJazzClient();
  const { manager } = client;

  // Apply the co-located snapshot exactly once, synchronously, before the first
  // key lookup — so the first paint already reads the seeded rows. The ref
  // guards against React's double-render (StrictMode) re-applying it.
  const snapshotApplied = useRef(false);
  if (snapshot && !snapshotApplied.current) {
    snapshotApplied.current = true;
    applySnapshot({
      manager,
      snapshot,
      expected: { principalId: client.session?.user_id ?? null },
    });
  }

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
 * @param options - query options, optionally including a server-rendered `snapshot`
 *
 * @returns the matching rows, or `undefined` if the query is not yet executed
 */
/**
 * Split a `useAll` options bag into the snapshot and the remaining query
 * options, preserving `undefined` when no query options are left — so the cache
 * key matches a caller that passed no options at all (the snapshot must never
 * affect the key).
 */
function splitSnapshot(options?: UseAllOptions): {
  queryOptions?: QueryOptions;
  snapshot?: DehydratedSnapshot;
} {
  if (!options || !("snapshot" in options)) {
    return { queryOptions: options };
  }
  const { snapshot, ...rest } = options;
  return { queryOptions: Object.keys(rest).length > 0 ? rest : undefined, snapshot };
}

export function useAll<T extends { id: string }>(
  query?: QueryBuilder<T>,
  options?: UseAllOptions,
): T[] | undefined {
  const { queryOptions, snapshot } = splitSnapshot(options);
  return useAllBase(query, queryOptions, { suspense: false, snapshot });
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
 * @param options - query options, optionally including a server-rendered `snapshot`
 *
 * @returns the matching rows
 */
export function useAllSuspense<T extends { id: string }>(
  query?: QueryBuilder<T>,
  options?: UseAllOptions,
): T[] {
  const { queryOptions, snapshot } = splitSnapshot(options);
  return useAllBase(query, queryOptions, { suspense: true, snapshot }) as T[];
}
