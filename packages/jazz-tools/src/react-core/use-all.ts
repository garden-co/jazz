import * as React from "react";
import { type Usable, use } from "react";
import type { QueryBuilder, QueryOptions } from "../runtime/db.js";
import { useJazzClient } from "./provider.js";

type UseAllOptions = {
  suspense?: boolean;
};

const SUSPEND_FOREVER: Promise<never> = new Promise(() => {});

function useAllBase<T extends { id: string }>(
  query?: QueryBuilder<T>,
  queryOptions?: QueryOptions,
  options?: UseAllOptions,
): T[] | undefined {
  const { suspense = false } = options ?? {};
  const { manager } = useJazzClient();
  const entry = React.useMemo(() => {
    if (!query) return null;
    const key = manager.makeQueryKey(query, queryOptions);
    return manager.getCacheEntry<T>(key);
  }, [manager, query, queryOptions]);
  const dispatch = React.useReducer((_, action) => action, entry?.state)[1];

  React.useLayoutEffect(() => {
    if (!entry) return;

    const unsubscribe = entry.subscribe({
      onfulfilled: () => {
        dispatch(entry.state);
      },
      onDelta: () => {
        dispatch(entry.state);
      },
      onError: () => {
        dispatch(entry.state);
      },
    });

    return () => {
      unsubscribe();
    };
  }, [entry]);

  if (!entry) {
    if (suspense) {
      return use(SUSPEND_FOREVER as unknown as Usable<T[]>);
    }
    return undefined;
  }

  const state = entry.state;

  if (suspense) {
    if (state.status === "pending") {
      return use(state.promise as unknown as Usable<T[]>);
    }

    if (state.status === "rejected") {
      throw state.error;
    }
  }

  return state.status === "fulfilled" ? state.data : undefined;
}

/**
 * Read all matching rows and subscribe to changes that modify the query's results.
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
