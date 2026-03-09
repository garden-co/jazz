import * as React from "react";
import { type Usable, use } from "react";
import type { QueryBuilder } from "../runtime/db.js";
import { useJazzClient } from "./provider.js";

type UseAllOptions = {
  suspense?: boolean;
};

const SUSPEND_FOREVER: Promise<never> = new Promise(() => {});

function useAllBase<T extends { id: string }>(
  query?: QueryBuilder<T>,
  options?: UseAllOptions,
): T[] | undefined {
  const { suspense = false } = options ?? {};
  const { manager } = useJazzClient();
  const entry = React.useMemo(() => {
    if (!query) return null;
    const key = manager.makeQueryKey(query);
    return manager.getCacheEntry<T>(key);
  }, [manager, query]);
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

export function useAll<T extends { id: string }>(query?: QueryBuilder<T>): T[] | undefined {
  return useAllBase(query, { suspense: false });
}

export function useAllSuspense<T extends { id: string }>(query?: QueryBuilder<T>): T[] {
  return useAllBase(query, { suspense: true }) as T[];
}
