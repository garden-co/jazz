import * as React from "react";
import { type Usable, use } from "react";
import type { QueryBuilder } from "../runtime/db.js";
import { useJazzClient } from "./provider.js";

function useAllBase<T extends { id: string }>(
  query: QueryBuilder<T>,
  suspense: boolean,
): T[] | undefined {
  const { manager } = useJazzClient();

  const key = manager.makeQueryKey(query);
  const entry = manager.getCacheEntry<T>(key);
  const dispatch = React.useReducer((_, action) => action, entry.state)[1];

  React.useLayoutEffect(() => {
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

export function useAll<T extends { id: string }>(query: QueryBuilder<T>): T[] | undefined {
  return useAllBase(query, false);
}

export function useAllSuspense<T extends { id: string }>(query: QueryBuilder<T>): T[] {
  return useAllBase(query, true) as T[];
}
