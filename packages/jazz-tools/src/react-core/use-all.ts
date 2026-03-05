import * as React from "react";
import { type Usable, use, useSyncExternalStore } from "react";
import type { QueryBuilder } from "../runtime/db.js";
import { useJazzClient } from "./provider.js";

function useAllBase<T extends { id: string }>(
  query: QueryBuilder<T>,
  suspense: boolean,
): T[] | undefined {
  const { manager } = useJazzClient();

  const key = manager.makeQueryKey(query);
  const entry = manager.getCacheEntry<T>(key);
  const subscribe = React.useCallback(
    (onStoreChange: () => void) =>
      entry.subscribe({
        onfulfilled: () => {
          onStoreChange();
        },
        onDelta: () => {
          onStoreChange();
        },
        onError: () => {
          onStoreChange();
        },
      }),
    [entry],
  );
  const state = useSyncExternalStore(
    subscribe,
    () => entry.state,
    () => entry.state,
  );

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
