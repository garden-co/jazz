import * as React from "react";
import { type Usable, use } from "react";
import type { PersistenceTier } from "../runtime/client.js";
import type { QueryBuilder } from "../runtime/db.js";
import type { SubscriptionDelta } from "../runtime/subscription-manager.js";
import type { UseAllState } from "../subscriptions-orchestrator.js";
import { useJazzClient } from "./provider.js";

type UseAllOptions = {
  tier?: PersistenceTier;
  suspense?: boolean;
};

type UseAllSuspenseOptions = Omit<UseAllOptions, "suspense">;

type UseAllAction<T extends { id: string }> =
  | { type: "entry_fulfilled"; data: T[] }
  | { type: "entry_delta"; data: T[] }
  | { type: "entry_error"; error: unknown };

function reducer<T extends { id: string }>(
  prev: UseAllState<T>,
  action: UseAllAction<T>,
): UseAllState<T> {
  switch (action.type) {
    case "entry_fulfilled":
      return { status: "fulfilled", data: action.data, error: null };
    case "entry_delta":
      return { status: "fulfilled", data: action.data, error: null };
    case "entry_error":
      return { status: "rejected", data: undefined, error: action.error };
    default:
      return prev;
  }
}

function useAllBase<T extends { id: string }>(
  query: QueryBuilder<T>,
  suspense: boolean,
): T[] | undefined {
  const { manager } = useJazzClient();

  const key = manager.makeQueryKey(query);
  const entry = manager.getCacheEntry<T>(key);
  const [state, dispatch] = React.useReducer(reducer<T>, entry.state);

  React.useLayoutEffect(() => {
    const unsubscribe = entry.subscribe({
      onfulfilled: (data: T[]) => {
        dispatch({ type: "entry_fulfilled", data });
      },
      onDelta: (delta: SubscriptionDelta<T>) => {
        dispatch({ type: "entry_delta", data: delta.all });
      },
      onError: (error: unknown) => {
        dispatch({ type: "entry_error", error });
      },
    });

    return () => {
      unsubscribe();
    };
  }, [entry]);

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
