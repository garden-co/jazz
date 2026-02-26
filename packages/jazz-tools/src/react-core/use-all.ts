import * as React from "react";
import { type Usable, use } from "react";
import type { QueryBuilder } from "../runtime/db.js";
import type { SubscriptionDelta } from "../runtime/subscription-manager.js";
import type { TrackedPromise, UseAllState } from "../subscriptions-orchestrator.js";
import { useJazzClient } from "./provider.js";

type UseAllAction<T extends { id: string }> =
  | { type: "entry_pending"; promise: TrackedPromise<T[]> }
  | { type: "entry_fulfilled"; data: T[] }
  | { type: "entry_delta"; delta: SubscriptionDelta<T> }
  | { type: "entry_error"; error: unknown };

function reducer<T extends { id: string }>(
  prev: UseAllState<T>,
  action: UseAllAction<T>,
): UseAllState<T> {
  switch (action.type) {
    case "entry_pending":
      return { status: "pending", data: undefined, promise: action.promise, error: null };
    case "entry_fulfilled":
      return { status: "fulfilled", data: action.data, error: null };
    case "entry_delta":
      return { status: "fulfilled", data: action.delta.all, error: null };
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
    if (entry.state.status === "pending") {
      dispatch({ type: "entry_pending", promise: entry.state.promise });
    } else if (entry.state.status === "fulfilled") {
      dispatch({ type: "entry_fulfilled", data: entry.state.data });
    } else if (entry.state.status === "rejected") {
      dispatch({ type: "entry_error", error: entry.state.error });
    }

    const unsubscribe = entry.subscribe({
      onfulfilled: (data: T[]) => {
        dispatch({ type: "entry_fulfilled", data });
      },
      onDelta: (delta: SubscriptionDelta<T>) => {
        dispatch({ type: "entry_delta", delta });
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
