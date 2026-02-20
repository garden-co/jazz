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
  | { type: "entry_delta"; delta: SubscriptionDelta<T> }
  | { type: "entry_error"; error: unknown };

function applyDelta<T extends { id: string }>(prev: T[], delta: SubscriptionDelta<T>): T[] {
  const next = [...prev];

  for (const { index } of [...delta.removed].sort((a, b) => b.index - a.index)) {
    if (index >= 0 && index < next.length) {
      next.splice(index, 1);
    }
  }

  for (const { oldIndex, newIndex, newItem } of [...delta.updated].sort(
    (a, b) => b.oldIndex - a.oldIndex,
  )) {
    if (oldIndex >= 0 && oldIndex < next.length) {
      next.splice(oldIndex, 1);
    }
    const insertIndex = Math.max(0, Math.min(newIndex, next.length));
    next.splice(insertIndex, 0, newItem);
  }

  for (const { item, index } of [...delta.added].sort((a, b) => a.index - b.index)) {
    const insertIndex = Math.max(0, Math.min(index, next.length));
    next.splice(insertIndex, 0, item);
  }

  return next;
}

function reducer<T extends { id: string }>(
  prev: UseAllState<T>,
  action: UseAllAction<T>,
): UseAllState<T> {
  switch (action.type) {
    case "entry_fulfilled":
      return { status: "fulfilled", data: action.data, error: null };
    case "entry_delta":
      if (prev.status !== "fulfilled") return prev;
      return { status: "fulfilled", data: applyDelta(prev.data, action.delta), error: null };
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
