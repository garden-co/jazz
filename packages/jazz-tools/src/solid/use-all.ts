import { batch, createEffect, onCleanup, type Accessor } from "solid-js";
import { createStore, produce, reconcile } from "solid-js/store";
import { applyDelta } from "../reconcile-array.js";
import type { QueryBuilder, QueryOptions } from "../runtime/db.js";
import type { SubscriptionDelta } from "../runtime/subscription-manager.js";
import { useJazzClient } from "./provider.js";

export type UseAllResult<T extends { id: string }> = {
  data: T[] | undefined;
  isLoading: boolean;
  error: Error | null;
};

export function useAll<T extends { id: string }>(
  args: Accessor<{
    query: QueryBuilder<T> | undefined;
    options?: QueryOptions | undefined;
  }>,
): UseAllResult<T> {
  const client = useJazzClient();
  const [state, setState] = createStore<UseAllResult<T>>({
    data: undefined,
    isLoading: false,
    error: null,
  });

  createEffect(() => {
    const { query, options } = args();
    if (!query) {
      setState({
        data: undefined,
        isLoading: false,
        error: null,
      });
      return;
    }

    try {
      const key = client.manager.makeQueryKey(query, options);
      const entry = client.manager.getCacheEntry<T>(key);

      setState({
        data: entry.state.data,
        isLoading: entry.state.status === "pending",
        error: entry.state.error ? normalizeError(entry.state.error) : null,
      });

      const unsubscribe = entry.subscribe({
        onError: (error: unknown) =>
          setState({
            data: undefined,
            isLoading: false,
            error: normalizeError(error),
          }),
        onfulfilled: (nextData) =>
          setState({
            data: nextData,
            isLoading: false,
            error: null,
          }),
        onDelta: (delta: SubscriptionDelta<T>) =>
          batch(() => {
            if (state.data) {
              setState(
                "data",
                produce((current) => {
                  if (!current) return;
                  applyDelta(current, delta);
                }),
              );
            } else {
              setState("data", reconcile(delta.all));
            }
            setState("isLoading", false);
            setState("error", null);
          }),
      });

      onCleanup(unsubscribe);
    } catch (error) {
      setState({
        data: undefined,
        isLoading: false,
        error: normalizeError(error),
      });
    }
  });

  return state;
}

function normalizeError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
