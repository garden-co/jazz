import { batch, createEffect, onCleanup, type Accessor } from "solid-js";
import { createStore, produce, reconcile } from "solid-js/store";
import { applyDelta, applyDeltaToRows } from "../reconcile-array.js";
import type { QueryBuilder, QueryOptions } from "../runtime/db.js";
import type { SubscriptionDelta } from "../runtime/subscription-manager.js";
import { getSubscriptionStore } from "../subscription-store-internal.js";
import { useJazzClient } from "./provider.js";
import { toError } from "../runtime/util.js";

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
  const store = getSubscriptionStore(client);
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
      const key = store.makeQueryKey(query, options);
      const entry = store.getCacheEntry<T>(key);

      // Own a copy: deltas are applied to the store's array in place, and the
      // cache entry's array is shared with every listener on the query.
      setState({
        data: entry.state.data && [...entry.state.data],
        isLoading: entry.state.status === "pending",
        error: entry.state.error ? toError(entry.state.error) : null,
      });

      const unsubscribe = entry.subscribe({
        onError: (error: unknown) =>
          setState({
            data: undefined,
            isLoading: false,
            error: toError(error),
          }),
        onReset: () =>
          batch(() => {
            setState("data", undefined);
            setState("isLoading", true);
            setState("error", null);
          }),
        onfulfilled: (nextData) =>
          setState({
            data: [...nextData],
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
              setState("data", reconcile(applyDeltaToRows(undefined, delta)));
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
        error: toError(error),
      });
    }
  });

  return state;
}
