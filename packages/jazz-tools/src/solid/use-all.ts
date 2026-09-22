import { batch, createEffect, onCleanup, type Accessor } from "solid-js";
import { createStore, produce, reconcile } from "solid-js/store";
import { applyDelta } from "../reconcile-array.js";
import type { QuerySettlementLevel } from "../shared/index.js";
import type { QueryBuilder, QueryOptions } from "../runtime/db.js";
import type { SubscriptionDelta } from "../runtime/subscription-manager.js";
import { useJazzClient } from "./provider.js";

export type UseAllResult<T extends { id: string }> = {
  data: T[] | undefined;
  isLoading: boolean;
  error: Error | null;
  highestSettledAt: QuerySettlementLevel;
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
    highestSettledAt: "unconfirmed",
  });

  createEffect(() => {
    const { query, options } = args();
    if (!query) {
      setState({
        data: undefined,
        isLoading: false,
        error: null,
        highestSettledAt: "unconfirmed",
      });
      return;
    }

    try {
      const key = store.makeQueryKey(query, options);
      const entry = store.getCacheEntry<T>(key);

      setState({
        data: entry.state.data,
        isLoading: entry.state.status === "pending",
        error: entry.state.error ? normalizeError(entry.state.error) : null,
        highestSettledAt: entry.state.highestSettledAt,
      });

      const unsubscribe = entry.subscribe({
        onError: (error: unknown) =>
          setState({
            data: undefined,
            isLoading: false,
            error: normalizeError(error),
            highestSettledAt: entry.state.highestSettledAt,
          }),
        onReset: () =>
          batch(() => {
            setState("data", undefined);
            setState("isLoading", true);
            setState("error", null);
            setState("highestSettledAt", "unconfirmed");
          }),
        onfulfilled: (nextData) =>
          setState({
            data: nextData,
            isLoading: false,
            error: null,
            highestSettledAt: entry.state.highestSettledAt,
          }),
        onDelta: (delta: SubscriptionDelta<T>) =>
          batch(() => {
            setState("highestSettledAt", entry.state.highestSettledAt);
            const metadataOnly =
              !delta.reset && delta.all === undefined && delta.delta.length === 0;
            if (!metadataOnly) {
              if (state.data) {
                setState(
                  "data",
                  produce((current) => {
                    if (!current) return;
                    applyDelta(current, delta);
                  }),
                );
              } else if (delta.reset) {
                setState("data", reconcile(delta.all));
              } else {
                const current: T[] = [];
                applyDelta(current, delta);
                setState("data", reconcile(current));
              }
            }
            setState("isLoading", entry.state.status === "pending");
            setState("error", null);
          }),
      });

      onCleanup(unsubscribe);
    } catch (error) {
      setState({
        data: undefined,
        isLoading: false,
        error: normalizeError(error),
        highestSettledAt: "unconfirmed",
      });
    }
  });

  return state;
}

function normalizeError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
