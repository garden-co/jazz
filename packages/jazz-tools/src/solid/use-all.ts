import { batch, createEffect, createSignal, onCleanup, type Accessor } from "solid-js";
import { createStore, produce, reconcile } from "solid-js/store";
import { applyDelta, applyDeltaToRows } from "../reconcile-array.js";
import type { Db, QueryBuilder, QueryOptions } from "../runtime/db.js";
import type { SubscriptionDelta } from "../runtime/subscription-manager.js";
import { getSubscriptionStore } from "../subscription-store-internal.js";
import { useJazzClient } from "./provider.js";
import { toError } from "../runtime/util.js";
import { unreadableRows, type UnreadableRow } from "../encryption/unreadable.js";

const NO_UNREADABLE: readonly UnreadableRow[] = Object.freeze([]);

export type UseAllResult<T extends { id: string }> = {
  data: T[] | undefined;
  isLoading: boolean;
  error: Error | null;
  /** The {@link Db} handle for writes, so components don't need a separate `useDb()`. */
  readonly db: Db;
  /**
   * The rows `data` leaves out because this device could not decrypt them
   * (empty when nothing was left out). Reactive: current with every delivery.
   */
  readonly unreadable: readonly UnreadableRow[];
};

type UseAllState<T extends { id: string }> = Omit<UseAllResult<T>, "db" | "unreadable">;

export function useAll<T extends { id: string }>(
  args: Accessor<{
    query: QueryBuilder<T> | undefined;
    options?: QueryOptions | undefined;
  }>,
): UseAllResult<T> {
  const client = useJazzClient();
  const store = getSubscriptionStore(client);
  const [state, setState] = createStore<UseAllState<T>>({
    data: undefined,
    isLoading: false,
    error: null,
  });
  // Kept outside the store: the list is replaced whole, never merged, and
  // `unreadableRows` would not see through a store proxy anyway.
  const [unreadable, setUnreadable] = createSignal<readonly UnreadableRow[]>(NO_UNREADABLE);

  createEffect(() => {
    const { query, options } = args();
    if (!query) {
      setUnreadable(NO_UNREADABLE);
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
      // The cache entry's array carries the list of rows its result leaves out.
      const syncUnreadable = () =>
        setUnreadable(
          entry.state.status === "fulfilled" ? unreadableRows(entry.state.data) : NO_UNREADABLE,
        );
      syncUnreadable();

      // Own a copy: deltas are applied to the store's array in place, and the
      // cache entry's array is shared with every listener on the query.
      setState({
        data: entry.state.data && [...entry.state.data],
        isLoading: entry.state.status === "pending",
        error: entry.state.error ? toError(entry.state.error) : null,
      });

      const unsubscribe = entry.subscribe({
        onError: (error: unknown) =>
          batch(() => {
            setUnreadable(NO_UNREADABLE);
            setState({
              data: undefined,
              isLoading: false,
              error: toError(error),
            });
          }),
        onReset: () =>
          batch(() => {
            setUnreadable(NO_UNREADABLE);
            setState("data", undefined);
            setState("isLoading", true);
            setState("error", null);
          }),
        onfulfilled: (nextData) =>
          batch(() => {
            setUnreadable(unreadableRows(nextData));
            setState({
              data: [...nextData],
              isLoading: false,
              error: null,
            });
          }),
        onDelta: (delta: SubscriptionDelta<T>) =>
          batch(() => {
            syncUnreadable();
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
      setUnreadable(NO_UNREADABLE);
      setState({
        data: undefined,
        isLoading: false,
        error: toError(error),
      });
    }
  });

  const db = client.db as Db;
  return {
    get data() {
      return state.data;
    },
    get isLoading() {
      return state.isLoading;
    },
    get error() {
      return state.error;
    },
    get unreadable() {
      return unreadable();
    },
    db,
  };
}
