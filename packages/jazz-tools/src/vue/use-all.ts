import { ref, toValue, watchEffect, type MaybeRefOrGetter, type Ref } from "vue";
import type { QueryBuilder, QueryOptions } from "../runtime/db.js";
import type { SubscriptionDelta } from "../runtime/subscription-manager.js";
import { applyDelta } from "../reconcile-array.js";
import type { CacheEntryHandle, UseAllState } from "../subscriptions-orchestrator.js";
import { useJazzClient } from "./provider.js";

function applyEntryState<T extends { id: string }>(
  state: UseAllState<T>,
  data: Ref<T[] | undefined>,
): void {
  if (state.status === "fulfilled") {
    data.value = state.data;
  } else {
    data.value = undefined;
  }
}

function subscribeToEntry<T extends { id: string }>(
  entry: CacheEntryHandle<T>,
  data: Ref<T[] | undefined>,
): () => void {
  applyEntryState(entry.state, data);

  return entry.subscribe({
    onfulfilled: (nextData) => {
      data.value = nextData;
    },
    onDelta: (delta: SubscriptionDelta<T>) => {
      if (data.value) {
        applyDelta(data.value, delta);
      } else {
        data.value = delta.all;
      }
    },
    onError: () => {
      data.value = undefined;
    },
  });
}

export function useAll<T extends { id: string }>(
  query: MaybeRefOrGetter<QueryBuilder<T> | undefined>,
  options?: MaybeRefOrGetter<QueryOptions | undefined>,
): Ref<T[] | undefined> {
  const { manager } = useJazzClient();
  const data = ref<T[] | undefined>(undefined) as Ref<T[] | undefined>;

  watchEffect((onCleanup) => {
    const resolvedQuery = toValue(query);
    if (!resolvedQuery) {
      data.value = undefined;
      return;
    }
    const resolvedOptions = toValue(options);
    const key = manager.makeQueryKey(resolvedQuery, resolvedOptions);
    const entry = manager.getCacheEntry<T>(key);
    const unsubscribe = subscribeToEntry(entry, data);

    onCleanup(() => {
      unsubscribe();
    });
  });

  return data;
}
