import { shallowRef, toValue, watchEffect, type MaybeRefOrGetter, type ShallowRef } from "vue";
import type { QueryBuilder } from "../runtime/db.js";
import type { SubscriptionDelta } from "../runtime/subscription-manager.js";
import type { CacheEntryHandle, UseAllState } from "../subscriptions-orchestrator.js";
import { useJazzClient } from "./provider.js";

function applyEntryState<T extends { id: string }>(
  state: UseAllState<T>,
  data: ShallowRef<T[] | undefined>,
): void {
  if (state.status === "fulfilled") {
    data.value = state.data;
  } else {
    data.value = undefined;
  }
}

function subscribeToEntry<T extends { id: string }>(
  entry: CacheEntryHandle<T>,
  data: ShallowRef<T[] | undefined>,
): () => void {
  applyEntryState(entry.state, data);

  return entry.subscribe({
    onfulfilled: (nextData) => {
      data.value = nextData;
    },
    onDelta: (delta: SubscriptionDelta<T>) => {
      data.value = delta.all;
    },
    onError: () => {
      data.value = undefined;
    },
  });
}

export function useAll<T extends { id: string }>(
  query: MaybeRefOrGetter<QueryBuilder<T>>,
): ShallowRef<T[] | undefined> {
  const { manager } = useJazzClient();
  const data = shallowRef<T[] | undefined>(undefined);

  watchEffect((onCleanup) => {
    const resolvedQuery = toValue(query);
    const key = manager.makeQueryKey(resolvedQuery);
    const entry = manager.getCacheEntry<T>(key);
    const unsubscribe = subscribeToEntry(entry, data);

    onCleanup(() => {
      unsubscribe();
    });
  });

  return data;
}
