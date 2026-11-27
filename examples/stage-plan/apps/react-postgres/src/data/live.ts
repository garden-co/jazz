// The app's data layer: TanStack Query caches REST reads; the live socket
// invalidates the queries that watch a changed table, which refetch. A query
// waits for its topics' subscriptions first, so no change can slip in between
// its fetch and its subscription.
import { useEffect, useSyncExternalStore } from "react";
import { QueryClient, useQuery as useTanStackQuery, useQueryClient, type Query } from "@tanstack/react-query";
import type { ChangeEvent } from "../../shared/types.js";
import { get } from "./api.js";
import { onLiveState, startLive, subscribe, topicSettled } from "./socket.js";

/** Which tables under which topic a query's result depends on. */
export type Watch = [topic: string, tables: string[]];

export type Live<T> = {
  key: readonly unknown[];
  path: string;
  empty: T;
  watch: (data: T) => Watch[];
};

export const queryClient = new QueryClient({
  defaultOptions: { queries: { staleTime: Infinity, refetchOnWindowFocus: false, retry: 1 } },
});

const watching = (query: Query, test: (watch: Watch) => boolean) => {
  const watch = query.meta?.watch as Live<unknown>["watch"] | undefined;
  return Boolean(watch?.(query.state.data ?? query.meta?.empty).some(test));
};

function invalidate(event: ChangeEvent) {
  void queryClient.invalidateQueries({
    predicate: (query) =>
      watching(query, ([topic, tables]) =>
        topic === event.topic && (event.tables.includes("*") || event.tables.some((t) => tables.includes(t))),
      ),
  });
}

/** Opens the live socket, once the session cookie is set. */
export const connectLive = () =>
  startLive({
  onChange: invalidate,
  // After a reconnect, changes may have been missed: refetch everything.
  onReconnect: () => void queryClient.invalidateQueries(),
  // A query that already has data (its topics came from that data) may have
  // missed a change before this subscription started: refetch it once.
  onSubscribed: (topic) =>
    void queryClient.invalidateQueries({
      predicate: (query) => query.state.dataUpdatedAt > 0 && watching(query, ([t]) => t === topic),
    }),
  });

export const useDb = useQueryClient;
export type Db = QueryClient;

/** Connection state of the live socket. */
export function useConnected() {
  return useSyncExternalStore(onLiveState, () => topicSettled("me"));
}

/** A live query: `[data, { type }]`, with `type` "complete" once the server answered. */
export function useQuery<T>(live: Live<T>): [T, { type: "unknown" | "complete" }] {
  const topics = [...new Set(live.watch(live.empty).map(([topic]) => topic))];
  const ready = useSyncExternalStore(onLiveState, () => topics.every(topicSettled));
  const query = useTanStackQuery({
    queryKey: live.key,
    queryFn: ({ signal }) => get<T>(live.path, signal),
    meta: { watch: live.watch, empty: live.empty },
    enabled: ready,
  });
  const data = query.data ?? live.empty;
  const dataTopics = [...new Set(live.watch(data).map(([topic]) => topic))].sort().join(" ");
  useEffect(() => {
    const topicList = dataTopics.split(" ").filter(Boolean);
    const unsubscribers = topicList.map(subscribe);
    return () => unsubscribers.forEach((unsubscribe) => unsubscribe());
  }, [dataTopics]);
  return [data, { type: query.isSuccess ? "complete" : "unknown" }];
}
