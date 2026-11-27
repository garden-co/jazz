// The app's data layer: TanStack Query caches REST reads; the live socket
// invalidates the queries that watch a changed table, which refetch. A query
// waits for its topics' subscriptions first, so no change can slip in between
// its fetch and its subscription.
import { useEffect, useSyncExternalStore } from "react";
import { QueryClient, useQuery as useTanStackQuery, type Query } from "@tanstack/react-query";
import type { ChangeEvent } from "../../schema";
import { onLiveState, startLive, subscribe, topicSettled } from "./socket";

/** Which tables under which topic a query's result depends on. */
export type Watch = [topic: string, tables: string[]];

export type Live<T> = {
  key: readonly unknown[];
  /** Fetches the result (a REST read). */
  load: (signal?: AbortSignal) => Promise<unknown>;
  empty: T;
  watch: (data: T) => Watch[];
  /** Picks this component's part of a shared result (e.g. one lane of a pattern's steps). */
  select?: (data: any) => T;
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
  onSubscribed: (topics) =>
    void queryClient.invalidateQueries({
      predicate: (query) => query.state.dataUpdatedAt > 0 && watching(query, ([t]) => topics.includes(t)),
    }),
  });


const none: Live<undefined> = { key: ["none"], load: async () => undefined, empty: undefined, watch: () => [] };

/** A live query: `[data, { type }]`, with `type` "complete" once the server answered. Pass undefined to skip. */
export function useQuery<T>(live: Live<T>): [T, { type: "unknown" | "complete" }];
export function useQuery<T>(live: Live<T> | undefined): [T | undefined, { type: "unknown" | "complete" }];
export function useQuery<T>(maybe: Live<T> | undefined): [T | undefined, { type: "unknown" | "complete" }] {
  const live = (maybe ?? none) as Live<T>;
  const topics = [...new Set(live.watch(live.empty).map(([topic]) => topic))];
  const ready = useSyncExternalStore(onLiveState, () => topics.every(topicSettled), () => false);
  const query = useTanStackQuery({
    queryKey: live.key,
    queryFn: ({ signal }) => live.load(signal),
    meta: { watch: live.watch, empty: live.empty },
    enabled: Boolean(maybe) && ready,
    select: live.select ?? ((data) => data as T),
  });
  const data = query.data ?? live.empty;
  const dataTopics = [...new Set(live.watch(data).map(([topic]) => topic))].sort().join(" ");
  useEffect(() => {
    const unsubscribers = dataTopics.split(" ").filter(Boolean).map(subscribe);
    return () => unsubscribers.forEach((unsubscribe) => unsubscribe());
  }, [dataTopics]);
  return [data, { type: query.isSuccess ? "complete" : "unknown" }];
}
