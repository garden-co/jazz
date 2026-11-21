"use client";

// The React side of the data layer: the provider (Better Auth session, query
// cache, live socket), live reads (`useAll`, `useOne`) and writes (`useDb`).
import { createContext, useContext, useEffect, type ReactNode } from "react";
import { QueryClientProvider } from "@tanstack/react-query";
import type { QuerySpec, TableName } from "../../schema";
import { authClient } from "../lib/auth-client";
import { call, post } from "./api";
import { connectLive, queryClient, useQuery, type Live } from "./live";
import { revive, type Query } from "./query";

const load = (spec: QuerySpec) => (signal?: AbortSignal) => post<unknown[]>("/api/query", spec, signal).then(revive);

function live<T extends TableName, R>(query: Query<T, R>): Live<R[]> {
  return {
    key: ["query", query.spec],
    load: load(query.spec),
    empty: [],
    watch: () => [[query.topic, query.tables]],
  };
}

/** A write's answer from the server: `wait()` resolves once it committed, or rejects. */
export type Write = { wait: (_options?: unknown) => Promise<void> };
const sent = (promise: Promise<unknown>): Write => {
  const done = promise.then(() => undefined);
  done.catch(() => {}); // reported by whoever waits
  return { wait: () => done };
};

/** The writes and one-off reads, with call shapes like the screens use. */
export const db = {
  insert<T extends TableName>(table: Query<T, any>, row: Record<string, unknown>, options?: { id?: string }) {
    const id = options?.id ?? crypto.randomUUID();
    return { id, value: { ...row, id }, ...sent(call("insert", { table: table.spec.table, id, row })) };
  },
  update<T extends TableName>(table: Query<T, any>, id: string, row: Record<string, unknown>) {
    return sent(call("update", { table: table.spec.table, id, row }));
  },
  delete<T extends TableName>(table: Query<T, any>, id: string) {
    return sent(call("delete", { table: table.spec.table, id }));
  },
  all<T extends TableName, R>(query: Query<T, R>) {
    return load(query.spec)() as Promise<R[]>;
  },
  async one<T extends TableName, R>(query: Query<T, R>) {
    return (await this.all(query.limit(1)))[0];
  },
};
export type Db = typeof db;
export const useDb = () => db;

/** A live list: refetched when the server says one of its tables changed. */
export function useAll<T extends TableName, R>(query: Query<T, R> | undefined) {
  const [rows, result] = useQuery(query ? live(query) : undefined);
  return { data: result.type === "complete" ? rows : undefined, isLoading: Boolean(query) && result.type !== "complete", db };
}

/** The first row of a live query. */
export function useOne<T extends TableName, R>(query: Query<T, R>) {
  const { data, isLoading } = useAll(query.limit(1));
  return { data: data?.[0], isLoading, db };
}

type Session = { user: string } | null;
const SessionContext = createContext<Session>(null);
/** The signed-in account (the Better Auth user id). */
export const useSession = () => useContext(SessionContext);

/**
 * Signed out: `signedOut`. Signed in: the app, with the query cache and the
 * live socket. Better Auth's session is the only state.
 */
export function DataProvider({
  signedOut,
  loading,
  error,
  children,
}: {
  signedOut: ReactNode;
  loading: ReactNode;
  error: (state: { error?: Error; retry: () => void }) => ReactNode;
  children: ReactNode;
}) {
  const { data, isPending, error: failure, refetch } = authClient.useSession();
  const user = data?.user.id;
  useEffect(() => {
    if (user) connectLive();
  }, [user]);
  if (isPending) return <>{loading}</>;
  if (failure) return <>{error({ error: new Error(failure.message), retry: () => void refetch() })}</>;
  if (!user) return <>{signedOut}</>;
  return (
    <QueryClientProvider client={queryClient}>
      <SessionContext.Provider value={{ user }}>{children}</SessionContext.Provider>
    </QueryClientProvider>
  );
}

/** Asks Better Auth again who is signed in (after signing in or out). */
export function useAuthRefresh() {
  const { refetch } = authClient.useSession();
  return { refresh: async () => void (await refetch()) };
}
