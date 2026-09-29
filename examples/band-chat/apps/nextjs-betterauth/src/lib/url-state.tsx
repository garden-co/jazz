"use client";

import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useSyncExternalStore,
  type ReactNode,
} from "react";

// BandChat keeps its view state in the query string (`?room=…`, `?join=…`) so
// a room link can be shared. It uses the History API rather than a framework
// router so the same component runs in Next and in browser tests, where each
// mounted preview gets its own in-memory store instead.

interface ParamStore {
  get(name: string): string | null;
  set(name: string, value: string | null): void;
  subscribe(listener: () => void): () => void;
}

function historyStore(): ParamStore {
  const listeners = new Set<() => void>();
  return {
    get: (name) =>
      typeof window === "undefined" ? null : new URLSearchParams(window.location.search).get(name),
    set(name, value) {
      const url = new URL(window.location.href);
      if (value) url.searchParams.set(name, value);
      else url.searchParams.delete(name);
      window.history.replaceState(window.history.state, "", url);
      for (const listener of listeners) listener();
    },
    subscribe(listener) {
      listeners.add(listener);
      window.addEventListener("popstate", listener);
      return () => {
        listeners.delete(listener);
        window.removeEventListener("popstate", listener);
      };
    },
  };
}

export function memoryStore(initial: Record<string, string> = {}): ParamStore {
  const values = new Map(Object.entries(initial));
  const listeners = new Set<() => void>();
  return {
    get: (name) => values.get(name) ?? null,
    set(name, value) {
      if (value) values.set(name, value);
      else values.delete(name);
      for (const listener of listeners) listener();
    },
    subscribe(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
  };
}

const defaultStore = historyStore();
const ParamStoreContext = createContext<ParamStore>(defaultStore);

export function ParamStoreProvider({
  store,
  children,
}: {
  store: ParamStore;
  children: ReactNode;
}) {
  return <ParamStoreContext.Provider value={store}>{children}</ParamStoreContext.Provider>;
}

export function useSearchParam(name: string): [string | null, (value: string | null) => void] {
  const store = useContext(ParamStoreContext);
  const value = useSyncExternalStore(
    store.subscribe,
    () => store.get(name),
    () => null,
  );
  const update = useCallback((next: string | null) => store.set(name, next), [store, name]);
  return useMemo(() => [value, update], [value, update]);
}

/** A link that opens a room, or asks to join it for someone who is not a member yet. */
export function roomLink(roomId: string): string {
  const url = new URL(window.location.href);
  url.search = "";
  url.hash = "";
  url.searchParams.set("join", roomId);
  return url.toString();
}
