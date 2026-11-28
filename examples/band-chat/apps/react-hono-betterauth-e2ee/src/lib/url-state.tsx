import { useCallback, useMemo, useSyncExternalStore } from "react";

// BandChat keeps its view state in the query string (`?room=…`, `?join=…`) so
// a room link can be shared. The History API keeps the URL in step with the view.
interface ParamStore {
  get(name: string): string | null;
  set(name: string, value: string | null): void;
  subscribe(listener: () => void): () => void;
}

function historyStore(): ParamStore {
  const listeners = new Set<() => void>();
  return {
    get: (name: string) =>
      typeof window === "undefined" ? null : new URLSearchParams(window.location.search).get(name),
    set(name, value: string | null) {
      const url = new URL(window.location.href);
      if (value) url.searchParams.set(name, value);
      else url.searchParams.delete(name);
      window.history.replaceState(window.history.state, "", url);
      for (const listener of listeners) listener();
    },
    subscribe(listener: () => void) {
      listeners.add(listener);
      window.addEventListener("popstate", listener);
      return () => {
        listeners.delete(listener);
        window.removeEventListener("popstate", listener);
      };
    },
  };
}

const store = historyStore();

export function useSearchParam(name: string): [string | null, (value: string | null) => void] {
  const value = useSyncExternalStore(
    store.subscribe,
    () => store.get(name),
    () => null,
  );
  const update = useCallback((next: string | null) => store.set(name, next), [store, name]);
  return useMemo(() => [value, update], [value, update]);
}

/** A link that opens a room or asks someone who is not a member to join it. */
export function roomLink(roomId: string): string {
  const url = new URL(window.location.href);
  url.search = "";
  url.hash = "";
  url.searchParams.set("join", roomId);
  return url.toString();
}
