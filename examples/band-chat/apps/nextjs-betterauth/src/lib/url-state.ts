import { useCallback, useEffect, useState } from "react";

// BandChat keeps its view state in the query string (`?room=…`, `?join=…`)
// so a room link can be shared. It deliberately uses the History API instead
// of a framework router so the same component runs in Next and in browser tests.

function read(name: string): string | null {
  if (typeof window === "undefined") return null;
  return new URLSearchParams(window.location.search).get(name);
}

const listeners = new Set<() => void>();

export function useSearchParam(name: string): [string | null, (value: string | null) => void] {
  const [value, setValue] = useState(() => read(name));
  useEffect(() => {
    const sync = () => setValue(read(name));
    listeners.add(sync);
    window.addEventListener("popstate", sync);
    return () => {
      listeners.delete(sync);
      window.removeEventListener("popstate", sync);
    };
  }, [name]);
  const update = useCallback(
    (next: string | null) => {
      const url = new URL(window.location.href);
      if (next) url.searchParams.set(name, next);
      else url.searchParams.delete(name);
      window.history.replaceState(window.history.state, "", url);
      for (const listener of listeners) listener();
    },
    [name],
  );
  return [value, update];
}

/** A link that opens a room, or asks to join it for someone who is not a member yet. */
export function roomLink(roomId: string): string {
  const url = new URL(window.location.href);
  url.search = "";
  url.hash = "";
  url.searchParams.set("join", roomId);
  return url.toString();
}
