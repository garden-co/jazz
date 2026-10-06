import { useSyncExternalStore } from "react";

/**
 * Invite joins this tab has committed locally and sent to the server. The
 * show page opens on the local commit; until the server accepts the
 * membership (and syncs the show), it shows "Joining the crew" instead of
 * "not on this crew", and if the server turns the invite down it says so.
 */
export type JoinState = "pending" | "failed";

// Kept in memory only, so a reload while a join is still pending forgets it.
// The membership itself is a durable local write that is still sent to the
// server after the reload; only the page's wording changes: until the server
// accepts it and the show syncs, the page says "not on this crew" instead of
// "Joining the crew", and a later rejection is not reported. The window is
// the server's round trip for the join, and the page fixes itself once the
// show arrives, so it is not worth persisting.

const joins = new Map<string, JoinState>();
const listeners = new Set<() => void>();

function set(showId: string, state: JoinState | undefined) {
  if (state) joins.set(showId, state);
  else joins.delete(showId);
  for (const listener of listeners) listener();
}

export function trackJoin(showId: string, accepted: Promise<void>) {
  set(showId, "pending");
  accepted.then(
    () => set(showId, undefined),
    (error: unknown) => {
      console.error("The server did not accept the invite", error);
      set(showId, "failed");
    },
  );
}

export function useJoinState(showId: string): JoinState | undefined {
  return useSyncExternalStore(
    (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    () => joins.get(showId),
    () => undefined,
  );
}
