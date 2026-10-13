import { useSyncExternalStore } from "react";

export type Route =
  | { page: "shows" }
  | { page: "checklist" }
  | { page: "show"; showId: string; tab: ShowTab; taskId?: string }
  | { page: "join"; showId: string; code: string };

export type ShowTab = "board" | "crew" | "activity";

/** Parses hash routes such as `#/shows/<id>/tasks/<taskId>`. */
export function parseRoute(hash: string): Route {
  const parts = hash.replace(/^#\/?/, "").split("/").filter(Boolean).map(decodeURIComponent);
  if (parts[0] === "checklist") return { page: "checklist" };
  if (parts[0] === "join" && parts[1] && parts[2]) {
    return { page: "join", showId: parts[1], code: parts[2] };
  }
  if (parts[0] === "shows" && parts[1]) {
    if (parts[2] === "tasks" && parts[3]) {
      return { page: "show", showId: parts[1], tab: "board", taskId: parts[3] };
    }
    const tab = parts[2] === "crew" || parts[2] === "activity" ? parts[2] : "board";
    return { page: "show", showId: parts[1], tab };
  }
  return { page: "shows" };
}

export const href = {
  shows: () => "#/",
  checklist: () => "#/checklist",
  show: (showId: string, tab: ShowTab = "board") =>
    tab === "board" ? `#/shows/${showId}` : `#/shows/${showId}/${tab}`,
  task: (showId: string, taskId: string) => `#/shows/${showId}/tasks/${taskId}`,
  // The code lives in the URL fragment, so it never reaches a server log.
  join: (showId: string, code: string) => `#/join/${showId}/${code}`,
};

export function navigate(to: string) {
  window.location.hash = to.replace(/^#/, "");
}

function subscribe(onChange: () => void) {
  window.addEventListener("hashchange", onChange);
  return () => window.removeEventListener("hashchange", onChange);
}

export function useRoute(): Route {
  const hash = useSyncExternalStore(subscribe, () => window.location.hash);
  return parseRoute(hash);
}
