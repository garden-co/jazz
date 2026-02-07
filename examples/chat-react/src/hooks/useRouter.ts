import { useSyncExternalStore } from "react";

function subscribe(callback: () => void) {
  window.addEventListener("popstate", callback);
  window.addEventListener("hashchange", callback);
  return () => {
    window.removeEventListener("popstate", callback);
    window.removeEventListener("hashchange", callback);
  };
}

export function useRouter() {
  const path = useSyncExternalStore(
    subscribe,
    () => window.location.hash.slice(1) || window.location.pathname || "/",
  );

  return {
    path,
    isActive: (p: string, exact = true) =>
      exact ? path === p : path.startsWith(p),
    navigate: (href: string) => navigate(href),
  };
}

export function navigate(href: string) {
  if (href.startsWith("#")) {
    window.location.hash = href;
  } else {
    window.history.pushState({}, "", href);
    window.dispatchEvent(new PopStateEvent("popstate"));
  }
}
