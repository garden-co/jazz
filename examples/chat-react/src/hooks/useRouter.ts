import {
  createContext,
  createElement,
  useContext,
  useMemo,
  useSyncExternalStore,
  useState,
} from "react";
import type { ReactNode } from "react";

interface RouterScopeValue {
  path: string;
  navigate: (href: string) => void;
}

const RouterScopeContext = createContext<RouterScopeValue | null>(null);

function routePathFromHref(href: string): string {
  if (href.startsWith("/#")) return href.slice(2) || "/";
  if (href.startsWith("#")) return href.slice(1) || "/";
  return href || "/";
}

export function RouterScope({
  initialPath = "/",
  children,
}: {
  initialPath?: string;
  children: ReactNode;
}) {
  const [path, setPath] = useState(() => routePathFromHref(initialPath));
  const value = useMemo(
    () => ({
      path,
      navigate: (href: string) => setPath(routePathFromHref(href)),
    }),
    [path],
  );

  return createElement(RouterScopeContext.Provider, { value }, children);
}

function subscribe(callback: () => void) {
  window.addEventListener("popstate", callback);
  window.addEventListener("hashchange", callback);
  return () => {
    window.removeEventListener("popstate", callback);
    window.removeEventListener("hashchange", callback);
  };
}

export function useRouter() {
  const scopedRouter = useContext(RouterScopeContext);
  const path = useSyncExternalStore(
    subscribe,
    () => window.location.hash.slice(1) || window.location.pathname || "/",
  );
  const currentPath = scopedRouter?.path ?? path;

  return {
    path: currentPath,
    isActive: (p: string, exact = true) => (exact ? currentPath === p : currentPath.startsWith(p)),
    navigate: scopedRouter?.navigate ?? ((href: string) => navigate(href)),
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
