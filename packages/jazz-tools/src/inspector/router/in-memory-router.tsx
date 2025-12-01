import { ReactNode, useCallback, useEffect, useMemo, useState } from "react";
import type { CoID, RawCoValue } from "cojson";
import { Router, RouterContext } from "./context.js";
import { PageInfo } from "../viewer/types.js";

const STORAGE_KEY = "jazz-inspector-paths";

export function InMemoryRouterProvider({
  children,
  defaultPath,
}: {
  children: ReactNode;
  defaultPath?: PageInfo[];
}) {
  const [path, setPath] = useState<PageInfo[]>(() => {
    if (typeof window === "undefined") return [];
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored) {
      try {
        return JSON.parse(stored);
      } catch (e) {
        console.warn("Failed to parse stored path:", e);
      }
    }
    return defaultPath || [];
  });

  const updatePath = useCallback((newPath: PageInfo[]) => {
    setPath(newPath);
    localStorage.setItem(STORAGE_KEY, JSON.stringify(newPath));
  }, []);

  useEffect(() => {
    if (defaultPath && JSON.stringify(path) !== JSON.stringify(defaultPath)) {
      updatePath(defaultPath);
    }
  }, [defaultPath, path, updatePath]);

  const router: Router = useMemo(() => {
    const addPages = (newPages: PageInfo[]) => {
      updatePath([...path, ...newPages]);
    };

    const goToIndex = (index: number) => {
      updatePath(path.slice(0, index + 1));
    };

    const setPage = (coId: CoID<RawCoValue>) => {
      updatePath([{ coId, name: "Root" }]);
    };

    const goBack = () => {
      updatePath(path.slice(0, path.length - 1));
    };

    return {
      path,
      addPages,
      goToIndex,
      setPage,
      goBack,
    };
  }, [path, updatePath]);

  return (
    <RouterContext.Provider value={router}>{children}</RouterContext.Provider>
  );
}
