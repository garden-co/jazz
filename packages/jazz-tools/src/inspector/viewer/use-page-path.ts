import { CoID, RawCoValue } from "cojson";
import { useCallback, useEffect, useState } from "react";
import { PageInfo } from "./types.js";

const STORAGE_KEY = "jazz-inspector-paths";

export function usePagePath(defaultPath?: PageInfo[]) {
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

  const addPages = useCallback(
    (newPages: PageInfo[]) => {
      updatePath([...path, ...newPages]);
    },
    [path, updatePath],
  );

  const goToIndex = useCallback(
    (index: number) => {
      updatePath(path.slice(0, index + 1));
    },
    [path, updatePath],
  );

  const setPage = useCallback(
    (coId: CoID<RawCoValue>) => {
      updatePath([{ coId, name: "Root" }]);
    },
    [updatePath],
  );

  const goBack = useCallback(() => {
    if (path.length > 1) {
      updatePath(path.slice(0, path.length - 1));
    }
  }, [path, updatePath]);

  return {
    path,
    setPage,
    addPages,
    goToIndex,
    goBack,
  };
}
