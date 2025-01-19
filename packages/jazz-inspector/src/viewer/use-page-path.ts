import { CoID, RawCoValue } from "cojson";
import { useCallback, useEffect, useState } from "react";
import { PageInfo } from "./types.ts";

export function usePagePath(defaultPath?: PageInfo[]) {
  const [path, setPath] = useState<PageInfo[]>([]);

  const updatePath = useCallback((newPath: PageInfo[]) => {
    setPath(newPath);
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
