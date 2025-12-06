import React, {
  ReactNode,
  useState,
  useCallback,
  useEffect,
  useMemo,
} from "react";
import { Router, RouterContext } from "./context.js";
import { PageInfo } from "../viewer/types.js";
import { CoID, RawCoValue } from "cojson";

export function HashRouterProvider({
  children,
  defaultPath,
}: {
  children: ReactNode;
  defaultPath?: PageInfo[];
}) {
  const [path, setPath] = useState<PageInfo[]>(() => {
    if (typeof window === "undefined") return defaultPath || [];
    const hash = window.location.hash.slice(2); // Remove '#/'
    if (hash) {
      try {
        return decodePathFromHash(hash);
      } catch (e) {
        console.error("Failed to parse hash:", e);
      }
    }
    return defaultPath || [];
  });

  const updatePath = useCallback((newPath: PageInfo[]) => {
    setPath(newPath);
    if (typeof window !== "undefined") {
      const hash = encodePathToHash(newPath);
      window.location.assign(`#/${hash}`);
    }
  }, []);

  // useEffect(() => {
  //   if (typeof window === "undefined") return;

  //   const handleHashChange = () => {
  //     const hash = window.location.hash.slice(2);
  //     const currentPath = encodePathToHash(path);

  //     if(hash === currentPath) return;

  //     if (hash) {
  //       try {
  //         const newPath = decodePathFromHash(hash);
  //         setPath(newPath);
  //       } catch (e) {
  //         console.error("Failed to parse hash:", e);
  //       }
  //     } else if (defaultPath) {
  //       setPath(defaultPath);
  //     }
  //   };

  //   window.addEventListener("hashchange", handleHashChange);
  //   return () => window.removeEventListener("hashchange", handleHashChange);
  // }, [path, defaultPath]);

  useEffect(() => {
    if (
      defaultPath &&
      encodePathToHash(path) !== encodePathToHash(defaultPath)
    ) {
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

function encodePathToHash(path: PageInfo[]): string {
  return path
    .map((page) => {
      if (page.name && page.name !== "Root") {
        return `${page.coId}:${encodeURIComponent(page.name)}`;
      }
      return page.coId;
    })
    .join("/");
}

function decodePathFromHash(hash: string): PageInfo[] {
  return hash.split("/").map((segment) => {
    const [coId, encodedName] = segment.split(":");
    return {
      coId,
      name: encodedName ? decodeURIComponent(encodedName) : undefined,
    } as PageInfo;
  });
}
