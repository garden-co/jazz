import { CoID, RawCoValue } from "cojson";
import { PageInfo } from "../viewer/types.js";
import { createContext, useContext } from "react";

export interface Router {
  path: PageInfo[];
  setPage: (coId: CoID<RawCoValue>) => void;
  addPages: (newPages: PageInfo[]) => void;
  goToIndex: (index: number) => void;
  goBack: () => void;
}

export const RouterContext = createContext<Router | null>(null);

export function useRouter(): Router {
  const context = useContext(RouterContext);
  if (!context) {
    throw new Error("useRouter must be used within a RouterProvider");
  }
  return context;
}
