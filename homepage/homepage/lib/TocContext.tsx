import type { Toc } from "@stefanprobst/rehype-extract-toc";
import { createContext, useContext } from "react";

type TocContextType = {
  tocItems: Toc | undefined;
  setTocItems: (items: Toc | undefined) => void;
};

export const TocContext = createContext<TocContextType | undefined>(undefined);

export function useTocItems() {
  const context = useContext(TocContext);
  if (context === undefined) {
    throw new Error("useTocItems must be used within a TocProvider");
  }
  return context;
}
