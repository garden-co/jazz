"use client";

import { TocContext } from "@/lib/TocContext";
import type { Toc, TocEntry } from "@stefanprobst/rehype-extract-toc";
import { useState } from "react";

export function TocProvider({ children }: { children: React.ReactNode }) {
  const [tocItems, setTocItems] = useState<Toc>();

  return (
    <TocContext.Provider value={{ tocItems, setTocItems }}>
      {children}
    </TocContext.Provider>
  );
}
