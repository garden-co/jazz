"use client";

import { useTocItems } from "@/lib/TocContext";
import type { TocEntry } from "@stefanprobst/rehype-extract-toc";
import { useEffect } from "react";

export function TocItemsSetter({ items }: { items: TocEntry[] | undefined }) {
  const { setTocItems } = useTocItems();

  useEffect(() => {
    setTocItems(items);
  }, [items, setTocItems]);

  return null;
}
