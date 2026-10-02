"use client";

import { createContext, useContext, type ReactNode } from "react";
import type { Category, Product, Stock } from "@/schema";

/** The public catalogue as rendered by the server, until the local copy has it. */
export type CatalogueSnapshot = { categories: Category[]; products: Product[]; stock: Stock[] };

const SnapshotContext = createContext<CatalogueSnapshot | null>(null);

export function CatalogueSnapshotProvider({
  snapshot,
  children,
}: {
  snapshot: CatalogueSnapshot | null;
  children: ReactNode;
}) {
  return <SnapshotContext.Provider value={snapshot}>{children}</SnapshotContext.Provider>;
}

export function useCatalogueSnapshot(): CatalogueSnapshot | null {
  return useContext(SnapshotContext);
}

/** The snapshot's products in one category and/or matching a search term, in shelf order. */
export function snapshotProducts(
  snapshot: CatalogueSnapshot,
  { categoryId, term }: { categoryId?: string; term?: string },
): Product[] {
  return snapshot.products.filter(
    (product) =>
      (!categoryId || product.categoryId === categoryId) &&
      (!term || product.searchText.includes(term)),
  );
}
