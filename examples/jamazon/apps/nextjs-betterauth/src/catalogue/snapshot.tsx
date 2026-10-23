"use client";

import { useSyncExternalStore } from "react";
import type { Category, Product, Stock } from "@/schema";

/** The public catalogue as the server read it, until the shopper's own sync has it. */
export type CatalogueSnapshot = { categories: Category[]; products: Product[]; stock: Stock[] };

/** How long the browser waits for a snapshot before relying on its own sync. */
const CLIENT_TIMEOUT_MS = 3000;

let snapshot: CatalogueSnapshot | null = null;
let requested = false;
const listeners = new Set<() => void>();

/** The pages that show the catalogue grid, and so have a use for the snapshot. */
export function isCataloguePath(pathname: string): boolean {
  return pathname === "/" || pathname.startsWith("/category/");
}

/**
 * Fetch the snapshot once per page load. Failures and timeouts are silent:
 * the page then waits for the shopper's own sync, as it did before.
 */
export function loadCatalogueSnapshot(): void {
  if (requested || typeof window === "undefined") return;
  requested = true;
  fetch("/api/catalogue", { signal: AbortSignal.timeout(CLIENT_TIMEOUT_MS) })
    .then((response) => (response.ok ? (response.json() as Promise<CatalogueSnapshot>) : null))
    .then((value) => {
      if (!value) return;
      snapshot = value;
      for (const listener of listeners) listener();
    })
    .catch(() => {});
}

// Start as soon as the bundle runs on a catalogue page, in parallel with
// opening Jazz, rather than after the first render.
if (typeof window !== "undefined" && isCataloguePath(window.location.pathname))
  loadCatalogueSnapshot();

/** The snapshot once it has arrived; null on the server and before then. */
export function useCatalogueSnapshot(): CatalogueSnapshot | null {
  return useSyncExternalStore(
    (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    () => snapshot,
    () => null,
  );
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

/**
 * Read options for live catalogue queries that the snapshot stands in for.
 * A local result that has rows arrives at once. An empty one is held back
 * until the server has answered, so "nothing synced yet" is never mistaken
 * for "nothing there". Until a query delivers, the snapshot is shown; once it
 * has, its result is the truth, even when it is empty (a removed product, sold
 * out stock). Offline, the local result arrives at once.
 */
export const UNTIL_SERVER_ANSWERS = { tier: "local-first-unless-empty" } as const;
