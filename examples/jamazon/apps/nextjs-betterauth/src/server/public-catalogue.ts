import { app } from "@/schema";
import type { CatalogueSnapshot } from "@/src/catalogue/snapshot";
import { backend } from "./backend";
import { ensureStore } from "./store";

/** How long a snapshot may take before the shopper is left to their own sync. */
const SNAPSHOT_TIMEOUT_MS = 1500;

/**
 * The public catalogue as the store's backend client sees it, for the first
 * paint of a catalogue page: categories, products and stock levels are
 * readable by everyone, so a shopper can browse before their own Jazz client
 * has opened and synced. Null when the backend can't answer within
 * `timeoutMs`; the page then waits for sync as before.
 */
export async function publicCatalogue(
  timeoutMs = SNAPSHOT_TIMEOUT_MS,
): Promise<CatalogueSnapshot | null> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timedOut = new Promise<null>((resolve) => {
    timer = setTimeout(() => {
      console.warn(`The public catalogue took longer than ${timeoutMs} ms; skipping the snapshot`);
      resolve(null);
    }, timeoutMs);
  });
  try {
    return await Promise.race([readCatalogue(), timedOut]);
  } finally {
    clearTimeout(timer);
  }
}

async function readCatalogue(): Promise<CatalogueSnapshot | null> {
  try {
    await ensureStore();
    const { db } = await backend();
    const [categories, products, stock] = await Promise.all([
      db.all(app.categories.orderBy("position", "asc")),
      db.all(app.products.orderBy("position", "asc")),
      db.all(app.stock),
    ]);
    return { categories, products, stock };
  } catch (error) {
    console.error("Could not read the public catalogue for the first paint", error);
    return null;
  }
}
