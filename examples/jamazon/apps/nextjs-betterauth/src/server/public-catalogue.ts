import { app } from "@/schema";
import type { CatalogueSnapshot } from "@/src/catalogue/snapshot";
import { backend } from "./backend";
import { ensureStore } from "./store";

/**
 * The public catalogue as the store's backend client sees it, for the first
 * paint of a page load: categories, products and stock levels are readable by
 * everyone, so a shopper can browse before their own Jazz client has opened
 * and synced. Null when the backend can't answer; the page then waits for
 * sync as before.
 */
export async function publicCatalogue(): Promise<CatalogueSnapshot | null> {
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
