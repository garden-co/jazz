import type { Db } from "jazz-tools";
import { app } from "@/schema";
import { CATEGORIES, PRODUCTS } from "@/src/catalogue/catalogue";
import { ids } from "@/src/lib/ids";

/**
 * Seed the synthetic catalogue. Deterministic ids make this idempotent:
 * categories and products are upserted (the code is their source of truth),
 * while stock is only created when missing, so a restart never refills
 * shelves that orders have emptied.
 */
export async function seedCatalogue(db: Db): Promise<void> {
  const existingStock = new Set(
    (await db.all(app.stock, { tier: "remote" })).map((row) => row.productId),
  );
  const write = await db.transaction((tx) => {
    CATEGORIES.forEach((category, position) =>
      tx.upsert(app.categories, ids.category(category.slug), { ...category, position }),
    );
    PRODUCTS.forEach((product, position) => {
      const productId = ids.product(product.sku);
      const category = CATEGORIES.find((c) => c.slug === product.category)!;
      tx.upsert(app.products, productId, {
        sku: product.sku,
        slug: product.slug,
        name: product.name,
        brand: product.brand,
        categoryId: ids.category(product.category),
        priceCents: product.priceCents,
        summary: product.summary,
        description: product.description,
        specs: product.specs,
        hue: product.hue,
        art: product.art,
        searchText: [product.name, product.brand, category.name, product.summary]
          .join(" ")
          .toLowerCase(),
        position,
      });
      if (!existingStock.has(productId))
        tx.upsert(app.stock, ids.stock(product.sku), { productId, onHand: product.onHand });
    });
  });
  await write.wait({ tier: "global" });
}
