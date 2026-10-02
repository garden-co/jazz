"use client";

import { EmptyState } from "@astryxdesign/core/EmptyState";
import { Grid } from "@astryxdesign/core/Grid";
import { Icon } from "@astryxdesign/core/Icon";
import { Skeleton } from "@astryxdesign/core/Skeleton";
import { VStack } from "@astryxdesign/core/Stack";
import { Heading, Text } from "@astryxdesign/core/Text";
import { TextInput } from "@astryxdesign/core/TextInput";
import { useAll } from "jazz-tools/react";
import { useState } from "react";
import { app, type Category, type Product, type Stock } from "@/schema";
import {
  snapshotProducts,
  useCatalogueSnapshot,
  type CatalogueSnapshot,
} from "@/src/catalogue/snapshot";
import { ProductCard } from "./ProductCard";
import { Page } from "./Page";

/**
 * The product grid. Search is a live Jazz query: every keystroke narrows the
 * subscription, and it runs against the local copy of the catalogue, so it
 * also works offline. Until that local copy has the catalogue (a first visit
 * syncs it), the grid shows the server-rendered snapshot instead of a
 * skeleton.
 */
export function Catalogue({ categorySlug }: { categorySlug?: string }) {
  const snapshot = useCatalogueSnapshot();
  const [search, setSearch] = useState("");
  const { data: liveCategories } = useAll(app.categories.orderBy("position", "asc"));
  const categories = liveCategories?.length ? liveCategories : snapshot?.categories;
  const category = categorySlug ? categories?.find((c) => c.slug === categorySlug) : undefined;
  const term = search.trim().toLowerCase();

  let query = app.products.orderBy("position", "asc");
  if (category) query = query.where({ categoryId: category.id });
  if (term) query = query.where({ searchText: { contains: term } });
  const waitingForCategory = categorySlug !== undefined && !category;
  const { data: liveProducts } = useAll(waitingForCategory ? undefined : query);
  // An empty local result on a first visit means "not synced yet" while the
  // snapshot has products; show those until the catalogue arrives.
  const fromSnapshot =
    snapshot && (!liveCategories?.length || !liveProducts?.length) && !waitingForCategory
      ? snapshotProducts(snapshot, { categoryId: category?.id, term })
      : undefined;
  const products = fromSnapshot?.length ? fromSnapshot : liveProducts;
  // Stock for the products on screen only, not the whole table.
  const shownIds = products?.map((product) => product.id) ?? [];
  const { data: liveStock } = useAll(
    shownIds.length ? app.stock.where({ productId: { in: shownIds } }) : undefined,
  );
  const stock = liveStock?.length ? liveStock : (snapshot?.stock ?? []);

  return (
    <CatalogueView
      categorySlug={categorySlug}
      category={category}
      products={products}
      stock={stock}
      search={search}
      onSearch={setSearch}
    />
  );
}

/**
 * The server-rendered catalogue, shown while the shopper's Jazz client opens.
 * Search waits for the live catalogue, so nothing typed is lost.
 */
export function SnapshotCatalogue({
  snapshot,
  categorySlug,
}: {
  snapshot: CatalogueSnapshot;
  categorySlug?: string;
}) {
  const category = categorySlug
    ? snapshot.categories.find((c) => c.slug === categorySlug)
    : undefined;
  return (
    <CatalogueView
      categorySlug={categorySlug}
      category={category}
      products={snapshotProducts(snapshot, { categoryId: category?.id })}
      stock={snapshot.stock}
      search=""
    />
  );
}

function CatalogueView({
  categorySlug,
  category,
  products,
  stock,
  search,
  onSearch,
}: {
  categorySlug?: string;
  category?: Category;
  products?: Product[];
  stock: Stock[];
  search: string;
  /** Without it the search box is shown disabled. */
  onSearch?: (search: string) => void;
}) {
  const term = search.trim();
  const title = category?.name ?? (categorySlug ? "" : "All products");
  return (
    <Page>
      <VStack gap={6}>
        <VStack gap={2}>
          <Heading level={1}>{title || " "}</Heading>
          <Text color="secondary">
            {category?.blurb ??
              "Instruments, studio gear and accessories, shipped from our own warehouse."}
          </Text>
        </VStack>
        <TextInput
          label={category ? `Search ${category.name.toLowerCase()}` : "Search the store"}
          isLabelHidden
          placeholder={category ? `Search ${category.name.toLowerCase()}` : "Search the store"}
          value={search}
          onChange={onSearch ?? (() => {})}
          isDisabled={!onSearch}
          startIcon={<Icon icon="search" size="sm" />}
          hasClear
          size="lg"
        />
        {products === undefined ? (
          <Grid columns={{ minWidth: 200 }} gap={4}>
            {Array.from({ length: 8 }, (_, i) => (
              <Skeleton key={i} height={240} />
            ))}
          </Grid>
        ) : products.length === 0 ? (
          <EmptyState
            title={term ? `Nothing matches "${term}"` : "No products yet"}
            description={
              term
                ? "Try a shorter word, or search all products."
                : "The catalogue is still loading."
            }
          />
        ) : (
          <Grid columns={{ minWidth: 200 }} gap={4}>
            {products.map((product) => (
              <ProductCard
                key={product.id}
                product={product}
                stock={stock.find((s) => s.productId === product.id)}
              />
            ))}
          </Grid>
        )}
      </VStack>
    </Page>
  );
}
