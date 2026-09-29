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
import { app } from "@/schema";
import { ProductCard } from "./ProductCard";
import { Page } from "./Page";

/**
 * The product grid. Search is a live Jazz query: every keystroke narrows the
 * subscription, and it runs against the local copy of the catalogue, so it
 * also works offline.
 */
export function Catalogue({ categorySlug }: { categorySlug?: string }) {
  const [search, setSearch] = useState("");
  const { data: categories } = useAll(app.categories.orderBy("position", "asc"));
  const category = categorySlug ? categories?.find((c) => c.slug === categorySlug) : undefined;
  const term = search.trim().toLowerCase();

  let query = app.products.orderBy("position", "asc");
  if (category) query = query.where({ categoryId: category.id });
  if (term) query = query.where({ searchText: { contains: term } });
  const waitingForCategory = categorySlug !== undefined && !category;
  const { data: products } = useAll(waitingForCategory ? undefined : query);
  const { data: stock = [] } = useAll(app.stock);

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
          onChange={setSearch}
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
            title={term ? `Nothing matches "${search.trim()}"` : "No products yet"}
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
