"use client";

import { BreadcrumbItem, Breadcrumbs } from "@astryxdesign/core/Breadcrumbs";
import { Button } from "@astryxdesign/core/Button";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import { MetadataList, MetadataListItem } from "@astryxdesign/core/MetadataList";
import { NumberInput } from "@astryxdesign/core/NumberInput";
import { Skeleton } from "@astryxdesign/core/Skeleton";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { Heading, Text } from "@astryxdesign/core/Text";
import { useAll } from "jazz-tools/react";
import { Link } from "@astryxdesign/core/Link";
import NextLink from "next/link";
import { useState } from "react";
import { MAX_LINE_QUANTITY } from "@/permissions";
import { app } from "@/schema";
import { useCart } from "@/src/store/cart";
import { formatMoney } from "@/src/store/pricing";
import { Page } from "./Page";
import { ProductArt } from "./ProductArt";
import { StockBadge } from "./StockBadge";
import { useShopper } from "./StoreProviders";

export function ProductDetail({ slug }: { slug: string }) {
  const shopper = useShopper();
  const cart = useCart(shopper.account);
  const { data: products } = useAll(app.products.where({ slug }).limit(1));
  const product = products?.[0];
  const { data: categories } = useAll(
    product ? app.categories.where({ id: product.categoryId }) : undefined,
  );
  const { data: stockRows } = useAll(product ? app.stock.where({ productId: product.id }) : undefined);
  const [quantity, setQuantity] = useState(1);

  if (products === undefined)
    return (
      <Page>
        <Skeleton height={360} />
      </Page>
    );
  if (!product)
    return (
      <Page>
        <EmptyState
          title="Product not found"
          description="It may have been removed from the catalogue."
          actions={<Button label="Browse all products" href="/" as={NextLink} />}
        />
      </Page>
    );

  const category = categories?.[0];
  const stock = stockRows?.[0];
  const inCart = cart.quantityOf(product.id);
  const available = Math.max(0, (stock?.onHand ?? 0) - inCart);
  const specs = product.specs as [string, string][];

  return (
    <Page>
      <VStack gap={6}>
        <Breadcrumbs>
          <BreadcrumbItem href="/" as={NextLink}>
            Shop
          </BreadcrumbItem>
          {category && (
            <BreadcrumbItem href={`/category/${category.slug}`} as={NextLink}>
              {category.name}
            </BreadcrumbItem>
          )}
          <BreadcrumbItem isCurrent>{product.name}</BreadcrumbItem>
        </Breadcrumbs>
        <div className="product-layout">
          <ProductArt art={product.art} hue={product.hue} label={product.name} />
          <VStack gap={5}>
            <VStack gap={2}>
              <Text color="secondary">{product.brand}</Text>
              <Heading level={1}>{product.name}</Heading>
              <HStack gap={3} vAlign="center" wrap="wrap">
                <Heading level={2}>{formatMoney(product.priceCents)}</Heading>
                <StockBadge stock={stock} />
              </HStack>
            </VStack>
            <Text>{product.summary}</Text>
            <HStack gap={3} vAlign="end" wrap="wrap">
              <NumberInput
                label="Quantity"
                value={quantity}
                onChange={(value) => setQuantity(value)}
                min={1}
                max={Math.max(1, Math.min(MAX_LINE_QUANTITY, available))}
                isIntegerOnly
                hasNumberSteppers
                width={120}
                size="lg"
                isDisabled={available === 0}
              />
              <Button
                label={inCart ? "Add more to cart" : "Add to cart"}
                size="lg"
                isDisabled={available === 0}
                onClick={() => {
                  cart.setQuantity(product.id, inCart + Math.min(quantity, available));
                  setQuantity(1);
                }}
              />
            </HStack>
            {inCart > 0 && (
              <Text color="secondary">
                {inCart} in your cart. <Link href="/cart" as={NextLink}>
                  View cart
                </Link>
              </Text>
            )}
            <Text>{product.description}</Text>
            <MetadataList title="Specifications" columns="single">
              <MetadataListItem label="SKU">{product.sku}</MetadataListItem>
              {specs.map(([label, value]) => (
                <MetadataListItem key={label} label={label}>
                  {value}
                </MetadataListItem>
              ))}
            </MetadataList>
          </VStack>
        </div>
      </VStack>
    </Page>
  );
}
