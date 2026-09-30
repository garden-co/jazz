"use client";

import { ClickableCard } from "@astryxdesign/core/ClickableCard";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { Text } from "@astryxdesign/core/Text";
import { useRouter } from "next/navigation";
import type { Product, Stock } from "@/schema";
import { formatMoney } from "@/src/store/pricing";
import { ProductArt } from "./ProductArt";
import { StockBadge } from "./StockBadge";

export function ProductCard({ product, stock }: { product: Product; stock?: Stock }) {
  const router = useRouter();
  const href = `/product/${product.slug}`;
  return (
    <ClickableCard
      label={product.name}
      href={href}
      onClick={(event) => {
        // Keep the Jazz client alive: navigate in-app instead of reloading.
        if (event.metaKey || event.ctrlKey || event.shiftKey) return;
        event.preventDefault();
        router.push(href);
      }}
      padding={3}
    >
      <VStack gap={3}>
        <ProductArt art={product.art} hue={product.hue} label="" />
        <VStack gap={1}>
          <Text weight="medium">{product.name}</Text>
          <HStack gap={2} vAlign="center" wrap="wrap">
            <Text color="secondary">{formatMoney(product.priceCents)}</Text>
            {stock && stock.onHand <= 5 && <StockBadge stock={stock} />}
          </HStack>
        </VStack>
      </VStack>
    </ClickableCard>
  );
}
