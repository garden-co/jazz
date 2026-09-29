"use client";

import { Button } from "@astryxdesign/core/Button";
import { Divider } from "@astryxdesign/core/Divider";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import { Link } from "@astryxdesign/core/Link";
import { NumberInput } from "@astryxdesign/core/NumberInput";
import { Skeleton } from "@astryxdesign/core/Skeleton";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { Heading, Text } from "@astryxdesign/core/Text";
import NextLink from "next/link";
import { Fragment } from "react";
import { MAX_LINE_QUANTITY } from "@/permissions";
import { FREE_STANDARD_FROM_CENTS, formatMoney } from "@/src/store/pricing";
import { useCart, type CartItem } from "@/src/store/cart";
import { OrderSummary } from "./OrderSummary";
import { Page } from "./Page";
import { ProductArt } from "./ProductArt";
import { useShopper } from "./StoreProviders";

export function CartView() {
  const shopper = useShopper();
  const cart = useCart(shopper.account);
  const overStock = cart.items.some((item) => item.line.quantity > (item.stock?.onHand ?? 0));

  return (
    <Page>
      <VStack gap={6}>
        <VStack gap={2}>
          <Heading level={1}>Cart</Heading>
          <Text color="secondary">
            {shopper.isSignedIn
              ? "Saved to your account and synced across your devices, even offline."
              : "Saved on this device, even offline. Sign in to keep it on all your devices."}
          </Text>
        </VStack>
        {cart.isLoading ? (
          <Skeleton height={200} />
        ) : cart.items.length === 0 ? (
          <EmptyState
            title="Your cart is empty"
            description="Everything you add shows up here, on every device you sign in on."
            actions={<Button label="Browse products" href="/" as={NextLink} />}
          />
        ) : (
          <div className="with-summary">
            <VStack gap={4}>
              {cart.items.map((item, index) => (
                <Fragment key={item.line.id}>
                  {index > 0 && <Divider />}
                  <CartRow item={item} setQuantity={cart.setQuantity} />
                </Fragment>
              ))}
            </VStack>
            <OrderSummary
              subtotalCents={cart.subtotalCents}
              shippingCents={cart.shippingCents}
              shippingLabel="Standard shipping"
            >
              {cart.subtotalCents < FREE_STANDARD_FROM_CENTS && (
                <Text type="supporting" color="secondary">
                  Free standard shipping from {formatMoney(FREE_STANDARD_FROM_CENTS)}.
                </Text>
              )}
              <Button
                label="Check out"
                size="lg"
                width="100%"
                href="/checkout"
                as={NextLink}
                isDisabled={overStock}
                tooltip={overStock ? "Reduce the quantities that exceed stock first" : undefined}
              />
            </OrderSummary>
          </div>
        )}
      </VStack>
    </Page>
  );
}

function CartRow({
  item,
  setQuantity,
}: {
  item: CartItem;
  setQuantity: (productId: string, quantity: number) => void;
}) {
  const { product, line, stock } = item;
  const onHand = stock?.onHand ?? 0;
  const tooMany = line.quantity > onHand;
  return (
    <HStack gap={4} vAlign="start">
      <div className="thumb">
        <ProductArt art={product.art} hue={product.hue} label="" />
      </div>
      <VStack gap={3} width="100%">
        <HStack gap={3} justify="space-between" wrap="wrap">
          <VStack gap={1}>
            <Link href={`/product/${product.slug}`} as={NextLink} weight="medium">
              {product.name}
            </Link>
            <Text color="secondary">{formatMoney(product.priceCents)} each</Text>
          </VStack>
          <Text weight="medium" hasTabularNumbers>
            {formatMoney(product.priceCents * line.quantity)}
          </Text>
        </HStack>
        <HStack gap={2} vAlign="end" wrap="wrap">
          <NumberInput
            label={`Quantity of ${product.name}`}
            isLabelHidden
            value={line.quantity}
            onChange={(value) => setQuantity(product.id, value)}
            min={1}
            max={MAX_LINE_QUANTITY}
            isIntegerOnly
            hasNumberSteppers
            width={120}
            status={
              tooMany
                ? {
                    type: "error",
                    message: onHand === 0 ? "Out of stock" : `Only ${onHand} in stock`,
                  }
                : undefined
            }
          />
          <Button label="Remove" variant="ghost" onClick={() => setQuantity(product.id, 0)} />
        </HStack>
      </VStack>
    </HStack>
  );
}
