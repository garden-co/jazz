"use client";

import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { Card } from "@astryxdesign/core/Card";
import { Divider } from "@astryxdesign/core/Divider";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import { FormLayout } from "@astryxdesign/core/FormLayout";
import { MetadataList, MetadataListItem } from "@astryxdesign/core/MetadataList";
import { RadioList, RadioListItem } from "@astryxdesign/core/RadioList";
import { Skeleton } from "@astryxdesign/core/Skeleton";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { Step, Stepper } from "@astryxdesign/core/Stepper";
import { Heading, Text } from "@astryxdesign/core/Text";
import { TextInput } from "@astryxdesign/core/TextInput";
import type { Db } from "jazz-tools";
import { useDb } from "jazz-tools/react";
import NextLink from "next/link";
import { useRouter } from "next/navigation";
import { Fragment, useEffect, useRef, useState } from "react";
import { app, type Cart, type ShippingMethod } from "@/schema";
import { requireBetterAuthToken } from "@/src/lib/auth-client";
import { useCart } from "@/src/store/cart";
import { SHIPPING, formatMoney, shippingCents } from "@/src/store/pricing";
import { OrderSummary } from "./OrderSummary";
import { Page } from "./Page";
import { useShopper } from "./StoreProviders";

type Address = Pick<
  Cart,
  "shipName" | "shipLine1" | "shipLine2" | "shipCity" | "shipPostcode" | "shipCountry"
>;
const REQUIRED: (keyof Address)[] = ["shipName", "shipLine1", "shipCity", "shipPostcode", "shipCountry"];

/**
 * Checkout is two steps over the synced cart row: shipping details, then a
 * review. Reaching the review mints the idempotency key that "Place order"
 * sends, so every retry of that click names the same order.
 */
export function Checkout() {
  const shopper = useShopper();
  const cart = useCart(shopper.account);

  if (cart.isLoading)
    return (
      <Page maxWidth={1000}>
        <Skeleton height={400} />
      </Page>
    );
  if (cart.items.length === 0)
    return (
      <Page maxWidth={1000}>
        <EmptyState
          title="Your cart is empty"
          description="Add something before checking out."
          actions={<Button label="Browse products" href="/" as={NextLink} />}
        />
      </Page>
    );

  const reviewing = Boolean(cart.cart?.checkoutKey);
  return (
    <Page maxWidth={1000}>
      <VStack gap={6}>
        <Heading level={1}>Checkout</Heading>
        <Stepper activeStep={reviewing ? 1 : 0} label="Checkout progress">
          <Step step={0} label="Shipping" />
          <Step step={1} label="Review" />
          <Step step={2} label="Payment" />
        </Stepper>
        {reviewing ? <ReviewStep cart={cart} /> : <ShippingStep cart={cart} />}
      </VStack>
    </Page>
  );
}

type CartState = ReturnType<typeof useCart>;

function ShippingStep({ cart }: { cart: CartState }) {
  const [draft, setDraft] = useState<Address>({});
  const [showErrors, setShowErrors] = useState(false);
  // Start from what the cart already holds (possibly typed on another device).
  const loaded = useRef(false);
  useEffect(() => {
    if (loaded.current || !cart.cart) return;
    loaded.current = true;
    setDraft(pickAddress(cart.cart));
  }, [cart.cart]);

  // Save the address to the cart shortly after typing stops, so it syncs to
  // the shopper's other devices and survives a reload, online or not.
  const { updateCheckout } = cart;
  useEffect(() => {
    if (!loaded.current) return;
    const timer = setTimeout(() => {
      const saved = cart.cart ? pickAddress(cart.cart) : {};
      const changed = Object.fromEntries(
        Object.entries(draft).filter(([k, v]) => (v?.trim() || null) !== (saved[k as keyof Address] ?? null)),
      );
      if (Object.keys(changed).length)
        updateCheckout(Object.fromEntries(Object.entries(changed).map(([k, v]) => [k, v?.trim() || null])));
    }, 600);
    return () => clearTimeout(timer);
  }, [draft]);

  const method = cart.cart?.shippingMethod ?? "standard";
  const field = (name: keyof Address, label: string, autoComplete: string, optional = false) => {
    const missing = showErrors && !optional && !draft[name]?.trim();
    return (
      <TextInput
        label={label}
        value={draft[name] ?? ""}
        onChange={(value) => setDraft((d) => ({ ...d, [name]: value }))}
        autoComplete={autoComplete}
        isOptional={optional}
        isRequired={!optional}
        status={missing ? { type: "error", message: `Enter ${label.toLowerCase()}` } : undefined}
      />
    );
  };

  function continueToReview() {
    if (REQUIRED.some((name) => !draft[name]?.trim())) return setShowErrors(true);
    const trimmed = Object.fromEntries(
      Object.entries(draft).map(([k, v]) => [k, v?.trim() || null]),
    ) as Address;
    cart.updateCheckout({ ...trimmed, checkoutKey: crypto.randomUUID() });
  }

  return (
    <div className="with-summary">
      <VStack gap={6}>
        <FormLayout>
          <Heading level={2}>Shipping address</Heading>
          {field("shipName", "Full name", "name")}
          {field("shipLine1", "Address", "address-line1")}
          {field("shipLine2", "Apartment, suite or unit", "address-line2", true)}
          <HStack gap={3} wrap="wrap">
            {field("shipCity", "City", "address-level2")}
            {field("shipPostcode", "Postcode", "postal-code")}
          </HStack>
          {field("shipCountry", "Country", "country-name")}
        </FormLayout>
        <RadioList
          label="Shipping method"
          value={method}
          onChange={(value) => cart.updateCheckout({ shippingMethod: value as ShippingMethod })}
        >
          {(Object.keys(SHIPPING) as ShippingMethod[]).map((key) => {
            const cents = shippingCents(key, cart.subtotalCents);
            return (
              <RadioListItem
                key={key}
                value={key}
                label={SHIPPING[key].label}
                description={SHIPPING[key].description}
                endContent={<Text hasTabularNumbers>{cents ? formatMoney(cents) : "Free"}</Text>}
              />
            );
          })}
        </RadioList>
        <HStack gap={3} wrap="wrap">
          <Button label="Continue to review" size="lg" onClick={continueToReview} />
          <Button label="Back to cart" variant="ghost" size="lg" href="/cart" as={NextLink} />
        </HStack>
      </VStack>
      <OrderSummary
        subtotalCents={cart.subtotalCents}
        shippingCents={cart.shippingCents}
        shippingLabel={`${SHIPPING[method].label} shipping`}
      />
    </div>
  );
}

function ReviewStep({ cart }: { cart: CartState }) {
  const shopper = useShopper();
  const db = useDb<Db>();
  const router = useRouter();
  const [placing, setPlacing] = useState(false);
  const [error, setError] = useState<string>();
  const current = cart.cart!;
  const method = current.shippingMethod;

  async function placeOrder() {
    setPlacing(true);
    setError(undefined);
    try {
      const orderId = await submitOrder(db, cart.cartId, current.checkoutKey!);
      router.push(`/orders/${orderId}`);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
      setPlacing(false);
    }
  }

  return (
    <div className="with-summary">
      <VStack gap={6}>
        <Card padding={5}>
          <VStack gap={4}>
            <HStack gap={2} justify="space-between" vAlign="center">
              <Heading level={2}>Ship to</Heading>
              <Button
                label="Edit"
                variant="ghost"
                onClick={() => cart.updateCheckout({ checkoutKey: null })}
              />
            </HStack>
            <MetadataList columns="single">
              <MetadataListItem label="Address">
                {[current.shipName, current.shipLine1, current.shipLine2, current.shipCity, current.shipPostcode, current.shipCountry]
                  .filter(Boolean)
                  .join(", ")}
              </MetadataListItem>
              <MetadataListItem label="Shipping">
                {SHIPPING[method].label}, {SHIPPING[method].description.toLowerCase()}
              </MetadataListItem>
            </MetadataList>
            <Divider />
            <Heading level={2}>Items</Heading>
            <VStack gap={2}>
              {cart.items.map(({ line, product }) => (
                <Fragment key={line.id}>
                  <HStack gap={3} justify="space-between">
                    <Text>
                      {line.quantity} × {product.name}
                    </Text>
                    <Text hasTabularNumbers>{formatMoney(line.quantity * product.priceCents)}</Text>
                  </HStack>
                </Fragment>
              ))}
            </VStack>
          </VStack>
        </Card>
        {error && <Banner status="error" title="The order was not placed" description={error} />}
      </VStack>
      <OrderSummary
        subtotalCents={cart.subtotalCents}
        shippingCents={cart.shippingCents}
        shippingLabel={`${SHIPPING[method].label} shipping`}
      >
        {shopper.isSignedIn ? (
          <Button
            label={placing ? "Placing order" : "Place order"}
            size="lg"
            width="100%"
            isLoading={placing}
            onClick={() => void placeOrder()}
          />
        ) : (
          <VStack gap={3}>
            <Text type="supporting" color="secondary">
              Sign in or create an account to place the order. Your cart comes with you.
            </Text>
            <Button label="Sign in to order" size="lg" width="100%" href="/sign-in?next=/checkout" as={NextLink} />
          </VStack>
        )}
        <Text type="supporting" color="secondary">
          You pay on the next step. Placing the order reserves the stock.
        </Text>
      </OrderSummary>
    </div>
  );
}

function pickAddress(cart: Cart): Address {
  const { shipName, shipLine1, shipLine2, shipCity, shipPostcode, shipCountry } = cart;
  return { shipName, shipLine1, shipLine2, shipCity, shipPostcode, shipCountry };
}

/**
 * Hand the reviewed cart to the backend. First make sure this device's cart
 * edits have reached the server, then POST the idempotency key. Network
 * failures are retried with the same key: the server returns the order it
 * already made instead of making a second one.
 */
async function submitOrder(db: Db, cartId: string, idempotencyKey: string): Promise<string> {
  if (!navigator.onLine) throw new Error("You're offline. Your cart is saved; place the order once you're back online.");
  await withTimeout(
    db.update(app.carts, cartId, { checkoutKey: idempotencyKey }).wait({ tier: "global" }),
    15_000,
    "Couldn't reach the store to sync your cart. Try again.",
  );
  let lastError: unknown;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const response = await fetch("/api/checkout", {
        method: "POST",
        credentials: "same-origin",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${await requireBetterAuthToken()}`,
        },
        body: JSON.stringify({ idempotencyKey }),
      });
      const body = (await response.json()) as { orderId?: string; error?: string };
      if (response.ok && body.orderId) return body.orderId;
      // The server answered: a retry would get the same answer.
      if (response.status < 500) throw new Error(body.error ?? "The order was not placed.");
      lastError = new Error(body.error ?? "The store had a problem.");
    } catch (cause) {
      if (!(cause instanceof TypeError)) throw cause; // TypeError: the request never got an answer
      lastError = cause;
    }
    await new Promise((resolve) => setTimeout(resolve, 1000 * (attempt + 1)));
  }
  throw lastError instanceof Error ? lastError : new Error("The order was not placed.");
}

function withTimeout<T>(promise: Promise<T>, ms: number, message: string): Promise<T> {
  return Promise.race([
    promise,
    new Promise<never>((_, reject) => setTimeout(() => reject(new Error(message)), ms)),
  ]);
}
