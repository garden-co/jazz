import { Card } from "@astryxdesign/core/Card";
import { Divider } from "@astryxdesign/core/Divider";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { Heading, Text } from "@astryxdesign/core/Text";
import type { ReactNode } from "react";
import { formatMoney } from "@/src/store/pricing";

/** Totals card shared by the cart, checkout review and order pages. */
export function OrderSummary({
  subtotalCents,
  shippingCents,
  shippingLabel = "Shipping",
  children,
}: {
  subtotalCents: number;
  shippingCents: number;
  shippingLabel?: string;
  children?: ReactNode;
}) {
  return (
    <Card padding={5}>
      <VStack gap={3}>
        <Heading level={2}>Summary</Heading>
        <Row label="Subtotal" value={formatMoney(subtotalCents)} />
        <Row
          label={shippingLabel}
          value={shippingCents === 0 ? "Free" : formatMoney(shippingCents)}
        />
        <Divider />
        <HStack gap={2} justify="space-between">
          <Text weight="semibold">Total</Text>
          <Text weight="semibold" hasTabularNumbers>
            {formatMoney(subtotalCents + shippingCents)}
          </Text>
        </HStack>
        {children}
      </VStack>
    </Card>
  );
}

function Row({ label, value }: { label: string; value: string }) {
  return (
    <HStack gap={2} justify="space-between">
      <Text color="secondary">{label}</Text>
      <Text hasTabularNumbers>{value}</Text>
    </HStack>
  );
}
