"use client";

import { Badge } from "@astryxdesign/core/Badge";
import { BreadcrumbItem, Breadcrumbs } from "@astryxdesign/core/Breadcrumbs";
import { Button } from "@astryxdesign/core/Button";
import { Card } from "@astryxdesign/core/Card";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import { MetadataList, MetadataListItem } from "@astryxdesign/core/MetadataList";
import { Skeleton } from "@astryxdesign/core/Skeleton";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { Step, Stepper } from "@astryxdesign/core/Stepper";
import {
  Table,
  TableBody,
  TableCell,
  TableHeader,
  TableHeaderCell,
  TableRow,
} from "@astryxdesign/core/Table";
import { Heading, Text } from "@astryxdesign/core/Text";
import { Timestamp } from "@astryxdesign/core/Timestamp";
import { useAll } from "jazz-tools/react";
import NextLink from "next/link";
import { app, type Order, type OrderEvent, type OrderStatus } from "@/schema";
import { SHIPPING, formatMoney } from "@/src/store/pricing";
import { OrderSummary } from "./OrderSummary";
import { Page } from "./Page";
import { PaymentPanel } from "./Payment";

export const STATUS_LABEL: Record<OrderStatus, string> = {
  placed: "Awaiting payment",
  paid: "Paid",
  payment_failed: "Payment failed",
  shipped: "Shipped",
};
export const STATUS_BADGE = {
  placed: "info",
  paid: "success",
  payment_failed: "error",
  shipped: "neutral",
} as const satisfies Record<OrderStatus, string>;

/**
 * An order as the shopper sees it. Every row here is written by the backend
 * (checkout, payment settlement, the fulfilment worker) and arrives through
 * the same live query, so the timeline advances on its own.
 */
export function OrderView({ orderId }: { orderId: string }) {
  const { data: orders } = useAll(app.orders.where({ id: orderId }));
  const { data: lines = [] } = useAll(app.orderLines.where({ orderId }));
  const { data: events = [] } = useAll(app.orderEvents.where({ orderId }).orderBy("at", "asc"));
  const { data: payments } = useAll(app.payments.where({ orderId }));
  const order = orders?.[0];

  if (orders === undefined)
    return (
      <Page maxWidth={1000}>
        <Skeleton height={400} />
      </Page>
    );
  if (!order)
    return (
      <Page maxWidth={1000}>
        <EmptyState
          title="Order not found"
          description="If you just placed it, it appears here in a moment. Orders are only visible to the account that placed them."
          actions={<Button label="Your orders" href="/orders" as={NextLink} />}
        />
      </Page>
    );

  const needsPayment = order.status === "placed" || order.status === "payment_failed";
  return (
    <Page maxWidth={1000}>
      <VStack gap={6}>
        <Breadcrumbs>
          <BreadcrumbItem href="/orders" as={NextLink}>
            Orders
          </BreadcrumbItem>
          <BreadcrumbItem isCurrent>{order.code}</BreadcrumbItem>
        </Breadcrumbs>
        <VStack gap={2}>
          <HStack gap={3} vAlign="center" wrap="wrap">
            <Heading level={1}>Order {order.code}</Heading>
            <Badge variant={STATUS_BADGE[order.status]} label={STATUS_LABEL[order.status]} />
          </HStack>
          <Text color="secondary">
            Placed <Timestamp value={order.placedAt.getTime()} format="date_time" />
          </Text>
        </VStack>
        <OrderTimeline order={order} events={events} />
        <div className="with-summary">
          <VStack gap={6}>
            {needsPayment && <PaymentPanel order={order} payment={payments?.[0]} />}
            <Table density="compact">
              <TableHeader>
                <TableRow>
                  <TableHeaderCell>Item</TableHeaderCell>
                  <TableHeaderCell>Qty</TableHeaderCell>
                  <TableHeaderCell>Price</TableHeaderCell>
                </TableRow>
              </TableHeader>
              <TableBody>
                {lines.map((line) => (
                  <TableRow key={line.id}>
                    <TableCell>{line.productName}</TableCell>
                    <TableCell>{line.quantity}</TableCell>
                    <TableCell>{formatMoney(line.unitPriceCents * line.quantity)}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            <MetadataList columns="single">
              <MetadataListItem label="Ship to">
                {[
                  order.shipName,
                  order.shipLine1,
                  order.shipLine2,
                  order.shipCity,
                  order.shipPostcode,
                  order.shipCountry,
                ]
                  .filter(Boolean)
                  .join(", ")}
              </MetadataListItem>
              <MetadataListItem label="Shipping">
                {SHIPPING[order.shippingMethod].label}
              </MetadataListItem>
            </MetadataList>
          </VStack>
          <OrderSummary
            subtotalCents={order.subtotalCents}
            shippingCents={order.shippingCents}
            shippingLabel={`${SHIPPING[order.shippingMethod].label} shipping`}
          />
        </div>
      </VStack>
    </Page>
  );
}

const STAGES: { status: OrderStatus; label: string }[] = [
  { status: "placed", label: "Placed" },
  { status: "paid", label: "Paid" },
  { status: "shipped", label: "Shipped" },
];

function OrderTimeline({ order, events }: { order: Order; events: OrderEvent[] }) {
  const reached =
    order.status === "payment_failed" ? 1 : STAGES.findIndex((s) => s.status === order.status) + 1;
  const eventFor = (status: OrderStatus) =>
    events.find((e) => e.status === status) ??
    (status === "paid" && order.status === "payment_failed"
      ? events.find((e) => e.status === "payment_failed")
      : undefined);
  return (
    <Card padding={5}>
      <Stepper activeStep={Math.min(reached, STAGES.length - 1)} label="Order status">
        {STAGES.map((stage, index) => {
          const event = eventFor(stage.status);
          const failed = stage.status === "paid" && order.status === "payment_failed";
          return (
            <Step
              key={stage.status}
              step={index}
              label={failed ? "Payment failed" : stage.label}
              status={failed ? "error" : index < reached ? "success" : undefined}
              description={
                event
                  ? formatTime(event.at)
                  : index === reached
                    ? nextHint(order.status)
                    : undefined
              }
            />
          );
        })}
      </Stepper>
    </Card>
  );
}

function nextHint(status: OrderStatus): string | undefined {
  if (status === "placed") return "Waiting for payment";
  if (status === "paid") return "Packing in the warehouse";
  if (status === "payment_failed") return "Retry the payment below";
  return undefined;
}

const time = new Intl.DateTimeFormat("en-US", { dateStyle: "medium", timeStyle: "short" });
function formatTime(at: Date): string {
  return time.format(at);
}
