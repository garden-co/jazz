"use client";

import { Badge } from "@astryxdesign/core/Badge";
import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { Card } from "@astryxdesign/core/Card";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import { MetadataList, MetadataListItem } from "@astryxdesign/core/MetadataList";
import { NumberInput } from "@astryxdesign/core/NumberInput";
import { SegmentedControl, SegmentedControlItem } from "@astryxdesign/core/SegmentedControl";
import { Selector } from "@astryxdesign/core/Selector";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import {
  Table,
  TableBody,
  TableCell,
  TableHeader,
  TableHeaderCell,
  TableRow,
} from "@astryxdesign/core/Table";
import { Heading, Text } from "@astryxdesign/core/Text";
import { useAll, useDb } from "jazz-tools/react";
import { useState } from "react";
import { formatCents } from "@/src/format";
import {
  consoleQueries,
  ORDER_STATUS,
  placeReservation,
  releaseReservation,
  reservationOf,
  warehouseQueries,
} from "@/src/warehouse";
import { useScope } from "./console";
import { Page } from "./page";

export function OrderStatus() {
  const { warehouse, district } = useScope();
  const scope = { warehouseId: warehouse.id, districtId: district.id };
  const [mode, setMode] = useState("customer");
  const [customerId, setCustomerId] = useState("");
  const [orderNumber, setOrderNumber] = useState<number | null>(null);
  const [selectedOrderId, setSelectedOrderId] = useState<string>();
  const customers = useAll(warehouseQueries(scope).customers);
  const recent = useAll(customerId ? consoleQueries.recentOrdersOf(customerId) : undefined);
  const byNumber = useAll(
    orderNumber !== null ? consoleQueries.orderByNumber(scope, orderNumber) : undefined,
  );
  const orders = mode === "customer" ? recent.data : byNumber.data;
  const shown = orders?.find((order) => order.id === selectedOrderId) ?? orders?.[0];

  return (
    <Page title="Order status" description="Look up an order's lines and whether it was delivered.">
      <VStack gap={4} maxWidth={480}>
        <SegmentedControl label="Look up by" value={mode} onChange={setMode}>
          <SegmentedControlItem value="customer" label="Customer" />
          <SegmentedControlItem value="number" label="Order number" />
        </SegmentedControl>
        {mode === "customer" ? (
          <Selector
            label="Customer"
            placeholder="Choose a customer"
            value={customerId}
            hasSearch
            onChange={(id) => {
              setCustomerId(id);
              setSelectedOrderId(undefined);
            }}
            options={(customers.data ?? []).map((row) => ({ value: row.id, label: row.name }))}
          />
        ) : (
          <NumberInput
            label="Order number"
            value={orderNumber}
            hasClear
            isIntegerOnly
            min={1}
            onChange={setOrderNumber}
          />
        )}
      </VStack>
      {mode === "customer" && recent.data && recent.data.length > 1 && (
        <VStack gap={3}>
          <Heading level={2}>Recent orders</Heading>
          <Table density="compact">
            <TableHeader>
              <TableRow>
                <TableHeaderCell>Order</TableHeaderCell>
                <TableHeaderCell>Status</TableHeaderCell>
                <TableHeaderCell>Total</TableHeaderCell>
                <TableHeaderCell>Details</TableHeaderCell>
              </TableRow>
            </TableHeader>
            <TableBody>
              {recent.data.map((order) => (
                <TableRow key={order.id}>
                  <TableCell>
                    <Text hasTabularNumbers>{order.order_number}</Text>
                  </TableCell>
                  <TableCell>
                    <StatusBadge status={order.status} />
                  </TableCell>
                  <TableCell>
                    <Text hasTabularNumbers>{formatCents(order.total_cents)}</Text>
                  </TableCell>
                  <TableCell>
                    <Button
                      label="View"
                      size="sm"
                      variant="ghost"
                      isDisabled={order.id === shown?.id}
                      onClick={() => setSelectedOrderId(order.id)}
                    />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </VStack>
      )}
      {shown ? (
        <OrderDetails
          warehouseId={warehouse.id}
          order={{
            id: shown.id,
            number: shown.order_number,
            status: shown.status,
            totalCents: shown.total_cents,
            customer: shown.customer?.name ?? "Unknown customer",
            reservedLines: shown.reserved_lines ?? null,
          }}
        />
      ) : (
        orders && (
          <EmptyState
            isCompact
            headingLevel={2}
            title="No matching order"
            description={
              mode === "customer"
                ? "This customer has no orders yet."
                : `No order with this number in ${district.name}.`
            }
          />
        )
      )}
    </Page>
  );
}

/** A draft's reservation for display; an unreadable one shows no lines. */
function readableReservation(order: ShownOrder) {
  try {
    return reservationOf({ reserved_lines: order.reservedLines, total_cents: order.totalCents });
  } catch {
    return [];
  }
}

interface ShownOrder {
  id: string;
  number: number;
  status: string;
  totalCents: number;
  customer: string;
  reservedLines: string | null;
}

function OrderDetails({ warehouseId, order }: { warehouseId: string; order: ShownOrder }) {
  const lines = useAll(consoleQueries.linesOf(order.id));
  const deliveries = useAll(consoleQueries.deliveriesOf(warehouseId, order.id));
  const items = useAll(order.status === ORDER_STATUS.draft ? consoleQueries.items : undefined);
  // A draft has no lines yet: show what it reserved instead.
  const rows =
    order.status === ORDER_STATUS.draft && order.reservedLines
      ? readableReservation(order).map((line, index) => ({
          key: `${index}`,
          lineNumber: index + 1,
          item: items.data?.find((item) => item.id === line.itemId)?.name,
          quantity: line.quantity,
          amountCents: line.amountCents,
        }))
      : (lines.data ?? []).map((line) => ({
          key: line.id,
          lineNumber: line.line_number,
          item: line.item?.name,
          quantity: line.quantity,
          amountCents: line.amount_cents,
        }));
  return (
    <Card padding={6}>
      <VStack gap={4}>
        <Heading level={2}>Order {order.number}</Heading>
        {order.status === ORDER_STATUS.draft && <ReservedActions orderId={order.id} />}
        <MetadataList columns="multi">
          <MetadataListItem label="Customer">{order.customer}</MetadataListItem>
          <MetadataListItem label="Status">
            <StatusBadge status={order.status} />
          </MetadataListItem>
          <MetadataListItem label="Total">{formatCents(order.totalCents)}</MetadataListItem>
          <MetadataListItem label="Deliveries">
            {deliveries.data ? String(deliveries.data.length) : "–"}
          </MetadataListItem>
        </MetadataList>
        <Table density="compact">
          <TableHeader>
            <TableRow>
              <TableHeaderCell>Line</TableHeaderCell>
              <TableHeaderCell>Item</TableHeaderCell>
              <TableHeaderCell>Quantity</TableHeaderCell>
              <TableHeaderCell>Amount</TableHeaderCell>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((line) => (
              <TableRow key={line.key}>
                <TableCell>{line.lineNumber}</TableCell>
                <TableCell>{line.item ?? "Unknown item"}</TableCell>
                <TableCell>
                  <Text hasTabularNumbers>{line.quantity}</Text>
                </TableCell>
                <TableCell>
                  <Text hasTabularNumbers>{formatCents(line.amountCents)}</Text>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </VStack>
    </Card>
  );
}

/**
 * A reservation whose checkout was interrupted after taking stock. Placing it
 * finishes the checkout; releasing it returns the stock and balance.
 */
function ReservedActions({ orderId }: { orderId: string }) {
  const db = useDb();
  const { canOperate } = useScope();
  const [busy, setBusy] = useState<"place" | "release" | null>(null);
  const [error, setError] = useState<string>();
  async function run(action: "place" | "release") {
    setBusy(action);
    setError(undefined);
    try {
      await (action === "place" ? placeReservation(db, orderId) : releaseReservation(db, orderId));
    } catch (failure) {
      setError(failure instanceof Error ? failure.message : String(failure));
    } finally {
      setBusy(null);
    }
  }
  return (
    <VStack gap={3}>
      <Banner
        status="warning"
        title="Reserved, not placed"
        description="This checkout took stock and charged the balance, then stopped before the order was placed. Place it to put it in the delivery queue, or release it to give the stock and balance back."
      />
      {error && <Banner status="error" title="That didn't go through" description={error} />}
      {canOperate && (
        <HStack gap={2} wrap="wrap">
          <Button
            label="Place order"
            variant="primary"
            size="sm"
            isLoading={busy === "place"}
            isDisabled={busy !== null}
            onClick={() => void run("place")}
          />
          <Button
            label="Release"
            variant="secondary"
            size="sm"
            isLoading={busy === "release"}
            isDisabled={busy !== null}
            onClick={() => void run("release")}
          />
        </HStack>
      )}
    </VStack>
  );
}

export function StatusBadge({ status }: { status: string }) {
  switch (status) {
    case ORDER_STATUS.delivered:
      return <Badge variant="success" label="Delivered" />;
    case ORDER_STATUS.draft:
      // A checkout that reserved stock but was interrupted before placing the
      // order. Resubmitting its request key places it.
      return <Badge variant="info" label="Reserved" />;
    case ORDER_STATUS.cancelled:
      return <Badge variant="neutral" label="Cancelled" />;
    default:
      return <Badge variant="warning" label="Pending" />;
  }
}
