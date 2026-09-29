"use client";

import { Badge } from "@astryxdesign/core/Badge";
import { Button } from "@astryxdesign/core/Button";
import { Card } from "@astryxdesign/core/Card";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import { MetadataList, MetadataListItem } from "@astryxdesign/core/MetadataList";
import { NumberInput } from "@astryxdesign/core/NumberInput";
import { SegmentedControl, SegmentedControlItem } from "@astryxdesign/core/SegmentedControl";
import { Selector } from "@astryxdesign/core/Selector";
import { VStack } from "@astryxdesign/core/Stack";
import {
  Table,
  TableBody,
  TableCell,
  TableHeader,
  TableHeaderCell,
  TableRow,
} from "@astryxdesign/core/Table";
import { Heading, Text } from "@astryxdesign/core/Text";
import { useAll } from "jazz-tools/react";
import { useState } from "react";
import { formatCents } from "@/src/format";
import { consoleQueries, warehouseQueries } from "@/src/warehouse";
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

function OrderDetails({
  warehouseId,
  order,
}: {
  warehouseId: string;
  order: { id: string; number: number; status: string; totalCents: number; customer: string };
}) {
  const lines = useAll(consoleQueries.linesOf(order.id));
  const deliveries = useAll(consoleQueries.deliveriesOf(warehouseId, order.id));
  return (
    <Card padding={6}>
      <VStack gap={4}>
        <Heading level={2}>Order {order.number}</Heading>
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
            {(lines.data ?? []).map((line) => (
              <TableRow key={line.id}>
                <TableCell>{line.line_number}</TableCell>
                <TableCell>{line.item?.name ?? "Unknown item"}</TableCell>
                <TableCell>
                  <Text hasTabularNumbers>{line.quantity}</Text>
                </TableCell>
                <TableCell>
                  <Text hasTabularNumbers>{formatCents(line.amount_cents)}</Text>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </VStack>
    </Card>
  );
}

export function StatusBadge({ status }: { status: string }) {
  return status === "delivered" ? (
    <Badge variant="success" label="Delivered" />
  ) : (
    <Badge variant="warning" label="Pending" />
  );
}
