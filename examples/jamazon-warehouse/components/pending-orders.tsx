"use client";

import { EmptyState } from "@astryxdesign/core/EmptyState";
import {
  Table,
  TableBody,
  TableCell,
  TableHeader,
  TableHeaderCell,
  TableRow,
} from "@astryxdesign/core/Table";
import { Text } from "@astryxdesign/core/Text";
import { useAll } from "jazz-tools/react";
import { formatCents } from "@/src/format";
import { consoleQueries, PAGE_SIZE } from "@/src/warehouse";
import { useScope } from "./console";

/** The district's delivery queue: oldest pending order first, one bounded page. */
export function PendingOrdersTable({ limit = PAGE_SIZE }: { limit?: number }) {
  const { warehouse, district } = useScope();
  const queue = useAll(
    consoleQueries.pendingQueue({ warehouseId: warehouse.id, districtId: district.id }),
  );
  const rows = queue.data?.slice(0, limit);
  if (rows && rows.length === 0) {
    return (
      <EmptyState
        isCompact
        headingLevel={3}
        title="No pending orders"
        description={`Every order in ${district.name} has been delivered.`}
      />
    );
  }
  return (
    <Table density="compact">
      <TableHeader>
        <TableRow>
          <TableHeaderCell>Order</TableHeaderCell>
          <TableHeaderCell>Customer</TableHeaderCell>
          <TableHeaderCell>Lines</TableHeaderCell>
          <TableHeaderCell>Total</TableHeaderCell>
        </TableRow>
      </TableHeader>
      <TableBody>
        {(rows ?? []).map((order) => (
          <TableRow key={order.id}>
            <TableCell>
              <Text hasTabularNumbers weight="medium">
                {order.order_number}
              </Text>
            </TableCell>
            <TableCell>{order.customer?.name ?? "Unknown customer"}</TableCell>
            <TableCell>
              <Text hasTabularNumbers>{order.order_linesViaOrder.length}</Text>
            </TableCell>
            <TableCell>
              <Text hasTabularNumbers>{formatCents(order.total_cents)}</Text>
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
