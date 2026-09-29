"use client";

import { Badge } from "@astryxdesign/core/Badge";
import { Button } from "@astryxdesign/core/Button";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import { Link } from "@astryxdesign/core/Link";
import { Skeleton } from "@astryxdesign/core/Skeleton";
import { VStack } from "@astryxdesign/core/Stack";
import { Table, TableBody, TableCell, TableHeader, TableHeaderCell, TableRow } from "@astryxdesign/core/Table";
import { Heading, Text } from "@astryxdesign/core/Text";
import { Timestamp } from "@astryxdesign/core/Timestamp";
import { useAll } from "jazz-tools/react";
import NextLink from "next/link";
import { app } from "@/schema";
import { formatMoney } from "@/src/store/pricing";
import { STATUS_BADGE, STATUS_LABEL } from "./OrderView";
import { Page } from "./Page";
import { useShopper } from "./StoreProviders";

/** Read permissions do the filtering: this query only ever sees your orders. */
export function OrderHistory() {
  const shopper = useShopper();
  const { data: orders } = useAll(app.orders.orderBy("placedAt", "desc"));

  return (
    <Page maxWidth={1000}>
      <VStack gap={6}>
        <Heading level={1}>Orders</Heading>
        {!shopper.isSignedIn ? (
          <EmptyState
            title="Sign in to see your orders"
            description="Orders belong to your account, so they follow you to every device."
            actions={<Button label="Sign in" href="/sign-in?next=/orders" as={NextLink} />}
          />
        ) : orders === undefined ? (
          <Skeleton height={200} />
        ) : orders.length === 0 ? (
          <EmptyState
            title="No orders yet"
            description="When you place an order, you can follow it here from payment to shipping."
            actions={<Button label="Browse products" href="/" as={NextLink} />}
          />
        ) : (
          <Table density="compact">
            <TableHeader>
              <TableRow>
                <TableHeaderCell>Order</TableHeaderCell>
                <TableHeaderCell>Placed</TableHeaderCell>
                <TableHeaderCell>Status</TableHeaderCell>
                <TableHeaderCell>Total</TableHeaderCell>
              </TableRow>
            </TableHeader>
            <TableBody>
              {orders.map((order) => (
                <TableRow key={order.id}>
                  <TableCell>
                    <Link href={`/orders/${order.id}`} as={NextLink}>
                      {order.code}
                    </Link>
                  </TableCell>
                  <TableCell>
                    <Timestamp value={order.placedAt.getTime()} format="date" />
                  </TableCell>
                  <TableCell>
                    <Badge variant={STATUS_BADGE[order.status]} label={STATUS_LABEL[order.status]} />
                  </TableCell>
                  <TableCell>
                    <Text hasTabularNumbers>{formatMoney(order.totalCents)}</Text>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </VStack>
    </Page>
  );
}
