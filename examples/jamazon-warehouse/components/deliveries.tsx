"use client";

import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import {
  Table,
  TableBody,
  TableCell,
  TableHeader,
  TableHeaderCell,
  TableRow,
} from "@astryxdesign/core/Table";
import { Text } from "@astryxdesign/core/Text";
import { useAll, useDb } from "jazz-tools/react";
import { useState } from "react";
import type { District } from "@/schema";
import { formatCents, formatCount } from "@/src/format";
import { consoleQueries, deliverBatch, PAGE_SIZE, type DeliveredOrder } from "@/src/warehouse";
import { useScope } from "./console";
import { Page } from "./page";

export function Deliveries() {
  const db = useDb();
  const { warehouse, districts, canOperate } = useScope();
  const [pending, setPending] = useState(false);
  const [result, setResult] = useState<DeliveredOrder[] | Error>();
  const districtName = (id: string) => districts.find((district) => district.id === id)?.name;

  return (
    <Page
      title="Delivery"
      description="One batch delivers the oldest pending order in every district of the warehouse."
    >
      <Table density="compact">
        <TableHeader>
          <TableRow>
            <TableHeaderCell>District</TableHeaderCell>
            <TableHeaderCell>Next order</TableHeaderCell>
            <TableHeaderCell>Customer</TableHeaderCell>
            <TableHeaderCell>Total</TableHeaderCell>
            <TableHeaderCell>Queue</TableHeaderCell>
          </TableRow>
        </TableHeader>
        <TableBody>
          {districts.map((district) => (
            <QueueHead key={district.id} warehouseId={warehouse.id} district={district} />
          ))}
        </TableBody>
      </Table>
      <VStack gap={3}>
        {result instanceof Error && (
          <Banner status="error" title="The batch was not delivered" description={result.message} />
        )}
        {Array.isArray(result) && (
          <Banner
            status={result.length ? "success" : "info"}
            title={
              result.length
                ? `Delivered ${result.length} ${result.length === 1 ? "order" : "orders"}`
                : "Nothing to deliver"
            }
            description={
              result.length
                ? result
                    .map((order) => `${order.orderNumber} (${districtName(order.districtId)})`)
                    .join(", ")
                : "Every district's queue is empty."
            }
          />
        )}
        <HStack>
          <Button
            label="Deliver next batch"
            variant="primary"
            isLoading={pending}
            isDisabled={!canOperate}
            onClick={async () => {
              setPending(true);
              try {
                setResult(await deliverBatch(db, warehouse.id));
              } catch (error) {
                setResult(error instanceof Error ? error : new Error(String(error)));
              } finally {
                setPending(false);
              }
            }}
          />
        </HStack>
      </VStack>
    </Page>
  );
}

function QueueHead({ warehouseId, district }: { warehouseId: string; district: District }) {
  const queue = useAll(consoleQueries.pendingQueue({ warehouseId, districtId: district.id }));
  const head = queue.data?.[0];
  return (
    <TableRow>
      <TableCell>
        <Text weight="medium">{district.name}</Text>
      </TableCell>
      <TableCell>
        <Text hasTabularNumbers>{head ? head.order_number : "–"}</Text>
      </TableCell>
      <TableCell>{head?.customer?.name ?? "–"}</TableCell>
      <TableCell>
        <Text hasTabularNumbers>{head ? formatCents(head.total_cents) : "–"}</Text>
      </TableCell>
      <TableCell>
        <Text hasTabularNumbers>
          {queue.data ? formatCount(queue.data.length, PAGE_SIZE) : "–"}
        </Text>
      </TableCell>
    </TableRow>
  );
}
