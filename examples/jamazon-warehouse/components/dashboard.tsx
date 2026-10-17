"use client";

import { Button } from "@astryxdesign/core/Button";
import { Grid } from "@astryxdesign/core/Grid";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { Heading } from "@astryxdesign/core/Text";
import { useAll } from "jazz-tools/react";
import Link from "next/link";
import { useState } from "react";
import { formatCount } from "@/src/format";
import { belowReorderLevel, consoleQueries, COUNT_CAP, startOfToday } from "@/src/warehouse";
import { useScope } from "./console";
import { Page } from "./page";
import { PendingOrdersTable } from "./pending-orders";
import { Stat } from "./stat";

export function Dashboard() {
  const { warehouse, district } = useScope();
  const [since] = useState(startOfToday);
  const ordersToday = useAll(consoleQueries.ordersSince(warehouse.id, since));
  const pending = useAll(consoleQueries.pendingInWarehouse(warehouse.id));
  const stock = useAll(consoleQueries.stockLevelCandidates(warehouse.id));
  const count = (rows: unknown[] | undefined) => (rows ? formatCount(rows.length, COUNT_CAP) : "–");

  return (
    <Page title="Dashboard" description="Numbers update live as other operators work.">
      <Grid columns={{ minWidth: 200 }} gap={3}>
        <Stat label="Orders today" value={count(ordersToday.data)} note="All districts" />
        <Stat label="Pending deliveries" value={count(pending.data)} note="All districts" />
        <Stat
          label="Low-stock items"
          value={stock.data ? String(belowReorderLevel(stock.data).length) : "–"}
          note="Below their reorder level"
        />
      </Grid>
      <VStack gap={3}>
        <HStack gap={3} vAlign="center" justify="between" wrap="wrap">
          <Heading level={2}>Next to deliver in {district.name}</Heading>
          <Button
            label="Open delivery"
            variant="secondary"
            size="sm"
            href="/deliveries"
            as={Link}
          />
        </HStack>
        <PendingOrdersTable limit={5} />
      </VStack>
    </Page>
  );
}
