"use client";

import { PendingOrdersTable } from "@/components/pending-orders";
import { Page } from "@/components/page";

export default function PendingOrdersPage() {
  return (
    <Page
      title="Pending orders"
      description="The delivery queue, oldest order first. The page holds the first 20."
    >
      <PendingOrdersTable />
    </Page>
  );
}
