"use client";

import { Badge } from "@astryxdesign/core/Badge";
import { Button } from "@astryxdesign/core/Button";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import { SegmentedControl, SegmentedControlItem } from "@astryxdesign/core/SegmentedControl";
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
import { belowReorderLevel, consoleQueries, restock } from "@/src/warehouse";
import { useScope } from "./console";
import { Page } from "./page";

export function StockLevel() {
  const db = useDb();
  const { warehouse, canOperate } = useScope();
  const [view, setView] = useState("low");
  const candidates = useAll(consoleQueries.stockLevelCandidates(warehouse.id));
  const all = useAll(view === "all" ? consoleQueries.stockOf(warehouse.id) : undefined);
  const rows =
    view === "low"
      ? candidates.data && belowReorderLevel(candidates.data)
      : all.data &&
        [...all.data].sort((a, b) => (a.item?.sku ?? "").localeCompare(b.item?.sku ?? ""));
  const [receiving, setReceiving] = useState<string>();

  return (
    <Page
      title="Stock level"
      description="Items whose stock on hand has fallen below their reorder level."
    >
      <SegmentedControl label="Show" value={view} onChange={setView}>
        <SegmentedControlItem value="low" label="Below reorder level" />
        <SegmentedControlItem value="all" label="All items" />
      </SegmentedControl>
      {rows && rows.length === 0 ? (
        <EmptyState
          isCompact
          headingLevel={2}
          title="Nothing to reorder"
          description={`Every item in ${warehouse.name} is at or above its reorder level.`}
        />
      ) : (
        <Table density="compact">
          <TableHeader>
            <TableRow>
              <TableHeaderCell>SKU</TableHeaderCell>
              <TableHeaderCell>Item</TableHeaderCell>
              <TableHeaderCell>On hand</TableHeaderCell>
              <TableHeaderCell>Reorder level</TableHeaderCell>
              <TableHeaderCell>Status</TableHeaderCell>
              <TableHeaderCell>Receive</TableHeaderCell>
            </TableRow>
          </TableHeader>
          <TableBody>
            {(rows ?? []).map((row) => {
              const low = row.on_hand < row.reorder_level;
              // Receive enough to reach twice the reorder level.
              const quantity = Math.max(row.reorder_level * 2 - row.on_hand, 1);
              return (
                <TableRow key={row.id}>
                  <TableCell>
                    <Text type="code">{row.item?.sku ?? "–"}</Text>
                  </TableCell>
                  <TableCell>{row.item?.name ?? "Unknown item"}</TableCell>
                  <TableCell>
                    <Text hasTabularNumbers>{row.on_hand}</Text>
                  </TableCell>
                  <TableCell>
                    <Text hasTabularNumbers>{row.reorder_level}</Text>
                  </TableCell>
                  <TableCell>
                    {low ? <Badge variant="warning" label="Low" /> : <Badge label="In stock" />}
                  </TableCell>
                  <TableCell>
                    <Button
                      label={`Receive ${quantity}`}
                      size="sm"
                      variant="secondary"
                      isDisabled={!canOperate || !low}
                      isLoading={receiving === row.id}
                      onClick={async () => {
                        setReceiving(row.id);
                        try {
                          await restock(db, row.id, quantity);
                        } finally {
                          setReceiving(undefined);
                        }
                      }}
                    />
                  </TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      )}
    </Page>
  );
}
