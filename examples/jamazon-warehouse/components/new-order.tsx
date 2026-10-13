"use client";

import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { Card } from "@astryxdesign/core/Card";
import { MetadataList, MetadataListItem } from "@astryxdesign/core/MetadataList";
import { NumberInput } from "@astryxdesign/core/NumberInput";
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
import { Code } from "@astryxdesign/core/Code";
import { Heading, Text } from "@astryxdesign/core/Text";
import { useAll, useDb } from "jazz-tools/react";
import { useState, type FormEvent } from "react";
import { formatCents, newRequestKey } from "@/src/format";
import {
  CheckoutCancelledError,
  consoleQueries,
  InsufficientStockError,
  MAX_ORDER_LINES,
  RequestMismatchError,
  purchase,
  type PurchaseReceipt,
  warehouseQueries,
} from "@/src/warehouse";
import { useScope } from "./console";
import { Page } from "./page";

type Line = { key: string; itemId: string; quantity: number };

type Outcome =
  | { kind: "idle" }
  | { kind: "placed"; receipt: PurchaseReceipt; replayed: boolean }
  | { kind: "failed"; error: unknown };

export function NewOrder() {
  const db = useDb();
  const { warehouse, district, canOperate } = useScope();
  const customers = useAll(
    warehouseQueries({ warehouseId: warehouse.id, districtId: district.id }).customers,
  );
  const items = useAll(consoleQueries.items);
  const stock = useAll(consoleQueries.stockOf(warehouse.id));
  const onHand = new Map(stock.data?.map((row) => [row.item_id, row.on_hand]));
  const itemName = (itemId: string) =>
    items.data?.find((item) => item.id === itemId)?.name ?? "This item";

  const [customerId, setCustomerId] = useState("");
  const [lines, setLines] = useState<Line[]>(() => [emptyLine()]);
  // One key per checkout attempt. A retry of the same attempt reuses it, so
  // the authority returns the first receipt instead of taking stock twice.
  const [requestKey, setRequestKey] = useState(newRequestKey);
  const [submitting, setSubmitting] = useState(false);
  const [outcome, setOutcome] = useState<Outcome>({ kind: "idle" });

  const customer = customers.data?.find((row) => row.id === customerId);
  const ready = canOperate && !!customer && lines.every((line) => line.itemId && line.quantity > 0);

  async function submit(replay = false) {
    setSubmitting(true);
    try {
      const receipt = await purchase(db, {
        warehouseId: warehouse.id,
        districtId: district.id,
        customerId,
        lines: lines.map(({ itemId, quantity }) => ({ itemId, quantity })),
        idempotencyKey: requestKey,
      });
      setOutcome({ kind: "placed", receipt, replayed: replay });
    } catch (error) {
      setOutcome({ kind: "failed", error });
    } finally {
      setSubmitting(false);
    }
  }

  function startOver() {
    setLines([emptyLine()]);
    setRequestKey(newRequestKey());
    setOutcome({ kind: "idle" });
  }

  if (outcome.kind === "placed") {
    const { receipt } = outcome;
    return (
      <Page title="New order" description="The order is in the delivery queue.">
        {outcome.replayed && (
          <Banner
            status="info"
            title="Same receipt returned"
            description="This request key was already accepted, so the order was not placed twice and no stock was taken again."
          />
        )}
        <Card padding={6}>
          <VStack gap={4}>
            <Heading level={2}>Order {receipt.orderNumber} placed</Heading>
            <MetadataList columns="multi">
              <MetadataListItem label="Customer">{customer?.name ?? "Customer"}</MetadataListItem>
              <MetadataListItem label="Total">{formatCents(receipt.totalCents)}</MetadataListItem>
              <MetadataListItem label="Request key">
                <Code>{requestKey.slice(0, 8)}</Code>
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
                {receipt.lines.map((line) => (
                  <TableRow key={line.lineNumber}>
                    <TableCell>{line.lineNumber}</TableCell>
                    <TableCell>{itemName(line.itemId)}</TableCell>
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
            <HStack gap={2} wrap="wrap">
              <Button label="New order" variant="primary" onClick={startOver} />
              <Button
                label="Submit the same request again"
                variant="secondary"
                isLoading={submitting}
                onClick={() => void submit(true)}
              />
            </HStack>
          </VStack>
        </Card>
      </Page>
    );
  }

  return (
    <Page
      title="New order"
      description="Stock, the district's order number and the customer's balance change together, or not at all."
    >
      <form
        onSubmit={(event: FormEvent) => {
          event.preventDefault();
          void submit();
        }}
      >
        <VStack gap={5}>
          <Selector
            label="Customer"
            placeholder="Choose a customer"
            value={customerId}
            onChange={setCustomerId}
            hasSearch
            isDisabled={!canOperate}
            options={(customers.data ?? []).map((row) => ({
              value: row.id,
              label: row.name,
              description: `Balance ${formatCents(row.balance_cents)}`,
            }))}
          />
          <VStack gap={3}>
            <Heading level={2}>Lines</Heading>
            {lines.map((line, index) => (
              <HStack key={line.key} gap={2} vAlign="end" wrap="wrap">
                <Selector
                  label={`Item, line ${index + 1}`}
                  placeholder="Choose an item"
                  value={line.itemId}
                  width={320}
                  hasSearch
                  isDisabled={!canOperate}
                  onChange={(itemId) => setLines(replace(lines, index, { ...line, itemId }))}
                  options={(items.data ?? []).map((item) => ({
                    value: item.id,
                    label: item.name,
                    description: `${item.sku} · ${formatCents(item.unit_price_cents)} · ${onHand.get(item.id) ?? 0} on hand`,
                  }))}
                />
                <NumberInput
                  label="Quantity"
                  value={line.quantity}
                  min={1}
                  isIntegerOnly
                  width={120}
                  isDisabled={!canOperate}
                  onChange={(quantity) => setLines(replace(lines, index, { ...line, quantity }))}
                />
                <Button
                  label="Remove"
                  variant="ghost"
                  isDisabled={lines.length === 1 || !canOperate}
                  onClick={() => setLines(lines.filter((_, other) => other !== index))}
                />
              </HStack>
            ))}
            <HStack>
              <Button
                label="Add line"
                variant="secondary"
                size="sm"
                isDisabled={lines.length >= MAX_ORDER_LINES || !canOperate}
                onClick={() => setLines([...lines, emptyLine()])}
              />
            </HStack>
          </VStack>
          {outcome.kind === "failed" && (
            <FailureBanner
              error={outcome.error}
              itemName={itemName}
              retry={() => void submit()}
              retrying={submitting}
              startOver={startOver}
            />
          )}
          <HStack gap={3} vAlign="center" wrap="wrap">
            <Button
              type="submit"
              variant="primary"
              label="Place order"
              isLoading={submitting}
              isDisabled={!ready}
            />
            <Text type="supporting" color="secondary">
              Request key <Code>{requestKey.slice(0, 8)}</Code>
            </Text>
          </HStack>
        </VStack>
      </form>
    </Page>
  );
}

function FailureBanner({
  error,
  itemName,
  retry,
  retrying,
  startOver,
}: {
  error: unknown;
  itemName: (itemId: string) => string;
  retry: () => void;
  retrying: boolean;
  startOver: () => void;
}) {
  if (error instanceof InsufficientStockError) {
    return (
      <Banner
        status="error"
        title="Insufficient stock"
        description={`${itemName(error.itemId)} has ${error.onHand} on hand; this order asks for ${error.requested}. Nothing was charged or taken from stock.`}
      />
    );
  }
  if (error instanceof RequestMismatchError) {
    return (
      <Banner
        status="warning"
        title={`Order ${error.orderNumber} is already reserved`}
        description="An earlier attempt of this request reserved different lines. Place or release that order from order status, or start a new order."
        endContent={<Button label="Start over" size="sm" onClick={startOver} />}
      />
    );
  }
  if (error instanceof CheckoutCancelledError) {
    return (
      <Banner
        status="error"
        title={`Order ${error.orderNumber} was cancelled`}
        description="The authority rejected this order after reserving it, so its stock and balance were returned. Start a new order to try again."
        endContent={<Button label="Start over" size="sm" onClick={startOver} />}
      />
    );
  }
  return (
    <Banner
      status="error"
      title="The order was not confirmed"
      description={`${error instanceof Error ? error.message : String(error)}. Retrying reuses this request's key, so it finishes this order instead of placing a second one.`}
      endContent={<Button label="Retry" size="sm" isLoading={retrying} onClick={retry} />}
    />
  );
}

function emptyLine(): Line {
  return { key: newRequestKey(), itemId: "", quantity: 1 };
}

function replace<T>(list: T[], index: number, value: T): T[] {
  return list.map((entry, other) => (other === index ? value : entry));
}
