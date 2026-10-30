"use client";

import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { MetadataList, MetadataListItem } from "@astryxdesign/core/MetadataList";
import { NumberInput } from "@astryxdesign/core/NumberInput";
import { Selector } from "@astryxdesign/core/Selector";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { useAll, useDb } from "jazz-tools/react";
import { useState, type FormEvent } from "react";
import { formatCents, newRequestKey } from "@/src/format";
import { recordPayment, warehouseQueries } from "@/src/warehouse";
import { useScope } from "./console";
import { Page } from "./page";

export function Payments() {
  const db = useDb();
  const { warehouse, district, canOperate } = useScope();
  const customers = useAll(
    warehouseQueries({ warehouseId: warehouse.id, districtId: district.id }).customers,
  );
  const [customerId, setCustomerId] = useState("");
  const [amount, setAmount] = useState<number | null>(null);
  const [requestKey, setRequestKey] = useState(newRequestKey);
  const [pending, setPending] = useState(false);
  const [result, setResult] = useState<{ amountCents: number; name: string } | Error>();
  const customer = customers.data?.find((row) => row.id === customerId);
  const amountCents = amount === null ? 0 : Math.round(amount * 100);

  async function submit(event: FormEvent) {
    event.preventDefault();
    if (!customer) return;
    setPending(true);
    try {
      await recordPayment(db, {
        warehouseId: warehouse.id,
        districtId: district.id,
        customerId: customer.id,
        amountCents,
        idempotencyKey: requestKey,
      });
      setResult({ amountCents, name: customer.name });
      setAmount(null);
      setRequestKey(newRequestKey());
    } catch (error) {
      // Keep the key: resubmitting this attempt can't credit the customer twice.
      setResult(error instanceof Error ? error : new Error(String(error)));
    } finally {
      setPending(false);
    }
  }

  return (
    <Page title="Payment" description="Credit a customer's balance. Each payment is recorded once.">
      <form onSubmit={submit}>
        <VStack gap={5} maxWidth={480}>
          <Selector
            label="Customer"
            placeholder="Choose a customer"
            value={customerId}
            onChange={setCustomerId}
            hasSearch
            isDisabled={!canOperate}
            options={(customers.data ?? []).map((row) => ({ value: row.id, label: row.name }))}
          />
          {customer && (
            <MetadataList columns="single">
              <MetadataListItem label="Balance">
                {formatCents(customer.balance_cents)}
              </MetadataListItem>
            </MetadataList>
          )}
          <NumberInput
            label="Amount"
            units="USD"
            value={amount}
            hasClear
            min={0.01}
            step={0.01}
            isDisabled={!canOperate}
            onChange={setAmount}
          />
          {result instanceof Error && (
            <Banner
              status="error"
              title="The payment was not recorded"
              description={`${result.message}. Submitting again reuses this payment's key.`}
            />
          )}
          {result && !(result instanceof Error) && (
            <Banner
              status="success"
              title={`Recorded ${formatCents(result.amountCents)} from ${result.name}`}
            />
          )}
          <HStack>
            <Button
              type="submit"
              variant="primary"
              label="Record payment"
              isLoading={pending}
              isDisabled={!canOperate || !customer || amountCents <= 0}
            />
          </HStack>
        </VStack>
      </form>
    </Page>
  );
}
