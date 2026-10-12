"use client";

import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { Card } from "@astryxdesign/core/Card";
import { Center } from "@astryxdesign/core/Center";
import { Selector } from "@astryxdesign/core/Selector";
import { VStack } from "@astryxdesign/core/Stack";
import { Heading, Text } from "@astryxdesign/core/Text";
import { useState } from "react";
import type { Warehouse } from "@/schema";
import { bootstrap } from "./console";

/**
 * Operators belong to one warehouse. The trusted bootstrap route staffs the
 * operator; the new membership row then arrives through the live query in
 * the console, which switches to the dashboard on its own.
 */
export function JoinWarehouse({ warehouses }: { warehouses: Warehouse[] }) {
  // Warehouse rows can arrive after this mounts (a fresh database is seeded on
  // first sign-in), so the default is derived from the current rows rather
  // than captured once as initial state.
  const [picked, setPicked] = useState<string>();
  const warehouseId =
    (picked && warehouses.some((warehouse) => warehouse.id === picked) ? picked : undefined) ??
    warehouses[0]?.id ??
    "";
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string>();

  return (
    <Center minHeight="100dvh" padding={4}>
      <Card padding={6} width="100%" maxWidth={420}>
        <VStack gap={4}>
          <VStack gap={1}>
            <Heading level={1}>Choose your warehouse</Heading>
            <Text color="secondary">
              You'll run orders, deliveries and stock for one warehouse. You can still view the
              others.
            </Text>
          </VStack>
          <Selector
            label="Warehouse"
            value={warehouseId}
            onChange={setPicked}
            options={warehouses.map((warehouse) => ({
              value: warehouse.id,
              label: warehouse.name,
            }))}
          />
          {error && <Banner status="error" title={error} />}
          <Button
            variant="primary"
            label="Join warehouse"
            width="100%"
            isLoading={pending}
            isDisabled={!warehouseId}
            onClick={async () => {
              setPending(true);
              setError(undefined);
              try {
                await bootstrap(warehouseId);
              } catch (cause) {
                setError(cause instanceof Error ? cause.message : String(cause));
                setPending(false);
              }
            }}
          />
        </VStack>
      </Card>
    </Center>
  );
}
