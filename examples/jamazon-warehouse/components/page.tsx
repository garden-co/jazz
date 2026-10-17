"use client";

import { Banner } from "@astryxdesign/core/Banner";
import { VStack } from "@astryxdesign/core/Stack";
import { Heading, Text } from "@astryxdesign/core/Text";
import type { ReactNode } from "react";
import { useScope } from "./console";

/** One console page: its title, what it is for, and the view-only notice when it applies. */
export function Page({
  title,
  description,
  children,
}: {
  title: string;
  description: string;
  children: ReactNode;
}) {
  const { warehouse, district, homeWarehouse, canOperate } = useScope();
  return (
    <VStack gap={6} maxWidth={1120} paddingBlock={4}>
      <VStack gap={1}>
        <Heading level={1}>{title}</Heading>
        <Text color="secondary">
          {warehouse.name}, {district.name} district. {description}
        </Text>
      </VStack>
      {!canOperate && (
        <Banner
          status="info"
          title="View only"
          description={`You operate ${homeWarehouse.name}. Changes here are rejected by the warehouse's permissions, so they are disabled.`}
        />
      )}
      {children}
    </VStack>
  );
}
