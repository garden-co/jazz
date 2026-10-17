"use client";

import { Outline, type OutlineItem } from "@astryxdesign/core/Outline";
import { Text } from "@astryxdesign/core/Text";
import { VStack } from "@astryxdesign/core/Stack";

/** "On this page" outline; the top nav is sticky, so headings land below it. */
export function DocsToc({ items }: { items: OutlineItem[] }) {
  if (items.length === 0) return null;
  return (
    <VStack gap={2}>
      <Text type="supporting" color="secondary" weight="medium" display="block">
        On this page
      </Text>
      <Outline items={items} density="compact" offset={72} label="On this page" />
    </VStack>
  );
}
