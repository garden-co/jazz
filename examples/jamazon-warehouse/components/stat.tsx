import { Card } from "@astryxdesign/core/Card";
import { VStack } from "@astryxdesign/core/Stack";
import { Heading, Text } from "@astryxdesign/core/Text";

/** A dashboard number: what it counts, the value, and what it covers. */
export function Stat({ label, value, note }: { label: string; value: string; note: string }) {
  return (
    <Card padding={4}>
      <VStack gap={1}>
        <Text type="supporting" color="secondary">
          {label}
        </Text>
        <Heading level={2} type="display-2">
          {value}
        </Heading>
        <Text type="supporting" color="secondary">
          {note}
        </Text>
      </VStack>
    </Card>
  );
}
