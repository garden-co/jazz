import { Spinner } from "@astryxdesign/core/Spinner";
import { Text } from "@astryxdesign/core/Text";
import { VStack } from "@astryxdesign/core/Stack";

export function Loading({ label }: { label: string }) {
  return (
    <VStack gap={2} hAlign="center" padding={10} role="status">
      <Spinner />
      <Text color="secondary">{label}</Text>
    </VStack>
  );
}
