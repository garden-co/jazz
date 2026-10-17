import { Center } from "@astryxdesign/core/Center";
import { Spinner } from "@astryxdesign/core/Spinner";

export function Loading({ label = "Opening the console" }: { label?: string }) {
  return (
    <Center minHeight="100dvh">
      <Spinner size="lg" label={label} />
    </Center>
  );
}
