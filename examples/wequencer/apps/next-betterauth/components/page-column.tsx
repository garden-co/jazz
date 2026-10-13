import type { ReactNode } from "react";
import { Center } from "@astryxdesign/core/Center";
import { VStack } from "@astryxdesign/core/VStack";

/** A centred page column; sections inside it are spaced by the stack. */
export function PageColumn({
  children,
  maxWidth = 1120,
}: {
  children: ReactNode;
  maxWidth?: number;
}) {
  return (
    <Center axis="horizontal">
      <VStack gap={6} width="100%" maxWidth={maxWidth}>
        {children}
      </VStack>
    </Center>
  );
}
