import { VStack } from "@astryxdesign/core/Stack";
import type { ReactNode } from "react";

/** Page body: consistent padding and a readable maximum width. */
export function Page({ children, maxWidth = 1200 }: { children: ReactNode; maxWidth?: number }) {
  return (
    <VStack padding={6} maxWidth={maxWidth} width="100%">
      {children}
    </VStack>
  );
}
