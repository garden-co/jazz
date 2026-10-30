import { VStack } from "@astryxdesign/core/Stack";
import type { ReactNode } from "react";

/**
 * Page body: consistent padding and a readable maximum width. The padding sits
 * on the outer stack so the full-width inner stack never grows past the
 * viewport by the padding.
 */
export function Page({ children, maxWidth = 1200 }: { children: ReactNode; maxWidth?: number }) {
  return (
    <VStack padding={6}>
      <VStack maxWidth={maxWidth} width="100%">
        {children}
      </VStack>
    </VStack>
  );
}
