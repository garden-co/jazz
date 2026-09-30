"use client";

import type { ReactNode } from "react";
import { Banner, Spinner, VStack } from "@astryxdesign/core";

/** Full-screen loading or failure state shown before the workspace opens. */
export function StatusScreen({
  label,
  error,
  action,
}: {
  label: string;
  error?: string;
  action?: ReactNode;
}) {
  return (
    <VStack height="100dvh" justify="center" align="center" padding={4}>
      {error ? (
        <VStack gap={3} maxWidth={480} width="100%">
          <Banner status="error" title={label} description={error} collapsible={false} />
          {action}
        </VStack>
      ) : (
        <Spinner size="lg" label={label} />
      )}
    </VStack>
  );
}
