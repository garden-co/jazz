"use client";

import { Center } from "@astryxdesign/core";
import type { ReactNode } from "react";

/**
 * A full-height centred page. Astryx's entry is a client module that
 * re-exports with `export *`, which Next cannot import directly into a
 * server component, so server pages use this wrapper.
 */
export function CenteredPage({ children }: { children: ReactNode }) {
  return (
    <main>
      <Center minHeight="100dvh" padding={4}>
        {children}
      </Center>
    </main>
  );
}
