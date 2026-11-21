"use client";

import type { ReactNode } from "react";
import { Theme } from "@astryxdesign/core";
import { jazzTheme } from "@garden-co/design/jazz";

/** The shared Jazz design-system theme, following the system colour mode. */
export function JazzTheme({ children }: { children: ReactNode }) {
  return (
    <Theme theme={jazzTheme} mode="system">
      {children}
    </Theme>
  );
}
