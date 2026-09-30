"use client";

import type { ReactNode } from "react";
import { Theme } from "@astryxdesign/core";
import { jazzTheme } from "@garden-co/design/jazz";
import "@astryxdesign/core/reset.css";
import "@astryxdesign/core/astryx.css";
import "@garden-co/design/jazz/theme.css";
import "@garden-co/design/jazz/components.css";
import "@garden-co/design/jazz/fonts.css";

/** The shared Jazz design-system theme, following the system colour mode. */
export function JazzTheme({ children }: { children: ReactNode }) {
  return (
    <Theme theme={jazzTheme} mode="system">
      {children}
    </Theme>
  );
}
